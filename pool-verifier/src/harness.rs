//! Process harness and a small synthetic miner used by runtime scenarios.
//!
//! Every [`Harness`] owns one disposable pool process, one loopback service
//! [`Mock`], and one scenario directory. [`Miner`] speaks the real line protocol
//! over TCP, so scenarios exercise framing, hashing, accounting and cleanup
//! through the same boundaries an actual miner uses.
//!
//! Pool state is read from the dashboard `/data` endpoint. In the disposable
//! copy that response carries an extra `verify` object with private counters
//! (see `prepare::add_observation_surface`).

use crate::mock::{unused_port, Mock, Upload};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpSocket, TcpStream},
    process::{Child, Command},
    time::{sleep, timeout, Instant},
};

/// Dashboard key written into every generated fixture configuration.
pub const KEY: &str = "verifier-dashboard-key-local-only";
/// Default deadline for a single observable condition (a reply, a state change).
pub const WAIT: Duration = Duration::from_secs(6);
/// How long the pool may take to become ready (dashboard up + first sync).
const STARTUP_DEADLINE: Duration = Duration::from_secs(20);
/// How long a graceful shutdown may take before the pool is killed.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(8);

/// Per-scenario fixture settings.
///
/// These are scenario inputs, not alternate pool implementations. They become
/// fixture config or mock behavior before the production process starts.
#[derive(Clone)]
pub struct Options {
    /// Make every accepted share a block (`block_chance = 1` for all tiers).
    pub blocks: bool,
    /// Narrow the AVR/MEGA hashrate bands so tier transitions can be triggered.
    pub transition: bool,
    /// `maxWorkers` per user; 0 means unlimited.
    pub max_workers: u64,
    /// `maxSharesPerMinute`.
    pub rate: u16,
    /// `syncTime` in seconds.
    pub sync_secs: u32,
    /// `updateMinersStatsEvery`: publish miner stats every N accepted shares.
    pub stats_every: u16,
    /// Scenario name, used to queue mock faults before the pool starts.
    pub case: &'static str,
    /// Config overrides merged into the generated config. A JSON `null` removes
    /// the key entirely.
    pub config: Value,
    /// Extra files `(relative path, contents)` written into the scenario
    /// directory before launch. They may overwrite the generated config.
    pub initial_files: Vec<(String, String)>,
    /// Return right after spawning, without waiting for readiness. Used by
    /// scenarios that expect startup to fail.
    pub launch_only: bool,
    /// The dashboard key is too short, so `/data` is disabled; readiness is
    /// detected through `/ping` instead.
    pub dashboard_disabled: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            blocks: false,
            transition: false,
            max_workers: 0,
            rate: 60000,
            sync_secs: 3600,
            stats_every: 1,
            case: "",
            config: json!({}),
            initial_files: vec![],
            launch_only: false,
            dashboard_disabled: false,
        }
    }
}

/// One running pool process plus everything it talks to.
pub struct Harness {
    pub mock: Mock,
    /// Scenario directory; also the pool's working directory.
    pub dir: PathBuf,
    /// Miner TCP port. Re-read from the config after startup (`guessPort` may change it).
    pub port: u16,
    /// Dashboard base URL, e.g. `http://127.0.0.1:12345`.
    pub dashboard: String,
    pub client: reqwest::Client,
    /// Dashboard key actually in effect (scenarios may override it).
    pub key: String,
    child: Child,
    /// Every line sent/received by any miner of this harness, for `miners.log`.
    transcript: Arc<Mutex<Vec<String>>>,
    /// The child has exited and been reaped (or killed); skip further shutdown.
    stopped: bool,
}

impl Harness {
    pub fn process_id(&self) -> Result<u32> {
        self.child.id().context("Pool process has no ID")
    }

    /// Writes a fixture into `dir`, launches the pool binary there, and (unless
    /// `options.launch_only`) waits until it is ready for assertions.
    pub async fn start(
        binary: &Path,
        dir: PathBuf,
        options: Options,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        fs::create_dir_all(dir.join("config"))?;
        fs::create_dir_all(dir.join("sync"))?;
        let mock = Mock::start(options.blocks, options.transition).await?;
        let port = unused_port()?;
        let dashboard_port = loop {
            let candidate = unused_port()?;
            if candidate != port {
                break candidate;
            }
        };

        let config = fixture_config(&options, &mock, port, dashboard_port)?;
        // Faults must be queued before launch; the pool's first requests would
        // otherwise race ahead of scenario setup.
        crate::expanded::configure_mock(options.case, &mock);
        fs::write(
            dir.join("config/config.json"),
            serde_json::to_vec_pretty(&config)?,
        )?;
        write_initial_files(&dir, &options.initial_files)?;

        let child = spawn_pool(binary, &dir, &mock)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()?;
        let mut harness = Self {
            mock,
            dashboard: format!("http://127.0.0.1:{dashboard_port}"),
            port,
            client,
            key: config["dashboard_key"].as_str().unwrap_or(KEY).to_owned(),
            dir,
            child,
            transcript: Arc::new(Mutex::new(vec![])),
            stopped: false,
        };
        if !options.launch_only {
            harness
                .wait_ready(cancelled, options.dashboard_disabled)
                .await?;
        }
        Ok(harness)
    }

    /// Waits until the pool can serve assertions.
    ///
    /// A listening socket alone is too early: readiness requires the dashboard
    /// (or `/ping` when the dashboard is disabled) *and* a completed initial sync.
    pub async fn wait_ready(
        &mut self,
        cancelled: &AtomicBool,
        dashboard_disabled: bool,
    ) -> Result<()> {
        let deadline = Instant::now() + STARTUP_DEADLINE;
        loop {
            if cancelled.load(Ordering::SeqCst) {
                self.abandon_startup().await;
                bail!("Interrupted during startup");
            }
            if let Some(status) = self.child.try_wait()? {
                self.abandon_startup().await;
                bail!(
                    "Pool exited during setup ({status}); see {}",
                    self.dir.join("pool.log").display()
                );
            }
            let responding = if dashboard_disabled {
                self.client
                    .get(format!("{}/ping", self.dashboard))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
            } else {
                self.state().await.is_ok()
            };
            if responding && self.mock.uploads().iter().any(|u| u.completed) {
                break;
            }
            if Instant::now() >= deadline {
                self.abandon_startup().await;
                bail!(
                    "Pool startup timed out; see {}",
                    self.dir.join("pool.log").display()
                );
            }
            sleep(Duration::from_millis(25)).await;
        }

        // The pool rewrites its config on startup; with `guessPort` the miner
        // port in the file is the one it actually bound.
        let config: Value =
            serde_json::from_slice(&fs::read(self.dir.join("config/config.json"))?)?;
        self.port = config["port"].as_u64().context("pool port")? as u16;
        if !dashboard_disabled {
            self.wait_state("empty startup state", |s| {
                s["connections"] == 0 && s["workers"].as_object().is_some_and(|m| m.is_empty())
            })
            .await?;
        }
        Ok(())
    }

    /// Saves diagnostics and kills the child after a failed startup.
    async fn abandon_startup(&mut self) {
        let _ = self.save().await;
        let _ = self.child.kill().await;
        self.stopped = true;
    }

    /// Fetches the authenticated dashboard state (including `verify`).
    pub async fn state(&self) -> Result<Value> {
        Ok(self
            .client
            .get(format!("{}/data", self.dashboard))
            .query(&[("key", self.key.as_str())])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Polls the dashboard until `predicate` holds, or fails after [`WAIT`].
    ///
    /// Polling observable state avoids sleeping for a guessed duration. The last
    /// state is included in the timeout error to make failures actionable.
    pub async fn wait_state(
        &self,
        label: &str,
        predicate: impl Fn(&Value) -> bool,
    ) -> Result<Value> {
        let deadline = Instant::now() + WAIT;
        loop {
            let state = self.state().await?;
            if predicate(&state) {
                return Ok(state);
            }
            if Instant::now() >= deadline {
                bail!("{label}: condition not reached; actual state: {state}");
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until no connections, worker allocations, or miner stats remain.
    pub async fn empty(&self) -> Result<()> {
        self.wait_state("all connections, stats, and allocations removed", |s| {
            s["connections"] == 0
                && s["workers"].as_object().is_some_and(|m| m.is_empty())
                && s["miner_stats"].as_object().is_some_and(|m| m.is_empty())
        })
        .await?;
        Ok(())
    }

    /// Connects a new miner from 127.0.0.1.
    pub async fn miner(&self, username: &str, tier: &str, rig: &str) -> Result<Miner> {
        self.miner_from(username, tier, rig, None).await
    }

    /// Connects a new miner, optionally binding a specific local source IP
    /// (e.g. `127.0.0.2`, because the pool never bans `127.0.0.1`).
    pub async fn miner_from(
        &self,
        username: &str,
        tier: &str,
        rig: &str,
        source_ip: Option<&str>,
    ) -> Result<Miner> {
        let pool = format!("127.0.0.1:{}", self.port).parse()?;
        let socket = match source_ip {
            Some(ip) => {
                let socket = TcpSocket::new_v4()?;
                socket.bind(format!("{ip}:0").parse()?)?;
                socket.connect(pool).await?
            }
            None => TcpStream::connect(pool).await?,
        };
        socket.set_nodelay(true)?;
        let mut miner = Miner {
            reader: BufReader::new(socket),
            username: username.into(),
            tier: tier.into(),
            rig: rig.into(),
            transcript: self.transcript.clone(),
        };
        anyhow::ensure!(miner.line().await? == "3.0", "Unexpected pool greeting");
        Ok(miner)
    }

    /// Waits for a sync upload at position `>= index` that matches `predicate`.
    ///
    /// Searching only after the caller's cursor lets retry tests tell a new
    /// attempt apart from a previously completed upload.
    pub async fn upload_after(
        &self,
        index: usize,
        predicate: impl Fn(&Upload) -> bool,
    ) -> Result<(usize, Upload)> {
        let deadline = Instant::now() + WAIT;
        loop {
            let found = self
                .mock
                .uploads()
                .into_iter()
                .enumerate()
                .skip(index)
                .find(|(_, upload)| predicate(upload));
            if let Some((i, upload)) = found {
                anyhow::ensure!(
                    upload.error.is_none(),
                    "Malformed multipart upload: {:?}",
                    upload.error
                );
                return Ok((i, upload));
            }
            if Instant::now() >= deadline {
                bail!(
                    "Expected sync upload not received; actual: {:?}",
                    self.mock.uploads()
                );
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Runs the production reward/ban refresh via the injected stdin command.
    /// The pool prints the marker only after the refresh has finished.
    pub async fn refresh(&mut self) -> Result<()> {
        self.child
            .stdin
            .as_mut()
            .context("stdin closed")?
            .write_all(b"refresh\n")
            .await?;
        self.wait_log("VERIFY_REFRESH_OK").await
    }

    /// Asks the pool to shut down (same channel as Ctrl-C) without waiting.
    pub async fn request_shutdown(&mut self) -> Result<()> {
        if let Some(mut input) = self.child.stdin.take() {
            input.write_all(b"shutdown\n").await?;
        }
        Ok(())
    }

    /// Waits for the pool to exit successfully after [`request_shutdown`](Self::request_shutdown).
    pub async fn wait_stopped(&mut self) -> Result<()> {
        let status = timeout(SHUTDOWN_DEADLINE, self.child.wait())
            .await
            .context("Shutdown deadline exceeded")??;
        self.stopped = true;
        anyhow::ensure!(status.success(), "Pool shutdown failed: {status}");
        Ok(())
    }

    /// Asserts that a `launch_only` pool exits with an error and logs `diagnostic`.
    pub async fn expect_startup_failure(&mut self, diagnostic: &str) -> Result<()> {
        let status = timeout(Duration::from_secs(6), self.child.wait())
            .await
            .context("Pool hung instead of rejecting startup")??;
        self.stopped = true;
        anyhow::ensure!(!status.success(), "Invalid startup unexpectedly succeeded");
        let log = fs::read_to_string(self.dir.join("pool.log"))?;
        anyhow::ensure!(
            log.contains(diagnostic),
            "Wrong startup failure: expected {diagnostic:?}; log: {log}"
        );
        Ok(())
    }

    /// Waits until `pool.log` contains `text`.
    pub async fn wait_log(&self, text: &str) -> Result<()> {
        let deadline = Instant::now() + WAIT;
        loop {
            if fs::read_to_string(self.dir.join("pool.log"))?.contains(text) {
                return Ok(());
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "Missing pool diagnostic {text:?}"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Gracefully stops the pool; kills it (and fails) if shutdown hangs.
    pub async fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        // Never leave a deliberately held upload blocking graceful shutdown.
        self.mock.hold(false);
        self.mock.shared.gate.add_permits(100);
        self.request_shutdown().await?;
        match timeout(SHUTDOWN_DEADLINE, self.child.wait()).await {
            Ok(status) => {
                self.stopped = true;
                anyhow::ensure!(
                    status?.success(),
                    "Pool exited unsuccessfully during shutdown"
                );
            }
            Err(_) => {
                self.child.kill().await?;
                self.stopped = true;
                bail!("Graceful shutdown timed out (pool killed)");
            }
        }
        Ok(())
    }

    /// End-of-scenario cleanup: save live state, stop the pool, then save the
    /// final logs and mock requests. Every step is attempted; all errors are
    /// returned so none hides another.
    pub async fn finish(&mut self) -> Vec<anyhow::Error> {
        let save_before = self.save().await;
        let stop = self.stop().await;
        let save_after = self.save().await;
        [save_before, stop, save_after]
            .into_iter()
            .filter_map(Result::err)
            .collect()
    }

    /// Writes `state.json` (while running), `requests.json` and `miners.log`.
    ///
    /// Useful both before and after shutdown: live state disappears on exit,
    /// while final logs and mock requests may arrive during cleanup.
    pub async fn save(&self) -> Result<()> {
        if !self.stopped {
            if let Ok(state) = self.state().await {
                fs::write(
                    self.dir.join("state.json"),
                    serde_json::to_vec_pretty(&state)?,
                )?;
            }
        }
        fs::write(
            self.dir.join("requests.json"),
            serde_json::to_vec_pretty(&self.mock.diagnostics())?,
        )?;
        fs::write(
            self.dir.join("miners.log"),
            self.transcript.lock().unwrap().join("\n"),
        )?;
        Ok(())
    }
}

/// Builds the pool configuration for one scenario.
///
/// It uses generated credentials and loopback endpoints only. Nothing is copied
/// from a real configuration.
fn fixture_config(options: &Options, mock: &Mock, port: u16, dashboard_port: u16) -> Result<Value> {
    let mut config = json!({
        "poolName": "verifier", "poolPassword": "fake-password", "poolID": "verifier-id",
        "poolVersion": "0.3", "host": "127.0.0.1", "port": port,
        "serverIP": "127.0.0.1", "serverPort": mock.master_port, "serverVersion": "3.0",
        "preGenJobCount": 4, "jobGenerationDelay": 1, "maxWorkers": options.max_workers,
        "motd": "verifier-motd\n", "initialBlockHash": "05b7406b3909071b2d85b2d2958ad07a0d52bd12",
        "expectedSharetime": 1, "updateMinersStatsEvery": options.stats_every,
        "blockProbability": 1000000, "blockReward": 2.0, "use_ngrok": false,
        "autoRestart": 0, "guessPort": false, "dashboard_port": dashboard_port,
        "syncTime": options.sync_secs, "timeout": 3,
        "server_sync_url": mock.http, "base_sync_folder": "sync",
        "maxSharesPerMinute": options.rate, "restart_script": "", "firewall_cmd": "",
        "dashboard_key": KEY, "update_restart_script": "", "auto_update": false,
        "update_check_time_seconds": 0
    });
    let overrides = options
        .config
        .as_object()
        .context("config overrides must be an object")?;
    let fields = config.as_object_mut().unwrap();
    for (key, value) in overrides {
        // A JSON null means "remove this key", so startup tests can tell a
        // genuinely missing required field apart from a default value.
        if value.is_null() {
            fields.remove(key);
        } else {
            fields.insert(key.clone(), value.clone());
        }
    }
    Ok(config)
}

/// Writes scenario-provided files below `dir`, rejecting paths that could escape it.
fn write_initial_files(dir: &Path, files: &[(String, String)]) -> Result<()> {
    for (name, contents) in files {
        let path = Path::new(name);
        anyhow::ensure!(
            path.components().all(|c| matches!(c, Component::Normal(_))),
            "Unsafe fixture path"
        );
        let path = dir.join(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
    }
    Ok(())
}

/// Launches the pool in `dir`, logging stdout and stderr to `pool.log`.
fn spawn_pool(binary: &Path, dir: &Path, mock: &Mock) -> Result<Child> {
    let log = fs::File::create(dir.join("pool.log"))?;
    // Proxy variables are cleared explicitly. Even loopback requests can be
    // intercepted on developer or CI machines with global proxy settings.
    Command::new(binary)
        .current_dir(dir)
        .env("POOL_VERIFY_HTTP", &mock.http)
        .env("NO_PROXY", "*")
        .env("no_proxy", "*")
        .env_remove("HTTP_PROXY")
        .env_remove("HTTPS_PROXY")
        .env_remove("ALL_PROXY")
        .stdin(Stdio::piped())
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .context("Starting disposable pool")
}

/// Finds the published dashboard stats entry for the rig named `rig`.
///
/// Stats are keyed by connection ID, not by rig name, so this searches values.
pub fn stat<'a>(state: &'a Value, rig: &str) -> Option<&'a Value> {
    state["miner_stats"]
        .as_object()?
        .values()
        .find(|v| v["id"] == rig)
}

/// `true` for the two replies that mean a share was accepted.
pub fn is_accepted(reply: &str) -> bool {
    reply == "GOOD" || reply == "BLOCK"
}

/// Fails unless `reply` is an acceptance.
pub fn ensure_accepted(reply: &str) -> Result<()> {
    anyhow::ensure!(is_accepted(reply), "Expected acceptance: {reply}");
    Ok(())
}

/// A job as advertised by the pool, already solved by the verifier.
#[derive(Debug)]
pub struct Job {
    pub difficulty: u64,
    /// The nonce that solves this job.
    pub nonce: u64,
    /// Previous block hash (the hashing prefix).
    pub previous: String,
    /// Target SHA-1 hex digest.
    pub hash: String,
}

/// Finds the nonce the pool expects for a job.
///
/// The pool defines a valid nonce as `SHA-1(previous || decimal nonce) == hash`
/// with the nonce in `1..=difficulty * 100`. Test difficulties are bounded so a
/// broken job fails quickly instead of searching forever.
pub fn solve(previous: &str, hash: &str, difficulty: u64) -> Result<u64> {
    anyhow::ensure!(
        difficulty > 0 && difficulty <= 100_000,
        "Unexpected test-job difficulty {difficulty}"
    );
    let limit = difficulty * 100;
    for nonce in 1..=limit {
        let mut sha = sha1_smol::Sha1::new();
        sha.update(previous.as_bytes());
        sha.update(nonce.to_string().as_bytes());
        if sha.digest().to_string() == hash {
            return Ok(nonce);
        }
    }
    bail!("Advertised job has no SHA-1 solution in 1..={limit}")
}

/// A synthetic miner connection that speaks the pool's line protocol.
///
/// Everything sent and received is appended to the harness transcript.
pub struct Miner {
    reader: BufReader<TcpStream>,
    pub username: String,
    pub tier: String,
    pub rig: String,
    transcript: Arc<Mutex<Vec<String>>>,
}

impl Miner {
    fn record(&self, direction: char, text: &str) {
        self.transcript.lock().unwrap().push(format!(
            "{} {direction} {}",
            self.rig,
            text.escape_debug()
        ));
    }

    /// Writes raw bytes (no framing is added).
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.record('>', &String::from_utf8_lossy(data));
        timeout(WAIT, self.reader.get_mut().write_all(data)).await??;
        Ok(())
    }

    /// Reads exactly `length` bytes (for replies without a line terminator).
    pub async fn bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0; length];
        timeout(WAIT, self.reader.read_exact(&mut bytes)).await??;
        Ok(bytes)
    }

    /// Reads one reply line without its terminator.
    pub async fn line(&mut self) -> Result<String> {
        self.line_timeout(WAIT).await
    }

    pub async fn line_timeout(&mut self, duration: Duration) -> Result<String> {
        let mut line = String::new();
        let n = timeout(duration, self.reader.read_line(&mut line))
            .await
            .context("Timed out reading pool response")??;
        anyhow::ensure!(n > 0, "Pool disconnected");
        let line = line.trim_end_matches(['\r', '\n']).to_string();
        self.record('<', &line);
        Ok(line)
    }

    /// Requests a job (`JOB,<user>,<tier>,...`) and solves it.
    pub async fn job(&mut self) -> Result<Job> {
        self.job_framed("\n").await
    }

    /// Like [`job`](Self::job) but with a custom line ending (`""` for none).
    pub async fn job_framed(&mut self, ending: &str) -> Result<Job> {
        let request = format!(
            "JOB,{},{},test-key,temperature:21{ending}",
            self.username, self.tier
        );
        self.send(request.as_bytes()).await?;
        self.read_job().await
    }

    /// Reads and solves a `previous,hash,difficulty` job line.
    pub async fn read_job(&mut self) -> Result<Job> {
        let line = self.line().await?;
        let fields: Vec<_> = line.split(',').collect();
        let [previous, hash, difficulty] = fields[..] else {
            bail!("Expected job, got {line:?}");
        };
        let difficulty = difficulty.parse()?;
        Ok(Job {
            difficulty,
            nonce: solve(previous, hash, difficulty)?,
            previous: previous.into(),
            hash: hash.into(),
        })
    }

    /// Answers `job` with its correct nonce, or with the wrong nonce `0`.
    pub async fn answer(&mut self, job: &Job, correct: bool) -> Result<String> {
        let nonce = if correct {
            job.nonce.to_string()
        } else {
            "0".into()
        };
        self.answer_fields(nonce, "1000", "chip-verifier").await
    }

    /// Sends `nonce,hashrate,Verifier,<rig>,chip,wallet-verifier` and returns
    /// the reply line.
    pub async fn answer_fields(
        &mut self,
        nonce: String,
        hashrate: &str,
        chip: &str,
    ) -> Result<String> {
        // A short pause keeps the share time the pool measures above zero.
        sleep(Duration::from_millis(3)).await;
        let answer = format!(
            "{nonce},{hashrate},Verifier,{},{chip},wallet-verifier\n",
            self.rig
        );
        self.send(answer.as_bytes()).await?;
        self.line().await
    }

    /// Requests a job, answers it correctly, and requires acceptance.
    pub async fn share(&mut self) -> Result<String> {
        let job = self.job().await?;
        let feedback = self.answer(&job, true).await?;
        anyhow::ensure!(
            is_accepted(&feedback),
            "Expected accepted share, got {feedback:?}"
        );
        Ok(feedback)
    }

    /// Graceful close (FIN).
    pub async fn close(mut self) -> Result<()> {
        self.reader.get_mut().shutdown().await?;
        Ok(())
    }

    /// Abortive close. Zero linger makes the OS send an immediate RST; the
    /// drop never waits.
    #[allow(deprecated)]
    pub fn reset(self) -> Result<()> {
        self.reader.get_ref().set_linger(Some(Duration::ZERO))?;
        drop(self);
        Ok(())
    }

    /// Requires the pool to close this connection within `duration`, without
    /// sending anything else first.
    pub async fn expect_closed(&mut self, duration: Duration) -> Result<()> {
        use std::io::ErrorKind;
        let mut line = String::new();
        match timeout(duration, self.reader.read_line(&mut line)).await {
            Ok(Ok(0)) => Ok(()),
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::BrokenPipe
                        | ErrorKind::UnexpectedEof
                ) =>
            {
                Ok(())
            }
            Ok(Err(error)) => Err(error).context("Unexpected error instead of a disconnect"),
            Ok(Ok(_)) => bail!("Expected disconnect, received {line:?}"),
            Err(_) => bail!("Connection stayed open past {duration:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn solver_uses_hash_and_checks_search_bounds() {
        let mut hash = sha1_smol::Sha1::new();
        hash.update(b"previous37");
        assert_eq!(
            solve("previous", &hash.digest().to_string(), 1).unwrap(),
            37
        );
        assert!(solve("previous", "0000000000000000000000000000000000000000", 1).is_err());
        assert!(solve("previous", &hash.digest().to_string(), 0).is_err());
    }
}
