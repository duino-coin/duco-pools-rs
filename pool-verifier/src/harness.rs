//! Process harness and small synthetic miner client used by runtime scenarios.
//!
//! Every `Harness` owns one disposable pool process, one loopback service mock,
//! and one scenario directory. `Miner` speaks the real line protocol over TCP;
//! tests therefore exercise framing, hashing, accounting, and cleanup through the
//! same boundaries used by an actual miner.

use crate::mock::{unused_port, Mock, Upload};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
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

pub const KEY: &str = "verifier-dashboard-key-local-only";
pub const WAIT: Duration = Duration::from_secs(6);

#[derive(Clone)]
pub struct Options {
    // These are scenario inputs, not alternate pool implementations. They become
    // fixture config or mock behavior before the production process starts.
    pub blocks: bool,
    pub transition: bool,
    pub max_workers: u64,
    pub rate: u16,
    pub sync_secs: u32,
    pub stats_every: u16,
    pub case: &'static str,
    pub config: Value,
    pub initial_files: Vec<(String, String)>,
    pub launch_only: bool,
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

pub struct Harness {
    pub mock: Mock,
    pub dir: PathBuf,
    pub port: u16,
    pub dashboard: String,
    pub client: reqwest::Client,
    child: Child,
    transcript: Arc<Mutex<Vec<String>>>,
    stopped: bool,
    pub key: String,
}

impl Harness {
    pub fn process_id(&self) -> Result<u32> {
        self.child.id().context("Pool process has no ID")
    }

    pub async fn start(
        binary: &Path,
        dir: PathBuf,
        options: Options,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        // The child receives generated credentials and only loopback endpoints.
        // Each scenario's current directory contains all config and sync files.
        fs::create_dir_all(dir.join("config"))?;
        fs::create_dir_all(dir.join("sync"))?;
        let mock = Mock::start(options.blocks, options.transition).await?;
        let port = unused_port()?;
        let dashboard_port = loop {
            let p = unused_port()?;
            if p != port {
                break p;
            }
        };
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
        // A JSON null means "remove this key", which allows startup tests to
        // distinguish a genuinely missing required field from a default value.
        for (key, value) in options
            .config
            .as_object()
            .context("config overrides must be an object")?
        {
            if value.is_null() {
                config.as_object_mut().unwrap().remove(key);
            } else {
                config[key] = value.clone();
            }
        }
        crate::expanded::configure_mock(options.case, &mock);
        fs::write(
            dir.join("config/config.json"),
            serde_json::to_vec_pretty(&config)?,
        )?;
        for (name, contents) in &options.initial_files {
            let path = Path::new(name);
            anyhow::ensure!(
                path.components()
                    .all(|p| matches!(p, std::path::Component::Normal(_))),
                "Unsafe fixture path"
            );
            if let Some(parent) = dir.join(path).parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(dir.join(path), contents)?;
        }
        let log = fs::File::create(dir.join("pool.log"))?;
        // Explicitly clear proxy variables: even loopback requests can otherwise be
        // intercepted on developer or CI machines with global proxy settings.
        let child = Command::new(binary)
            .current_dir(&dir)
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
            .context("Starting disposable pool")?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()?;
        let mut h = Self {
            mock,
            dir,
            port,
            dashboard: format!("http://127.0.0.1:{dashboard_port}"),
            client,
            child,
            transcript: Arc::new(Mutex::new(vec![])),
            stopped: false,
            key: config["dashboard_key"].as_str().unwrap_or(KEY).to_owned(),
        };
        if options.launch_only {
            return Ok(h);
        }
        h.wait_ready(cancelled, options.dashboard_disabled).await?;
        Ok(h)
    }

    pub async fn wait_ready(
        &mut self,
        cancelled: &AtomicBool,
        dashboard_disabled: bool,
    ) -> Result<()> {
        // Readiness requires both the dashboard/ping endpoint and the initial sync.
        // A listening socket alone is too early for scenarios to make assertions.
        let h = self;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if cancelled.load(Ordering::SeqCst) {
                let _ = h.child.kill().await;
                h.stopped = true;
                let _ = h.save().await;
                bail!("Interrupted during startup");
            }
            if let Some(status) = h.child.try_wait()? {
                h.stopped = true;
                let _ = h.save().await;
                bail!(
                    "Pool exited during setup ({status}); see {}",
                    h.dir.join("pool.log").display()
                );
            }
            let ready = if dashboard_disabled {
                h.client
                    .get(format!("{}/ping", h.dashboard))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
            } else {
                h.state().await.is_ok()
            };
            if ready && h.mock.uploads().iter().any(|u| u.completed) {
                break;
            }
            if Instant::now() >= deadline {
                let _ = h.save().await;
                let _ = h.child.kill().await;
                h.stopped = true;
                bail!(
                    "Pool startup timed out; see {}",
                    h.dir.join("pool.log").display()
                );
            }
            sleep(Duration::from_millis(25)).await;
        }
        let config: Value = serde_json::from_slice(&fs::read(h.dir.join("config/config.json"))?)?;
        h.port = config["port"].as_u64().context("pool port")? as u16;
        if !dashboard_disabled {
            h.wait_state("empty startup state", |s| {
                s["connections"] == 0 && s["workers"].as_object().is_some_and(|m| m.is_empty())
            })
            .await?;
        }
        Ok(())
    }

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

    pub async fn wait_state(
        &self,
        label: &str,
        predicate: impl Fn(&Value) -> bool,
    ) -> Result<Value> {
        // Poll observable state rather than sleeping for a guessed duration. The
        // last state is included in timeout errors to make failures actionable.
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

    pub async fn empty(&self) -> Result<()> {
        self.wait_state("all connections, stats, and allocations removed", |s| {
            s["connections"] == 0
                && s["workers"].as_object().is_some_and(|m| m.is_empty())
                && s["miner_stats"].as_object().is_some_and(|m| m.is_empty())
        })
        .await?;
        Ok(())
    }

    pub async fn miner(&self, username: &str, tier: &str, rig: &str) -> Result<Miner> {
        self.miner_from(username, tier, rig, None).await
    }

    pub async fn miner_from(
        &self,
        username: &str,
        tier: &str,
        rig: &str,
        ip: Option<&str>,
    ) -> Result<Miner> {
        let socket = if let Some(ip) = ip {
            let socket = TcpSocket::new_v4()?;
            socket.bind(format!("{ip}:0").parse()?)?;
            socket
                .connect(format!("127.0.0.1:{}", self.port).parse()?)
                .await?
        } else {
            TcpStream::connect(("127.0.0.1", self.port)).await?
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

    pub async fn upload_after(
        &self,
        index: usize,
        predicate: impl Fn(&Upload) -> bool,
    ) -> Result<(usize, Upload)> {
        // Search only after the caller's cursor. Sync retry tests can therefore
        // distinguish a new attempt from a previously completed upload.
        let deadline = Instant::now() + WAIT;
        loop {
            for (i, upload) in self.mock.uploads().into_iter().enumerate().skip(index) {
                if predicate(&upload) {
                    anyhow::ensure!(
                        upload.error.is_none(),
                        "Malformed multipart upload: {:?}",
                        upload.error
                    );
                    return Ok((i, upload));
                }
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

    pub async fn refresh(&mut self) -> Result<()> {
        // The injected stdin command calls the production refresh operation and
        // prints a marker only after it finishes.
        self.child
            .stdin
            .as_mut()
            .context("stdin closed")?
            .write_all(b"refresh\n")
            .await?;
        self.wait_log("VERIFY_REFRESH_OK").await
    }
    pub async fn request_shutdown(&mut self) -> Result<()> {
        if let Some(mut input) = self.child.stdin.take() {
            input.write_all(b"shutdown\n").await?;
        }
        Ok(())
    }
    pub async fn wait_stopped(&mut self) -> Result<()> {
        let status = timeout(Duration::from_secs(8), self.child.wait())
            .await
            .context("Shutdown deadline exceeded")??;
        self.stopped = true;
        anyhow::ensure!(status.success(), "Pool shutdown failed: {status}");
        Ok(())
    }
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
    pub async fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        // Never leave a deliberately held upload blocking graceful shutdown.
        self.mock.hold(false);
        self.mock.shared.gate.add_permits(100);
        if let Some(mut input) = self.child.stdin.take() {
            input.write_all(b"shutdown\n").await?;
        }
        match timeout(Duration::from_secs(8), self.child.wait()).await {
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

    pub async fn save(&self) -> Result<()> {
        // Saving is useful both before and after shutdown: live state may disappear
        // on exit, while final logs and mock requests may arrive during cleanup.
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

pub fn stat<'a>(state: &'a Value, rig: &str) -> Option<&'a Value> {
    state["miner_stats"]
        .as_object()?
        .values()
        .find(|v| v["id"] == rig)
}

pub struct Miner {
    reader: BufReader<TcpStream>,
    pub username: String,
    pub tier: String,
    pub rig: String,
    transcript: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug)]
pub struct Job {
    pub difficulty: u64,
    pub nonce: u64,
    pub previous: String,
    pub hash: String,
}

pub fn solve(previous: &str, hash: &str, difficulty: u64) -> Result<u64> {
    // The pool defines a valid nonce as SHA-1(previous || decimal nonce) matching
    // the advertised hash. Test difficulties are bounded to keep failures finite.
    anyhow::ensure!(
        difficulty > 0 && difficulty <= 100_000,
        "Unexpected test-job difficulty {difficulty}"
    );
    for nonce in 1..=difficulty * 100 {
        let mut sha = sha1_smol::Sha1::new();
        sha.update(previous.as_bytes());
        sha.update(nonce.to_string().as_bytes());
        if sha.digest().to_string() == hash {
            return Ok(nonce);
        }
    }
    bail!(
        "Advertised job has no SHA-1 solution in 1..={}",
        difficulty * 100
    )
}

impl Miner {
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.transcript.lock().unwrap().push(format!(
            "{} > {}",
            self.rig,
            String::from_utf8_lossy(data).escape_debug()
        ));
        timeout(WAIT, self.reader.get_mut().write_all(data)).await??;
        Ok(())
    }
    pub async fn bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0; length];
        timeout(WAIT, self.reader.read_exact(&mut bytes)).await??;
        Ok(bytes)
    }
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
        self.transcript
            .lock()
            .unwrap()
            .push(format!("{} < {}", self.rig, line.escape_debug()));
        Ok(line)
    }
    pub async fn job(&mut self) -> Result<Job> {
        self.job_framed("\n").await
    }
    pub async fn job_framed(&mut self, ending: &str) -> Result<Job> {
        self.send(
            format!(
                "JOB,{},{},test-key,temperature:21{ending}",
                self.username, self.tier
            )
            .as_bytes(),
        )
        .await?;
        self.read_job().await
    }
    pub async fn read_job(&mut self) -> Result<Job> {
        let line = self.line().await?;
        let fields: Vec<_> = line.split(',').collect();
        anyhow::ensure!(fields.len() == 3, "Expected job, got {line:?}");
        let difficulty = fields[2].parse()?;
        let nonce = solve(fields[0], fields[1], difficulty)?;
        Ok(Job {
            difficulty,
            nonce,
            previous: fields[0].into(),
            hash: fields[1].into(),
        })
    }
    pub async fn answer(&mut self, job: &Job, correct: bool) -> Result<String> {
        self.answer_fields(
            if correct {
                job.nonce.to_string()
            } else {
                "0".into()
            },
            "1000",
            "chip-verifier",
        )
        .await
    }
    pub async fn answer_fields(
        &mut self,
        nonce: String,
        hashrate: &str,
        chip: &str,
    ) -> Result<String> {
        sleep(Duration::from_millis(3)).await;
        self.send(
            format!(
                "{nonce},{hashrate},Verifier,{},{}{},wallet-verifier\n",
                self.rig, chip, ""
            )
            .as_bytes(),
        )
        .await?;
        self.line().await
    }
    pub async fn share(&mut self) -> Result<String> {
        let job = self.job().await?;
        let feedback = self.answer(&job, true).await?;
        anyhow::ensure!(
            feedback == "GOOD" || feedback == "BLOCK",
            "Expected accepted share, got {feedback:?}"
        );
        Ok(feedback)
    }
    pub async fn close(mut self) -> Result<()> {
        self.reader.get_mut().shutdown().await?;
        Ok(())
    }
    // Zero linger intentionally produces an immediate RST; it never waits on drop.
    #[allow(deprecated)]
    pub fn reset(self) -> Result<()> {
        self.reader.get_ref().set_linger(Some(Duration::ZERO))?;
        drop(self);
        Ok(())
    }
    pub async fn expect_closed(&mut self, duration: Duration) -> Result<()> {
        let mut line = String::new();
        match timeout(duration, self.reader.read_line(&mut line)).await {
            Ok(Ok(0)) => Ok(()),
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
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
