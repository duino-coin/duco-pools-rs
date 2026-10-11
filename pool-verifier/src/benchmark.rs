//! Opt-in end-to-end benchmarks against an explicitly recorded local baseline.
//!
//! Each sample launches a fresh pool built with `--features bench` (the nonce
//! is always 1 and share-rate throttling is off). It connects one or more
//! clients, warms them up, samples memory, and then times a fixed number of
//! shares per client.
//!
//! The timed region contains no observations, sleeps, dashboard polling, or
//! transcript logging. Client hashing and TCP round trips are included.
//!
//! Results are compared with `benchmarks/baseline.json`, which is only written
//! by an explicit `--record-baseline` run.

use crate::{
    harness::{Harness, Options, WAIT},
    prepare,
    session::{self, Paths, RunLock},
};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::Barrier,
    task::JoinSet,
    time::{sleep, timeout},
};

/// Report format version. Bump it when the meaning of a stored field changes.
const SCHEMA: u32 = 3;

/// Workloads in their fixed report order. `serial-*` uses one client and
/// `concurrent-*` uses `--benchmark-clients`. The suffix selects the traffic.
const CASES: [&str; 6] = [
    "serial-accepted",
    "serial-rejected",
    "serial-mixed",
    "concurrent-accepted",
    "concurrent-rejected",
    "concurrent-mixed",
];
// Indices into CASES used to calibrate the rejected-share weight.
const SERIAL_ACCEPTED: usize = 0;
const SERIAL_REJECTED: usize = 1;

/// Upper bound for one sample's timed workload.
const SAMPLE_DEADLINE: Duration = Duration::from_secs(120);

/// Which shares a client deliberately answers wrong.
#[derive(Clone, Copy)]
enum Traffic {
    /// Always the correct nonce (1).
    Accepted,
    /// Always the wrong nonce (0).
    Rejected,
    /// Alternating, starting with an accepted share.
    Mixed,
}

impl Traffic {
    fn for_case(case: &str) -> Self {
        if case.ends_with("-rejected") {
            Self::Rejected
        } else if case.ends_with("-mixed") {
            Self::Mixed
        } else {
            Self::Accepted
        }
    }

    /// Whether the share at `index` (0-based) is answered wrong.
    fn reject(self, index: usize) -> bool {
        match self {
            Self::Accepted => false,
            Self::Rejected => true,
            Self::Mixed => index % 2 == 1,
        }
    }

    /// Expected `(accepted, rejected)` totals across all clients.
    fn counts(self, shares: usize, clients: usize) -> (usize, usize) {
        let rejected = match self {
            Self::Accepted => 0,
            Self::Rejected => shares,
            Self::Mixed => shares / 2,
        };
        ((shares - rejected) * clients, rejected * clients)
    }
}

fn client_count(case: &str, settings: &Settings) -> usize {
    if case.starts_with("serial-") {
        1
    } else {
        settings.clients
    }
}

/// Workload size. Must match between recording and comparing.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    /// Samples per workload.
    samples: usize,
    /// Timed shares per client.
    shares: usize,
    /// Untimed accepted shares per client before the start barrier.
    warmup: usize,
    /// Clients in the `concurrent-*` workloads.
    clients: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            samples: 5,
            shares: 10_000,
            warmup: 100,
            clients: 16,
        }
    }
}

impl Settings {
    fn validate(&self) -> Result<()> {
        ensure!(
            (3..=100).contains(&self.samples),
            "Use 3..=100 benchmark samples"
        );
        ensure!(
            (10_000..=500_000).contains(&self.shares),
            "Use 10_000..=500000 shares per client"
        );
        ensure!(
            (1..=1000).contains(&self.warmup),
            "Use 1..=1000 warmup shares per client"
        );
        ensure!(
            (2..=256).contains(&self.clients),
            "Use 2..=256 concurrent clients"
        );
        Ok(())
    }
}

/// Benchmark-only command-line options.
#[derive(Default)]
pub struct Args {
    /// `--record-baseline`: replace the saved baseline instead of comparing.
    pub record: bool,
    /// Any benchmark option was given (rejected without `--suite benchmark`).
    pub used: bool,
    settings: Settings,
}

impl Args {
    /// Consumes `arg` (and its value) if it is a benchmark option.
    /// Returns `false` for anything else.
    pub fn parse(&mut self, arg: &str, args: &mut impl Iterator<Item = String>) -> Result<bool> {
        match arg {
            "--record-baseline" => self.record = true,
            "--benchmark-samples" => self.settings.samples = number(arg, args)?,
            "--benchmark-shares" => self.settings.shares = number(arg, args)?,
            "--benchmark-warmup" => self.settings.warmup = number(arg, args)?,
            "--benchmark-clients" => self.settings.clients = number(arg, args)?,
            _ => return Ok(false),
        }
        self.used = true;
        Ok(true)
    }
}

fn number(arg: &str, args: &mut impl Iterator<Item = String>) -> Result<usize> {
    args.next()
        .with_context(|| format!("{arg} needs a positive integer"))?
        .parse()
        .with_context(|| format!("Invalid value for {arg}"))
}

pub fn list() {
    for name in CASES {
        println!("benchmark/{name}");
    }
}

// ---- Report data -------------------------------------------------------------

/// Everything that must match for two runs to be comparable.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Environment {
    os: String,
    arch: String,
    logical_cpus: usize,
    rustc: String,
    cargo: String,
    build: String,
    local_cargo_config: String,
    /// Hash of the measurement code. Changes to the harness invalidate an old
    /// baseline.
    harness_sha1: String,
}

/// One measured run of one workload against one fresh pool process.
#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    seconds: f64,
    accepted: usize,
    rejected: usize,
    /// `(accepted + rejected) / seconds`.
    shares_per_second: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    /// Pool RSS after startup, before any miner connects.
    baseline_memory_bytes: u64,
    /// Pool RSS with every client connected and warmed up.
    connected_memory_bytes: u64,
    /// `(connected - baseline) / clients`, clamped at zero.
    memory_per_connection_bytes: f64,
    /// Diagnostic directory for this sample.
    artifacts: PathBuf,
}

/// All samples of one workload.
#[derive(Debug, Serialize, Deserialize)]
struct Measurement {
    case: String,
    samples: Vec<Sample>,
}

impl Measurement {
    fn median_of(&self, metric: impl Fn(&Sample) -> f64) -> f64 {
        median(self.samples.iter().map(metric))
    }

    /// Median of `(accepted + weight * rejected) / seconds`.
    fn weighted_throughput(&self, rejected_weight: f64) -> f64 {
        self.median_of(|s| (s.accepted as f64 + s.rejected as f64 * rejected_weight) / s.seconds)
    }

    fn throughput(&self) -> f64 {
        self.median_of(|s| s.shares_per_second)
    }

    /// Median of the per-sample p95 latency.
    fn latency(&self) -> f64 {
        self.median_of(|s| s.latency_p95_ms)
    }

    fn baseline_memory(&self) -> f64 {
        self.median_of(|s| s.baseline_memory_bytes as f64)
    }

    fn memory_per_connection(&self) -> f64 {
        self.median_of(|s| s.memory_per_connection_bytes)
    }
}

/// A complete benchmark run; also the on-disk baseline format.
#[derive(Debug, Serialize, Deserialize)]
struct Report {
    schema: u32,
    recorded_at_ms: u128,
    environment: Environment,
    settings: Settings,
    /// Hash of the pool working tree, including uncommitted changes.
    source_sha1: String,
    git_revision: String,
    /// Cost of a rejected share relative to an accepted one. It is calibrated
    /// once when recording, then reused from the baseline.
    rejected_weight: f64,
    /// One entry per `CASES` element, in the same order.
    measurements: Vec<Measurement>,
}

impl Report {
    /// Median idle RSS across every sample of every workload.
    fn baseline_memory(&self) -> f64 {
        median(
            self.measurements
                .iter()
                .flat_map(|m| &m.samples)
                .map(|s| s.baseline_memory_bytes as f64),
        )
    }

    /// Median bytes per connection across the concurrent workloads only. With a
    /// single client the delta is too small to rise above page rounding.
    fn memory_per_connection(&self) -> f64 {
        median(
            self.measurements
                .iter()
                .filter(|m| m.case.starts_with("concurrent-"))
                .flat_map(|m| &m.samples)
                .map(|s| s.memory_per_connection_bytes),
        )
    }
}

// ---- Statistics and fingerprints ---------------------------------------------

/// Nearest-rank percentile (for even-length inputs too). Panics on empty input.
fn percentile(values: &[f64], fraction: f64) -> f64 {
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    values[((values.len() as f64 * fraction).ceil() as usize).saturating_sub(1)]
}

fn median(values: impl IntoIterator<Item = f64>) -> f64 {
    percentile(&values.into_iter().collect::<Vec<_>>(), 0.5)
}

/// Relative change as a percentage (`+10.0` means 10 % higher than `baseline`).
fn change_percent(current: f64, baseline: f64) -> f64 {
    (current / baseline - 1.0) * 100.0
}

fn command_output(root: &Path, name: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(name).args(args).current_dir(root).output()?;
    ensure!(
        output.status.success(),
        "{name} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// SHA-1 over the pool's build inputs: relative path and content of every
/// file, in sorted order. Uncommitted changes count.
fn source_hash(root: &Path) -> Result<String> {
    fn add(path: &Path, root: &Path, hash: &mut sha1_smol::Sha1) -> Result<()> {
        ensure!(
            !fs::symlink_metadata(path)?.file_type().is_symlink(),
            "Source symlinks are unsupported"
        );
        if path.is_dir() {
            let mut paths = fs::read_dir(path)?
                .map(|e| e.map(|e| e.path()))
                .collect::<std::io::Result<Vec<_>>>()?;
            paths.sort();
            for path in paths {
                add(&path, root, hash)?;
            }
        } else {
            // Normalize separators so the hash is the same on every OS.
            let relative = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            hash.update(relative.as_bytes());
            hash.update(&[0]);
            hash.update(&fs::read(path)?);
            hash.update(&[0]);
        }
        Ok(())
    }
    let mut hash = sha1_smol::Sha1::new();
    for name in ["src", "Cargo.toml", "Cargo.lock", "build.rs"] {
        add(&root.join(name), root, &mut hash)?;
    }
    Ok(hash.digest().to_string())
}

fn environment(root: &Path) -> Result<Environment> {
    // Every file that affects how a sample is launched or measured.
    let mut harness = sha1_smol::Sha1::new();
    for source in [
        include_str!("benchmark.rs"),
        include_str!("harness.rs"),
        include_str!("mock.rs"),
        include_str!("prepare.rs"),
        include_str!("session.rs"),
    ] {
        harness.update(source.as_bytes());
    }
    Ok(Environment {
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        logical_cpus: std::thread::available_parallelism()?.get(),
        rustc: command_output(root, "rustc", &["-Vv"])?,
        cargo: command_output(root, "cargo", &["-V"])?,
        build: "release --locked --features bench".into(),
        local_cargo_config: [".cargo/config", ".cargo/config.toml"]
            .iter()
            .map(|name| fs::read_to_string(root.join(name)).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n"),
        harness_sha1: harness.digest().to_string(),
    })
}

/// Checks that `baseline` is complete, internally consistent, and was recorded
/// with the same settings and environment. It never fabricates missing values.
fn validate_baseline(
    baseline: &Report,
    settings: &Settings,
    environment: &Environment,
) -> Result<()> {
    let same_schema = baseline.schema == SCHEMA;
    let same_settings = &baseline.settings == settings;
    let same_environment = &baseline.environment == environment;
    ensure!(
        same_schema && same_settings && same_environment,
        "Baseline is incompatible (schema matches: {same_schema}, settings match: {same_settings}, environment/harness match: {same_environment}); use the original settings or explicitly --record-baseline"
    );
    ensure!(
        baseline.measurements.len() == CASES.len(),
        "Incomplete baseline"
    );
    ensure!(
        baseline.rejected_weight.is_finite() && baseline.rejected_weight > 0.0,
        "Invalid rejected-share weight"
    );
    for (case, measurement) in CASES.iter().zip(&baseline.measurements) {
        ensure!(
            measurement.case == *case && measurement.samples.len() == settings.samples,
            "Invalid baseline case or sample count"
        );
        let expected_counts =
            Traffic::for_case(case).counts(settings.shares, client_count(case, settings));
        for sample in &measurement.samples {
            ensure!(
                (sample.accepted, sample.rejected) == expected_counts,
                "Invalid baseline outcome counts"
            );
            for value in [
                sample.seconds,
                sample.shares_per_second,
                sample.latency_p50_ms,
                sample.latency_p95_ms,
                sample.baseline_memory_bytes as f64,
                sample.connected_memory_bytes as f64,
            ] {
                ensure!(value.is_finite() && value > 0.0, "Invalid baseline metric");
            }
            ensure!(
                sample.memory_per_connection_bytes.is_finite()
                    && sample.memory_per_connection_bytes >= 0.0,
                "Invalid per-connection memory metric"
            );
            let expected = (sample.accepted + sample.rejected) as f64 / sample.seconds;
            ensure!(
                (expected - sample.shares_per_second).abs() <= expected * 1e-9,
                "Inconsistent baseline throughput"
            );
        }
    }
    ensure!(
        baseline.baseline_memory() > 0.0 && baseline.memory_per_connection() > 0.0,
        "Baseline needs positive aggregate memory measurements"
    );
    Ok(())
}

// ---- Measurement -------------------------------------------------------------

/// Median of five RSS readings of the pool process, 20 ms apart.
async fn resident_memory(pid: u32) -> Result<u64> {
    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    let mut readings = Vec::with_capacity(5);
    for _ in 0..5 {
        let updated = system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_memory().without_tasks(),
        );
        ensure!(
            updated == 1,
            "Pool process {pid} disappeared during memory measurement"
        );
        readings.push(
            system
                .process(pid)
                .context("Pool process missing after memory refresh")?
                .memory(),
        );
        sleep(Duration::from_millis(20)).await;
    }
    readings.sort_unstable();
    Ok(readings[readings.len() / 2])
}

/// A minimal benchmark miner. Unlike `harness::Miner` it does not log, sleep,
/// or brute-force nonces.
struct Client {
    stream: BufReader<TcpStream>,
}

impl Client {
    async fn connect(port: u16) -> Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port)).await?;
        stream.set_nodelay(true)?;
        let mut client = Self {
            stream: BufReader::new(stream),
        };
        ensure!(client.line().await? == "3.0", "Unexpected greeting");
        Ok(client)
    }

    async fn line(&mut self) -> Result<String> {
        let mut line = String::new();
        timeout(WAIT, self.stream.read_line(&mut line))
            .await
            .context("Benchmark response timeout")??;
        ensure!(
            !line.is_empty() && line.len() <= 1024,
            "Missing or oversized pool response"
        );
        Ok(line.trim_end_matches(['\r', '\n']).to_owned())
    }

    /// Requests and answers one job. Returns `true` if accepted.
    ///
    /// Any reply other than the expected one (acceptance, or `BAD,Incorrect
    /// result` when `reject`) fails the sample rather than inflating throughput.
    async fn share(&mut self, reject: bool) -> Result<bool> {
        self.stream
            .get_mut()
            .write_all(b"JOB,benchmark,AVR,test-key\n")
            .await?;
        let line = self.line().await?;
        let fields = line.split(',').collect::<Vec<_>>();
        let [previous, hash, difficulty] = fields[..] else {
            bail!("Invalid job: {line}");
        };
        let difficulty: u64 = difficulty.parse()?;

        // The bench feature fixes the nonce at 1, so verify instead of searching.
        let mut expected = sha1_smol::Sha1::new();
        expected.update(previous.as_bytes());
        expected.update(b"1");
        ensure!(
            expected.digest().to_string() == hash && difficulty > 0,
            "bench feature did not issue nonce 1"
        );

        let nonce = if reject { 0 } else { 1 };
        self.stream
            .get_mut()
            .write_all(
                format!("{nonce},1000,Benchmark,rig,chip-benchmark,wallet-benchmark\n").as_bytes(),
            )
            .await?;
        let feedback = self.line().await?;
        let accepted = feedback == "GOOD" || feedback == "BLOCK";
        let expected_feedback = if reject {
            feedback == "BAD,Incorrect result"
        } else {
            accepted
        };
        ensure!(
            expected_feedback,
            "Unexpected benchmark outcome (expected rejection={reject}): {feedback}"
        );
        Ok(accepted)
    }
}

/// What one client task reports after the timed region.
struct ClientResult {
    /// Kept open until every client has finished, so the pool's connection
    /// count does not drop during the measurement.
    _client: Client,
    latencies_ms: Vec<f64>,
    accepted: usize,
    rejected: usize,
    finished: Instant,
}

/// Runs one workload against an already-started pool.
///
/// Sequence: idle RSS, then connect and warm every client (untimed), then
/// connected RSS, then a barrier releases all clients at once and the timed
/// region runs until the last client finishes.
async fn workload(
    pid: u32,
    port: u16,
    clients: usize,
    traffic: Traffic,
    settings: &Settings,
) -> Result<Sample> {
    let baseline_memory_bytes = resident_memory(pid).await?;
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let mut client = Client::connect(port).await?;
        for _ in 0..settings.warmup {
            client.share(false).await?;
        }
        let barrier = barrier.clone();
        let shares = settings.shares;
        tasks.spawn(async move {
            let mut latencies_ms = Vec::with_capacity(shares);
            let (mut accepted, mut rejected) = (0, 0);
            barrier.wait().await;
            for index in 0..shares {
                let start = Instant::now();
                let outcome = client.share(traffic.reject(index)).await?;
                latencies_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                if outcome {
                    accepted += 1;
                } else {
                    rejected += 1;
                }
            }
            Ok::<_, anyhow::Error>(ClientResult {
                _client: client,
                latencies_ms,
                accepted,
                rejected,
                finished: Instant::now(),
            })
        });
    }

    // Memory sampling happens before the barrier, so it is outside the timed region.
    let connected_memory_bytes = resident_memory(pid).await?;
    let memory_per_connection_bytes =
        connected_memory_bytes.saturating_sub(baseline_memory_bytes) as f64 / clients as f64;

    barrier.wait().await;
    let start = Instant::now();
    let mut finished = start;
    let (mut accepted, mut rejected) = (0, 0);
    let mut latencies = Vec::with_capacity(clients * settings.shares);
    let mut open_clients = Vec::with_capacity(clients);
    while let Some(result) = tasks.join_next().await {
        let mut result = result??;
        accepted += result.accepted;
        rejected += result.rejected;
        finished = finished.max(result.finished);
        latencies.append(&mut result.latencies_ms);
        open_clients.push(result);
    }
    let seconds = finished.duration_since(start).as_secs_f64();
    ensure!(
        (accepted, rejected) == traffic.counts(settings.shares, clients)
            && latencies.len() == accepted + rejected,
        "Incomplete measured workload"
    );
    drop(open_clients);

    Ok(Sample {
        seconds,
        accepted,
        rejected,
        shares_per_second: latencies.len() as f64 / seconds,
        latency_p50_ms: percentile(&latencies, 0.5),
        latency_p95_ms: percentile(&latencies, 0.95),
        baseline_memory_bytes,
        connected_memory_bytes,
        memory_per_connection_bytes,
        artifacts: PathBuf::new(),
    })
}

/// Launches a fresh pool, runs one workload, and always stops and reaps the
/// child, even when the measurement fails.
async fn sample(
    binary: &Path,
    dir: PathBuf,
    clients: usize,
    traffic: Traffic,
    settings: &Settings,
    cancelled: &AtomicBool,
) -> Result<Sample> {
    let mut harness = Harness::start(binary, dir.clone(), Options::default(), cancelled).await?;
    let pid = harness.process_id()?;
    let measured = tokio::select! {
        result = timeout(SAMPLE_DEADLINE, workload(pid, harness.port, clients, traffic, settings)) => {
            result.unwrap_or_else(|_| Err(anyhow::anyhow!("Benchmark sample exceeded 120 seconds")))
        }
        _ = session::interrupted(cancelled) => Err(anyhow::anyhow!("Interrupted")),
    };
    let cleanup_errors = harness.finish().await;
    match measured {
        Ok(mut sample) if cleanup_errors.is_empty() => {
            sample.artifacts = dir;
            Ok(sample)
        }
        measured => {
            let errors: Vec<_> = measured
                .err()
                .into_iter()
                .chain(cleanup_errors)
                .map(|e| format!("{e:#}"))
                .collect();
            bail!("{}", errors.join("; "))
        }
    }
}

// ---- Run pipeline ------------------------------------------------------------

pub async fn run(args: Args) -> Result<i32> {
    ensure!(!cfg!(debug_assertions), "Run benchmarks with cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite benchmark");
    args.settings.validate()?;
    let paths = Paths::discover()?;
    let _lock = RunLock::acquire(&paths.work)?;
    let baseline_path = paths.home.join("benchmarks/baseline.json");
    let environment = environment(&paths.root)?;

    // Validate the baseline before spending minutes on measurements.
    let baseline = if args.record {
        None
    } else {
        Some(load_baseline(&baseline_path, &args.settings, &environment)?)
    };
    let cancelled = session::interrupt_flag();
    let (dir, mut report) = start_report(&paths, &args.settings, environment, baseline.as_ref())?;

    let (root, build_home, flag) = (
        paths.root.clone(),
        paths.work.join("benchmark"),
        cancelled.clone(),
    );
    let built =
        tokio::task::spawn_blocking(move || prepare::build_benchmark(&root, &build_home, flag))
            .await?;
    if cancelled.load(Ordering::SeqCst) {
        return Ok(130);
    }
    let binary = built?;
    ensure!(
        source_hash(&paths.root)? == report.source_sha1,
        "Pool source changed while building; rerun benchmark"
    );

    println!("Benchmarking current pool with --features bench ({} samples, {} shares/client); artifacts: {}", args.settings.samples, args.settings.shares, dir.display());
    if let Some(exit_code) =
        measure_all(&binary, &dir, &mut report, &args.settings, &cancelled).await?
    {
        return Ok(exit_code);
    }
    ensure!(
        source_hash(&paths.root)? == report.source_sha1,
        "Pool source changed during measurement; baseline was not saved"
    );

    if baseline.is_none() {
        // Serial cost per share = 1 / throughput, so the rejected/accepted cost
        // ratio is accepted throughput / rejected throughput. It is calibrated
        // once and then frozen in the baseline for all future comparisons.
        report.rejected_weight = report.measurements[SERIAL_ACCEPTED].throughput()
            / report.measurements[SERIAL_REJECTED].throughput();
    }
    validate_baseline(&report, &args.settings, &report.environment)?;
    fs::write(
        dir.join("benchmark.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!(
        "Cost weights: accepted=1.000, rejected={:.3} (baseline serial end-to-end cost)",
        report.rejected_weight
    );
    if cancelled.load(Ordering::SeqCst) {
        return Ok(130);
    }

    write_summary(&dir, &report)?;
    match baseline {
        Some(baseline) => compare(&dir, &report, &baseline)?,
        None => save_baseline(&baseline_path, &paths.home, report)?,
    }
    println!("Benchmark report: {}", dir.join("benchmark.json").display());
    Ok(0)
}

fn load_baseline(path: &Path, settings: &Settings, environment: &Environment) -> Result<Report> {
    let data = fs::read(path)
        .context("No measured baseline. Run --suite benchmark --record-baseline first")?;
    let baseline = serde_json::from_slice(&data).context("Invalid baseline JSON")?;
    validate_baseline(&baseline, settings, environment)?;
    Ok(baseline)
}

/// Creates `runs/benchmark-<millis>-<pid>/` and an empty report, and writes
/// `metadata.json` before anything is built.
fn start_report(
    paths: &Paths,
    settings: &Settings,
    environment: Environment,
    baseline: Option<&Report>,
) -> Result<(PathBuf, Report)> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let dir = paths
        .home
        .join("runs")
        .join(format!("benchmark-{now}-{}", std::process::id()));
    fs::create_dir_all(&dir)?;
    fs::write(
        paths.work.join("latest-run.txt"),
        dir.to_string_lossy().as_bytes(),
    )?;
    let report = Report {
        schema: SCHEMA,
        // Reuse the frozen weight when comparing; recording calibrates it later.
        rejected_weight: baseline.map_or(1.0, |b| b.rejected_weight),
        recorded_at_ms: now,
        environment,
        settings: settings.clone(),
        source_sha1: source_hash(&paths.root)?,
        git_revision: command_output(&paths.root, "git", &["rev-parse", "HEAD"])
            .unwrap_or_else(|_| "unavailable".into()),
        measurements: CASES
            .iter()
            .map(|case| Measurement {
                case: (*case).into(),
                samples: vec![],
            })
            .collect(),
    };
    fs::write(
        dir.join("metadata.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok((dir, report))
}

/// Runs every sample of every workload.
///
/// Returns `Some(exit code)` if a sample failed. The failure is then recorded
/// in `failure.json`.
async fn measure_all(
    binary: &Path,
    dir: &Path,
    report: &mut Report,
    settings: &Settings,
    cancelled: &AtomicBool,
) -> Result<Option<i32>> {
    for round in 0..settings.samples {
        // Reverse the workload order on alternate rounds to reduce warm-machine bias.
        let order: Vec<usize> = if round % 2 == 0 {
            (0..CASES.len()).collect()
        } else {
            (0..CASES.len()).rev().collect()
        };
        for index in order {
            let case = CASES[index];
            let artifact = dir.join(format!("{case}-{}", round + 1));
            print!("  {case} sample {}/{} ... ", round + 1, settings.samples);
            std::io::stdout().flush()?;
            let measured = sample(
                binary,
                artifact.clone(),
                client_count(case, settings),
                Traffic::for_case(case),
                settings,
                cancelled,
            )
            .await;
            match measured {
                Ok(sample) => {
                    println!(
                        "{:.0} shares/s ({} accepted, {} rejected), p95 {:.3} ms, {:.2} MiB + {:.1} KiB/connection",
                        sample.shares_per_second,
                        sample.accepted,
                        sample.rejected,
                        sample.latency_p95_ms,
                        sample.baseline_memory_bytes as f64 / 1_048_576.0,
                        sample.memory_per_connection_bytes / 1024.0
                    );
                    report.measurements[index].samples.push(sample);
                }
                Err(error) => {
                    println!("FAILED: {error:#}");
                    let failure = json!({
                        "case": case,
                        "sample": round + 1,
                        "error": format!("{error:#}"),
                        "artifacts": artifact,
                    });
                    fs::write(
                        dir.join("failure.json"),
                        serde_json::to_vec_pretty(&failure)?,
                    )?;
                    let interrupted = cancelled.load(Ordering::SeqCst);
                    return Ok(Some(if interrupted { 130 } else { 1 }));
                }
            }
            // Keep partial progress on disk after every sample.
            fs::write(
                dir.join("benchmark.json"),
                serde_json::to_vec_pretty(report)?,
            )?;
        }
    }
    Ok(None)
}

fn write_summary(dir: &Path, report: &Report) -> Result<()> {
    let measurements: Vec<_> = report
        .measurements
        .iter()
        .map(|m| {
            json!({
                "case": m.case,
                "median_shares_per_second": m.throughput(),
                "median_weighted_shares_per_second": m.weighted_throughput(report.rejected_weight),
                "median_p95_ms": m.latency(),
                "median_baseline_memory_bytes": m.baseline_memory(),
                "median_memory_per_connection_bytes": m.memory_per_connection(),
                "accepted": m.samples.iter().map(|s| s.accepted).sum::<usize>(),
                "rejected": m.samples.iter().map(|s| s.rejected).sum::<usize>(),
            })
        })
        .collect();
    let summary = json!({
        "weights": {"accepted": 1.0, "rejected": report.rejected_weight},
        "memory": {
            "baseline_bytes": report.baseline_memory(),
            "bytes_per_connection": report.memory_per_connection(),
        },
        "measurements": measurements,
    });
    fs::write(
        dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(())
}

/// Compares one workload with its baseline, prints the result, and returns it as JSON.
fn compare_measurement(current: &Measurement, old: &Measurement, rejected_weight: f64) -> Value {
    let throughput_ratio = current.throughput() / old.throughput();
    let weighted_current = current.weighted_throughput(rejected_weight);
    let weighted_baseline = old.weighted_throughput(rejected_weight);
    let latency_ratio = current.latency() / old.latency();
    println!("  {}:", current.case);
    println!(
        "    throughput: {:.0} shares/s current vs {:.0} baseline ({:+.2}%)",
        current.throughput(),
        old.throughput(),
        change_percent(current.throughput(), old.throughput())
    );
    println!(
        "    weighted throughput: {:.0} shares/s current vs {:.0} baseline ({:+.2}%)",
        weighted_current,
        weighted_baseline,
        change_percent(weighted_current, weighted_baseline)
    );
    println!(
        "    p95 latency: {:.3} ms current vs {:.3} ms baseline ({:+.2}%)",
        current.latency(),
        old.latency(),
        change_percent(current.latency(), old.latency())
    );
    json!({
        "case": current.case,
        "baseline_shares_per_second": old.throughput(),
        "current_shares_per_second": current.throughput(),
        "throughput_ratio": throughput_ratio,
        "throughput_change_percent": (throughput_ratio - 1.0) * 100.0,
        "baseline_p95_ms": old.latency(),
        "current_p95_ms": current.latency(),
        "latency_ratio": latency_ratio,
        "baseline_weighted_shares_per_second": weighted_baseline,
        "current_weighted_shares_per_second": weighted_current,
        "weighted_throughput_ratio": weighted_current / weighted_baseline,
    })
}

/// Prints the comparison with the baseline and writes `comparison.json`.
fn compare(dir: &Path, report: &Report, baseline: &Report) -> Result<()> {
    println!("Comparison to measured baseline:");
    let comparisons: Vec<_> = report
        .measurements
        .iter()
        .zip(&baseline.measurements)
        .map(|(current, old)| compare_measurement(current, old, report.rejected_weight))
        .collect();

    let baseline_memory = baseline.baseline_memory();
    let current_memory = report.baseline_memory();
    let baseline_per_connection = baseline.memory_per_connection();
    let current_per_connection = report.memory_per_connection();
    println!(
        "  idle memory: {:.2} MiB current vs {:.2} MiB baseline ({:+.2}%)",
        current_memory / 1_048_576.0,
        baseline_memory / 1_048_576.0,
        change_percent(current_memory, baseline_memory),
    );
    println!(
        "  memory per connection: {:.1} KiB current vs {:.1} KiB baseline ({:+.2}%)",
        current_per_connection / 1024.0,
        baseline_per_connection / 1024.0,
        change_percent(current_per_connection, baseline_per_connection),
    );

    let comparison = json!({
        "baseline_recorded_at_ms": baseline.recorded_at_ms,
        "baseline_source_sha1": baseline.source_sha1,
        "current_source_sha1": report.source_sha1,
        "weights": {"accepted": 1.0, "rejected": report.rejected_weight},
        "memory": {
            "baseline_idle_bytes": baseline_memory,
            "current_idle_bytes": current_memory,
            "idle_ratio": current_memory / baseline_memory,
            "baseline_bytes_per_connection": baseline_per_connection,
            "current_bytes_per_connection": current_per_connection,
            "per_connection_ratio": current_per_connection / baseline_per_connection,
        },
        "comparisons": comparisons,
    });
    fs::write(
        dir.join("comparison.json"),
        serde_json::to_vec_pretty(&comparison)?,
    )?;
    Ok(())
}

/// Atomically replaces the baseline file with `report`.
///
/// Only reached after every measurement and cleanup succeeded. Artifact paths
/// are stored relative to `benchmarks/baseline.json` (`../runs/...`), so the
/// file can move with the verifier directory without exposing local paths.
fn save_baseline(path: &Path, home: &Path, mut report: Report) -> Result<()> {
    fs::create_dir_all(path.parent().unwrap())?;
    for sample in report.measurements.iter_mut().flat_map(|m| &mut m.samples) {
        let relative = sample.artifacts.strip_prefix(home)?;
        sample.artifacts = PathBuf::from(format!(
            "../{}",
            relative.to_string_lossy().replace('\\', "/")
        ));
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&report)?)?;
    fs::rename(temporary, path)?;
    println!("Measured baseline saved: {}", path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_throughput_counts_both_outcomes_using_fixed_cost() {
        let mut report = fixture();
        let sample = &mut report.measurements[2].samples[0];
        sample.accepted = 60;
        sample.rejected = 40;
        sample.seconds = 2.0;
        report.measurements[2].samples.truncate(1);
        assert_eq!(report.measurements[2].weighted_throughput(0.5), 40.0);
        assert_eq!(Traffic::Mixed.counts(5, 3), (9, 6));
        assert!(Traffic::Rejected.reject(0));
        assert!(!Traffic::Mixed.reject(0));
        assert!(Traffic::Mixed.reject(1));
    }

    #[test]
    fn statistics_use_nearest_rank() {
        assert_eq!(percentile(&[5., 1., 4., 2., 3.], 0.5), 3.);
        assert_eq!(percentile(&[5., 1., 4., 2., 3.], 0.95), 5.);
        assert_eq!(percentile(&[4., 1., 3., 2.], 0.5), 2.);
    }

    #[test]
    fn memory_summary_uses_idle_and_concurrent_connection_samples() {
        let mut report = fixture();
        report.measurements[0].samples[0].baseline_memory_bytes = 9_000_000;
        report.measurements[3].samples[0].memory_per_connection_bytes = 120_000.0;
        assert_eq!(report.baseline_memory(), 10_000_000.0);
        assert_eq!(report.memory_per_connection(), 100_000.0);
    }

    #[test]
    fn settings_bound_workload() {
        assert!(Settings::default().validate().is_ok());
        for settings in [
            Settings {
                samples: 0,
                ..Settings::default()
            },
            Settings {
                shares: 501_000,
                ..Settings::default()
            },
            Settings {
                warmup: 0,
                ..Settings::default()
            },
            Settings {
                clients: 1,
                ..Settings::default()
            },
        ] {
            assert!(settings.validate().is_err());
        }
    }

    /// A complete, valid report with identical samples.
    fn fixture() -> Report {
        let settings = Settings::default();
        Report {
            schema: SCHEMA,
            rejected_weight: 1.0,
            recorded_at_ms: 1,
            source_sha1: "source".into(),
            git_revision: "revision".into(),
            environment: Environment {
                os: "test".into(),
                arch: "test".into(),
                logical_cpus: 4,
                rustc: "test".into(),
                cargo: "test".into(),
                build: "bench".into(),
                local_cargo_config: String::new(),
                harness_sha1: "test".into(),
            },
            measurements: CASES
                .iter()
                .map(|case| Measurement {
                    case: (*case).into(),
                    samples: (0..settings.samples)
                        .map(|_| {
                            let (accepted, rejected) = Traffic::for_case(case)
                                .counts(settings.shares, client_count(case, &settings));
                            Sample {
                                seconds: 1.0,
                                accepted,
                                rejected,
                                shares_per_second: (accepted + rejected) as f64,
                                latency_p50_ms: 0.1,
                                latency_p95_ms: 0.2,
                                baseline_memory_bytes: 10_000_000,
                                connected_memory_bytes: 10_000_000
                                    + 100_000 * client_count(case, &settings) as u64,
                                memory_per_connection_bytes: 100_000.0,
                                artifacts: PathBuf::new(),
                            }
                        })
                        .collect(),
                })
                .collect(),
            settings,
        }
    }

    #[test]
    fn baseline_rejects_incompatible_and_incomplete_measurements() {
        let mut baseline = fixture();
        assert!(validate_baseline(&baseline, &baseline.settings, &baseline.environment).is_ok());
        let mut settings = baseline.settings.clone();
        settings.clients += 1;
        assert!(validate_baseline(&baseline, &settings, &baseline.environment).is_err());
        let mut environment = baseline.environment.clone();
        environment.build = "other".into();
        assert!(validate_baseline(&baseline, &baseline.settings, &environment).is_err());
        baseline.measurements[0].samples[0].shares_per_second *= 2.0;
        assert!(validate_baseline(&baseline, &baseline.settings, &baseline.environment).is_err());
        let mut baseline = fixture();
        baseline.measurements[3].samples[0].memory_per_connection_bytes = f64::NAN;
        assert!(validate_baseline(&baseline, &baseline.settings, &baseline.environment).is_err());
        let mut baseline = fixture();
        baseline.measurements[0].samples.pop();
        assert!(validate_baseline(&baseline, &baseline.settings, &baseline.environment).is_err());
        let mut baseline = fixture();
        baseline.measurements.pop();
        assert!(validate_baseline(&baseline, &baseline.settings, &baseline.environment).is_err());
    }

    #[tokio::test]
    async fn feedback_outcomes_are_validated_and_counted() {
        for (reject, feedback, expected) in [
            (false, "GOOD", Some(true)),
            (true, "BAD,Incorrect result", Some(false)),
            (false, "BAD,Incorrect result", None),
            (true, "GOOD", None),
            (true, "BAD,Too low difficulty", None),
        ] {
            // A one-shot fake pool: greet, issue a nonce-1 job, reply `feedback`.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                stream.get_mut().write_all(b"3.0\n").await.unwrap();
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                let hash = sha1_smol::Sha1::from("previous1").digest();
                stream
                    .get_mut()
                    .write_all(format!("previous,{hash},1\n").as_bytes())
                    .await
                    .unwrap();
                line.clear();
                stream.read_line(&mut line).await.unwrap();
                assert!(line.starts_with(if reject { "0," } else { "1," }));
                stream
                    .get_mut()
                    .write_all(format!("{feedback}\n").as_bytes())
                    .await
                    .unwrap();
            });
            let mut client = Client::connect(port).await.unwrap();
            assert_eq!(client.share(reject).await.ok(), expected);
            server.await.unwrap();
        }
    }
}
