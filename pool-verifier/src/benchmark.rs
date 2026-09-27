//! Opt-in end-to-end benchmarks against an explicitly recorded local baseline.
//! No observations, sleeps, dashboard polling, or miner transcript logging occur
//! in the timed region. Client hashing and TCP round trips are included.
use crate::{
    harness::{Harness, Options, WAIT},
    prepare, RunLock,
};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs,
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

const SCHEMA: u32 = 3;
const CASES: [&str; 6] = [
    "serial-accepted",
    "serial-rejected",
    "serial-mixed",
    "concurrent-accepted",
    "concurrent-rejected",
    "concurrent-mixed",
];

#[derive(Clone, Copy)]
enum Traffic {
    Accepted,
    Rejected,
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
    fn reject(self, index: usize) -> bool {
        match self {
            Self::Accepted => false,
            Self::Rejected => true,
            Self::Mixed => index % 2 == 1,
        }
    }
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    samples: usize,
    shares: usize,
    warmup: usize,
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
#[derive(Default)]
pub struct Args {
    pub record: bool,
    pub used: bool,
    settings: Settings,
}
impl Args {
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
pub fn list() {
    for name in CASES {
        println!("benchmark/{name}");
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Environment {
    os: String,
    arch: String,
    logical_cpus: usize,
    rustc: String,
    cargo: String,
    build: String,
    local_cargo_config: String,
    // Changes to the measurement harness invalidate an old baseline.
    harness_sha1: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    seconds: f64,
    accepted: usize,
    rejected: usize,
    shares_per_second: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    baseline_memory_bytes: u64,
    connected_memory_bytes: u64,
    memory_per_connection_bytes: f64,
    artifacts: PathBuf,
}
#[derive(Debug, Serialize, Deserialize)]
struct Measurement {
    case: String,
    samples: Vec<Sample>,
}
impl Measurement {
    fn weighted_throughput(&self, rejected_weight: f64) -> f64 {
        percentile(
            &self
                .samples
                .iter()
                .map(|sample| {
                    (sample.accepted as f64 + sample.rejected as f64 * rejected_weight)
                        / sample.seconds
                })
                .collect::<Vec<_>>(),
            0.5,
        )
    }
    fn throughput(&self) -> f64 {
        percentile(
            &self
                .samples
                .iter()
                .map(|s| s.shares_per_second)
                .collect::<Vec<_>>(),
            0.5,
        )
    }
    fn latency(&self) -> f64 {
        percentile(
            &self
                .samples
                .iter()
                .map(|s| s.latency_p95_ms)
                .collect::<Vec<_>>(),
            0.5,
        )
    }

    fn baseline_memory(&self) -> f64 {
        percentile(
            &self
                .samples
                .iter()
                .map(|s| s.baseline_memory_bytes as f64)
                .collect::<Vec<_>>(),
            0.5,
        )
    }

    fn memory_per_connection(&self) -> f64 {
        percentile(
            &self
                .samples
                .iter()
                .map(|s| s.memory_per_connection_bytes)
                .collect::<Vec<_>>(),
            0.5,
        )
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct Report {
    schema: u32,
    recorded_at_ms: u128,
    environment: Environment,
    settings: Settings,
    source_sha1: String,
    git_revision: String,
    rejected_weight: f64,
    measurements: Vec<Measurement>,
}
impl Report {
    fn baseline_memory(&self) -> f64 {
        percentile(
            &self
                .measurements
                .iter()
                .flat_map(|measurement| &measurement.samples)
                .map(|sample| sample.baseline_memory_bytes as f64)
                .collect::<Vec<_>>(),
            0.5,
        )
    }

    fn memory_per_connection(&self) -> f64 {
        percentile(
            &self
                .measurements
                .iter()
                .filter(|measurement| measurement.case.starts_with("concurrent-"))
                .flat_map(|measurement| &measurement.samples)
                .map(|sample| sample.memory_per_connection_bytes)
                .collect::<Vec<_>>(),
            0.5,
        )
    }
}

fn percentile(values: &[f64], fraction: f64) -> f64 {
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    // Nearest rank, including even-length sample sets.
    values[((values.len() as f64 * fraction).ceil() as usize).saturating_sub(1)]
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
            hash.update(
                path.strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/")
                    .as_bytes(),
            );
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
    let mut hash = sha1_smol::Sha1::new();
    for source in [
        include_str!("benchmark.rs"),
        include_str!("harness.rs"),
        include_str!("mock.rs"),
        include_str!("prepare.rs"),
    ] {
        hash.update(source.as_bytes());
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
        harness_sha1: hash.digest().to_string(),
    })
}
fn validate_baseline(
    baseline: &Report,
    settings: &Settings,
    environment: &Environment,
) -> Result<()> {
    ensure!(baseline.schema == SCHEMA && &baseline.settings == settings && &baseline.environment == environment,
        format!("Baseline settings ({}), environment ({}), or harness differ ({}); use the original settings or explicitly --record-baseline", baseline.schema == SCHEMA, &baseline.settings == settings, &baseline.environment == environment));
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
    async fn share(&mut self, reject: bool) -> Result<bool> {
        self.stream
            .get_mut()
            .write_all(b"JOB,benchmark,AVR,test-key\n")
            .await?;
        let line = self.line().await?;
        let fields = line.split(',').collect::<Vec<_>>();
        ensure!(fields.len() == 3, "Invalid job: {line}");
        let difficulty: u64 = fields[2].parse()?;
        let nonce = {
            // The real bench feature fixes the nonce at one; do not brute-force it.
            let mut hash = sha1_smol::Sha1::new();
            hash.update(fields[0].as_bytes());
            hash.update(b"1");
            ensure!(
                hash.digest().to_string() == fields[1] && difficulty > 0,
                "bench feature did not issue nonce 1"
            );
            if reject {
                0
            } else {
                1
            }
        };
        self.stream
            .get_mut()
            .write_all(
                format!("{nonce},1000,Benchmark,rig,chip-benchmark,wallet-benchmark\n").as_bytes(),
            )
            .await?;
        let feedback = self.line().await?;
        let accepted = feedback == "GOOD" || feedback == "BLOCK";
        ensure!(
            if reject {
                feedback == "BAD,Incorrect result"
            } else {
                accepted
            },
            "Unexpected benchmark outcome (expected rejection={reject}): {feedback}"
        );
        Ok(accepted)
    }
}

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
    // Warm every connection before measuring; all workers start together.
    for _ in 0..clients {
        let mut client = Client::connect(port).await?;
        for _ in 0..settings.warmup {
            client.share(false).await?;
        }
        let barrier = barrier.clone();
        let shares = settings.shares;
        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(shares);
            barrier.wait().await;
            let mut accepted = 0;
            let mut rejected = 0;
            for index in 0..shares {
                let start = Instant::now();
                let outcome = client.share(traffic.reject(index)).await?;
                latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                if outcome {
                    accepted += 1;
                } else {
                    rejected += 1;
                }
            }
            // Keep the socket alive until all timed work has completed.
            Ok::<_, anyhow::Error>((client, latencies, accepted, rejected, Instant::now()))
        });
    }
    let mut sockets = Vec::with_capacity(clients);
    let mut latencies = Vec::with_capacity(clients * settings.shares);
    let start = Instant::now();
    let mut finished = start;
    let mut accepted = 0;
    let mut rejected = 0;
    let connected_memory_bytes = resident_memory(pid).await?;
    let memory_per_connection_bytes =
        connected_memory_bytes.saturating_sub(baseline_memory_bytes) as f64 / clients as f64;
    barrier.wait().await;
    while let Some(result) = tasks.join_next().await {
        let (client, mut times, good, bad, end) = result??;
        accepted += good;
        rejected += bad;
        finished = finished.max(end);
        sockets.push(client);
        latencies.append(&mut times);
    }
    let seconds = finished.duration_since(start).as_secs_f64();
    ensure!(
        (accepted, rejected) == traffic.counts(settings.shares, clients)
            && latencies.len() == accepted + rejected,
        "Incomplete measured workload"
    );
    drop(sockets);
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

async fn sample(
    binary: &Path,
    dir: PathBuf,
    clients: usize,
    traffic: Traffic,
    settings: &Settings,
    cancelled: &AtomicBool,
) -> Result<Sample> {
    let mut h = Harness::start(binary, dir.clone(), Options::default(), cancelled).await?;
    let pid = h.process_id()?;
    let result = tokio::select! {
        result = timeout(Duration::from_secs(120), workload(pid, h.port, clients, traffic, settings)) => result.unwrap_or_else(|_| Err(anyhow::anyhow!("Benchmark sample exceeded 120 seconds"))),
        _ = async { while !cancelled.load(Ordering::SeqCst) { sleep(Duration::from_millis(50)).await; } } => Err(anyhow::anyhow!("Interrupted")),
    };
    // Always stop and reap the child even when the measurement fails.
    let save_before = h.save().await;
    let stop = h.stop().await;
    let save_after = h.save().await;
    let mut errors = vec![];
    for check in [save_before, stop, save_after] {
        if let Err(error) = check {
            errors.push(format!("{error:#}"));
        }
    }
    match result {
        Ok(mut result) if errors.is_empty() => {
            result.artifacts = dir;
            Ok(result)
        }
        result => {
            if let Err(error) = result {
                errors.insert(0, format!("{error:#}"));
            }
            bail!("{}", errors.join("; "))
        }
    }
}

pub async fn run(args: Args) -> Result<i32> {
    ensure!(!cfg!(debug_assertions), "Run benchmarks with cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite benchmark");
    args.settings.validate()?;
    let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).canonicalize()?;
    let root = home.parent().context("Missing pool root")?.to_owned();
    let work = home.join("work");
    fs::create_dir_all(&work)?;
    let lock = work.join("run.lock");
    drop(
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock)
            .context("Another verifier run may be active (work/run.lock)")?,
    );
    let _lock = RunLock(lock);
    let baseline_path = home.join("benchmarks/baseline.json");
    let environment = environment(&root)?;
    let baseline: Option<Report> = if args.record {
        None
    } else {
        let data = fs::read(&baseline_path)
            .context("No measured baseline. Run --suite benchmark --record-baseline first")?;
        let baseline = serde_json::from_slice(&data).context("Invalid baseline JSON")?;
        validate_baseline(&baseline, &args.settings, &environment)?;
        Some(baseline)
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            flag.store(true, Ordering::SeqCst);
        }
    });
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let dir = home
        .join("runs")
        .join(format!("benchmark-{now}-{}", std::process::id()));
    fs::create_dir_all(&dir)?;
    fs::write(
        work.join("latest-run.txt"),
        dir.to_string_lossy().as_bytes(),
    )?;
    let mut report = Report {
        schema: SCHEMA,
        rejected_weight: baseline
            .as_ref()
            .map_or(1.0, |baseline| baseline.rejected_weight),
        recorded_at_ms: now,
        environment,
        settings: args.settings.clone(),
        source_sha1: source_hash(&root)?,
        git_revision: command_output(&root, "git", &["rev-parse", "HEAD"])
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
    let build_root = root.clone();
    let flag = cancelled.clone();
    let build_home = work.join("benchmark");
    let built = tokio::task::spawn_blocking(move || {
        prepare::build_benchmark(&build_root, &build_home, flag)
    })
    .await?;
    if cancelled.load(Ordering::SeqCst) {
        return Ok(130);
    }
    let binary = built?;
    ensure!(
        source_hash(&root)? == report.source_sha1,
        "Pool source changed while building; rerun benchmark"
    );
    println!("Benchmarking current pool with --features bench ({} samples, {} shares/client); artifacts: {}", args.settings.samples, args.settings.shares, dir.display());
    for round in 0..args.settings.samples {
        // Reverse case order on alternate rounds to reduce warm-machine bias.
        let order: Vec<_> = if round % 2 == 0 {
            (0..CASES.len()).collect()
        } else {
            (0..CASES.len()).rev().collect()
        };
        for index in order {
            let case = CASES[index];
            let clients = client_count(case, &args.settings);
            let traffic = Traffic::for_case(case);
            let artifact = dir.join(format!("{case}-{}", round + 1));
            print!(
                "  {case} sample {}/{} ... ",
                round + 1,
                args.settings.samples
            );
            use std::io::Write;
            std::io::stdout().flush()?;
            match sample(
                &binary,
                artifact.clone(),
                clients,
                traffic,
                &args.settings,
                &cancelled,
            )
            .await
            {
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
                    fs::write(
                        dir.join("failure.json"),
                        serde_json::to_vec_pretty(
                            &json!({"case":case,"sample":round+1,"error":format!("{error:#}"),"artifacts":artifact}),
                        )?,
                    )?;
                    return Ok(if cancelled.load(Ordering::SeqCst) {
                        130
                    } else {
                        1
                    });
                }
            }
            fs::write(
                dir.join("benchmark.json"),
                serde_json::to_vec_pretty(&report)?,
            )?;
        }
    }
    ensure!(
        source_hash(&root)? == report.source_sha1,
        "Pool source changed during measurement; baseline was not saved"
    );
    if baseline.is_none() {
        // Serial cost/share = 1 / throughput. Calibrate once and freeze the
        // baseline weight for all future comparisons, including mixed traffic.
        report.rejected_weight =
            report.measurements[0].throughput() / report.measurements[1].throughput();
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
    let summary: Vec<_> = report.measurements.iter().map(|measurement| json!({
        "case": measurement.case,
        "median_shares_per_second": measurement.throughput(),
        "median_weighted_shares_per_second": measurement.weighted_throughput(report.rejected_weight),
        "median_p95_ms": measurement.latency(),
        "median_baseline_memory_bytes": measurement.baseline_memory(),
        "median_memory_per_connection_bytes": measurement.memory_per_connection(),
        "accepted": measurement.samples.iter().map(|sample| sample.accepted).sum::<usize>(),
        "rejected": measurement.samples.iter().map(|sample| sample.rejected).sum::<usize>(),
    })).collect();
    fs::write(
        dir.join("summary.json"),
        serde_json::to_vec_pretty(
            &json!({"weights":{"accepted":1.0,"rejected":report.rejected_weight},"memory":{"baseline_bytes":report.baseline_memory(),"bytes_per_connection":report.memory_per_connection()},"measurements":summary}),
        )?,
    )?;
    if let Some(baseline) = baseline {
        println!("Comparison to measured baseline:");
        let comparisons: Vec<_> = report.measurements.iter().zip(&baseline.measurements).map(|(current, old)| {
            let ratio = current.throughput() / old.throughput();
            let weighted_current = current.weighted_throughput(report.rejected_weight);
            let weighted_baseline = old.weighted_throughput(report.rejected_weight);
            let weighted_ratio = weighted_current / weighted_baseline;
            let latency_ratio = current.latency() / old.latency();
            println!("  {}:", current.case);
            println!(
                "    throughput: {:.0} shares/s current vs {:.0} baseline ({:+.2}%)",
                current.throughput(),
                old.throughput(),
                (ratio - 1.0) * 100.0
            );
            println!(
                "    weighted throughput: {:.0} shares/s current vs {:.0} baseline ({:+.2}%)",
                weighted_current,
                weighted_baseline,
                (weighted_ratio - 1.0) * 100.0
            );
            println!(
                "    p95 latency: {:.3} ms current vs {:.3} ms baseline ({:+.2}%)",
                current.latency(),
                old.latency(),
                (latency_ratio - 1.0) * 100.0
            );
            json!({"case":current.case,"baseline_shares_per_second":old.throughput(),"current_shares_per_second":current.throughput(),"throughput_ratio":ratio,"throughput_change_percent":(ratio-1.0)*100.0,"baseline_p95_ms":old.latency(),"current_p95_ms":current.latency(),"latency_ratio":current.latency()/old.latency(),"baseline_weighted_shares_per_second":weighted_baseline,"current_weighted_shares_per_second":weighted_current,"weighted_throughput_ratio":weighted_current/weighted_baseline})
        }).collect();
        let baseline_memory = baseline.baseline_memory();
        let current_memory = report.baseline_memory();
        let baseline_per_connection = baseline.memory_per_connection();
        let current_per_connection = report.memory_per_connection();
        println!(
            "  idle memory: {:.2} MiB current vs {:.2} MiB baseline ({:+.2}%)",
            current_memory / 1_048_576.0,
            baseline_memory / 1_048_576.0,
            (current_memory / baseline_memory - 1.0) * 100.0,
        );
        println!(
            "  memory per connection: {:.1} KiB current vs {:.1} KiB baseline ({:+.2}%)",
            current_per_connection / 1024.0,
            baseline_per_connection / 1024.0,
            (current_per_connection / baseline_per_connection - 1.0) * 100.0,
        );
        fs::write(
            dir.join("comparison.json"),
            serde_json::to_vec_pretty(
                &json!({"baseline_recorded_at_ms":baseline.recorded_at_ms,"baseline_source_sha1":baseline.source_sha1,"current_source_sha1":report.source_sha1,"weights":{"accepted":1.0,"rejected":report.rejected_weight},"memory":{"baseline_idle_bytes":baseline_memory,"current_idle_bytes":current_memory,"idle_ratio":current_memory/baseline_memory,"baseline_bytes_per_connection":baseline_per_connection,"current_bytes_per_connection":current_per_connection,"per_connection_ratio":current_per_connection/baseline_per_connection},"comparisons":comparisons}),
            )?,
        )?;
    } else {
        fs::create_dir_all(baseline_path.parent().unwrap())?;
        // All measurements and cleanup must succeed before replacing a baseline.
        let temporary = baseline_path.with_extension("json.tmp");
        // Baseline paths resolve from benchmarks/baseline.json, so the file
        // can move with the verifier directory without exposing local paths.
        for measurement in &mut report.measurements {
            for sample in &mut measurement.samples {
                let relative = sample.artifacts.strip_prefix(&home)?;
                sample.artifacts = PathBuf::from(format!(
                    "../{}",
                    relative.to_string_lossy().replace('\\', "/")
                ));
            }
        }
        fs::write(&temporary, serde_json::to_vec_pretty(&report)?)?;
        fs::rename(temporary, &baseline_path)?;
        println!("Measured baseline saved: {}", baseline_path.display());
    }
    println!("Benchmark report: {}", dir.join("benchmark.json").display());
    Ok(0)
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
