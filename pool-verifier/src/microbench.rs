//! opt-in A/B microbenchmark of the share logic, no sockets involved
//!
//! the pool's test executable gets built twice: from a git revision (`base`,
//! `--against`, `main` by default) and from the working tree (`current`); both only
//! get `injected/microbench.rs` appended to `mining/mining.rs`, so what's timed is
//! the production share logic (`mining::Session`)
//!
//! every sample runs each workload once per side in a fresh process, back to back,
//! and which side goes first flips every sample; drift of the machine (clock speed,
//! background load) hits both sides alike that way, which is why there's no saved
//! baseline; each sample is reported as `current / base`
//!
//! the environment gets padded by a different length every sample (same for both
//! sides of it); it sits above the initial stack, so the samples cover a bunch of
//! stack alignments instead of one that happens to suit or hurt the code

use crate::{
    benchmark::{change_percent, command_output, median, number},
    prepare,
    session::{self, Paths, RunLock},
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{process::Command, time::timeout};

/// report format version; bump it when the meaning of a stored field changes
const SCHEMA: u32 = 1;

/// workload names and the injected test that runs one sample of each
const WORKLOADS: [(&str, &str); 5] = [
    ("accepted", "accepted"),
    ("accepted-stats-every-10", "accepted_stats_every_10"),
    ("rejected", "rejected"),
    ("esp32", "esp32"),
    ("contended", "contended"),
];
const TEST_MODULE: &str = "mining::mining::verifier_bench::";

/// the environment padding grows by this many bytes per sample and wraps after
/// `PADDING_STEPS`, so it covers 0-240 bytes of stack offset
const PADDING_STEP: usize = 16;
const PADDING_STEPS: usize = 16;

/// upper bound for one sample process
const SAMPLE_DEADLINE: Duration = Duration::from_secs(120);

/// command-line options only the microbenchmark takes
pub struct Args {
    /// any microbenchmark option was given (rejected without `--suite microbench`)
    pub used: bool,
    /// `--against`: the git revision `current` gets compared with
    against: String,
    /// `--microbench-samples`: samples per workload and side
    samples: usize,
    /// `--microbench-shares`: timed shares per sample
    shares: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            used: false,
            against: "main".into(),
            samples: 15,
            shares: 1_000_000,
        }
    }
}

impl Args {
    /// eats `arg` (and its value) if it's a microbenchmark option; `false` for
    /// anything else
    pub fn parse(&mut self, arg: &str, args: &mut impl Iterator<Item = String>) -> Result<bool> {
        match arg {
            "--against" => self.against = args.next().context("--against needs a git revision")?,
            "--microbench-samples" => self.samples = number(arg, args)?,
            "--microbench-shares" => self.shares = number(arg, args)?,
            _ => return Ok(false),
        }
        self.used = true;
        Ok(true)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            (3..=100).contains(&self.samples),
            "Use 3..=100 microbench samples"
        );
        ensure!(
            (10_000..=10_000_000).contains(&self.shares),
            "Use 10_000..=10000000 microbench shares"
        );
        Ok(())
    }
}

pub fn list() {
    for (name, _) in WORKLOADS {
        println!("microbench/{name}");
    }
    println!("microbench/task-size");
}

/// runs one injected test in a fresh process and hands back its `VERIFY_BENCH` JSON
async fn run_test(
    binary: &Path,
    test: &str,
    shares: usize,
    padding: usize,
    dir: &Path,
) -> Result<Value> {
    let command = Command::new(binary)
        .args([
            &format!("{TEST_MODULE}{test}"),
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("VERIFY_BENCH_SHARES", shares.to_string())
        .env("VERIFY_BENCH_PADDING", "x".repeat(padding))
        .current_dir(dir)
        .kill_on_drop(true)
        .output();
    let output = timeout(SAMPLE_DEADLINE, command)
        .await
        .with_context(|| format!("Microbench {test} exceeded {SAMPLE_DEADLINE:?}"))??;
    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(
        output.status.success(),
        "Microbench {test} failed: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // libtest prints the test name without a newline before the test runs
    let line = stdout
        .lines()
        .find_map(|line| line.split_once("VERIFY_BENCH ").map(|(_, json)| json))
        .with_context(|| format!("Microbench {test} printed no result: {stdout}"))?;
    Ok(serde_json::from_str(line)?)
}

/// measured values of one workload, per side
struct Measurement {
    base: Vec<f64>,
    current: Vec<f64>,
}

impl Measurement {
    /// `current / base` of each sample; both ran back to back
    fn ratios(&self) -> Vec<f64> {
        self.base
            .iter()
            .zip(&self.current)
            .map(|(base, current)| current / base)
            .collect()
    }

    fn summary(&self, name: &str) -> Value {
        let ratios = self.ratios();
        json!({
            "workload": name,
            "base_ns_per_share": self.base,
            "current_ns_per_share": self.current,
            "base_median_ns": median(self.base.iter().copied()),
            "current_median_ns": median(self.current.iter().copied()),
            "ratio_median": median(ratios.iter().copied()),
            "ratio_min": ratios.iter().copied().fold(f64::INFINITY, f64::min),
            "ratio_max": ratios.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        })
    }
}

pub async fn run(args: Args) -> Result<i32> {
    ensure!(!cfg!(debug_assertions), "Run microbenchmarks with cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite microbench");
    args.validate()?;
    let paths = Paths::discover()?;
    let _lock = RunLock::acquire(&paths.work)?;
    let cancelled = session::interrupt_flag();

    // resolved once, so a revision that moves mid-run can't mix two commits
    let revision = command_output(
        &paths.root,
        "git",
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{}^{{commit}}", args.against),
        ],
    )?;

    let (root, home, flag, built_revision) = (
        paths.root.clone(),
        paths.work.join("microbench"),
        cancelled.clone(),
        revision.clone(),
    );
    let built = tokio::task::spawn_blocking(move || {
        prepare::build_microbench(&root, &home, &built_revision, flag)
    })
    .await?;
    if cancelled.load(Ordering::SeqCst) {
        return Ok(130);
    }
    let [base, current] = built?;
    let sides: [(&str, &PathBuf); 2] = [("base", &base), ("current", &current)];

    let dir = paths.home.join("runs").join(format!(
        "microbench-{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        std::process::id()
    ));
    fs::create_dir_all(&dir)?;
    println!(
        "Microbenchmarking working tree against {} ({revision}): {} samples, {} shares; artifacts: {}",
        args.against,
        args.samples,
        args.shares,
        dir.display()
    );

    let mut task_bytes = [0u64; 2];
    for (index, (_, binary)) in sides.iter().enumerate() {
        task_bytes[index] = run_test(binary, "task_size", args.shares, 0, &dir).await?["bytes"]
            .as_u64()
            .context("task_size printed no byte count")?;
    }

    let mut measurements: Vec<_> = WORKLOADS
        .iter()
        .map(|_| Measurement {
            base: Vec::with_capacity(args.samples),
            current: Vec::with_capacity(args.samples),
        })
        .collect();
    let mut runs = 0;
    for sample in 0..args.samples {
        let padding = PADDING_STEP * (sample % PADDING_STEPS);
        for ((name, test), measurement) in WORKLOADS.iter().zip(&mut measurements) {
            // flip which side goes first, so neither always gets the warmer machine
            let order = if sample % 2 == 0 { [0, 1] } else { [1, 0] };
            for side in order {
                // a fresh copy for every run; Windows picks an executable's load address
                // once per file and boot, which pinned each side to its own address (and
                // its own speed, 2-4% apart for identical code) for the whole run
                let copy = dir.join(format!("run-{runs:05}{}", std::env::consts::EXE_SUFFIX));
                runs += 1;
                fs::copy(sides[side].1, &copy)?;
                let result = run_test(&copy, test, args.shares, padding, &dir).await;
                // it's only disk space, the copy never runs again
                let _ = fs::remove_file(&copy);
                // Ctrl-C hits the sample process too, so its failure isn't a result
                if cancelled.load(Ordering::SeqCst) {
                    return Ok(130);
                }
                let ns = result?["ns_per_share"]
                    .as_f64()
                    .with_context(|| format!("{name} printed no time"))?;
                if side == 0 {
                    measurement.base.push(ns);
                } else {
                    measurement.current.push(ns);
                }
            }
            println!(
                "  {name} sample {}/{} ... base {:.1} ns/share, current {:.1} ns/share",
                sample + 1,
                args.samples,
                measurement.base[sample],
                measurement.current[sample]
            );
        }
    }

    println!("Current against base (median of per-sample ratios, range over samples):");
    let mut summaries = vec![];
    for ((name, _), measurement) in WORKLOADS.iter().zip(&measurements) {
        let summary = measurement.summary(name);
        println!(
            "  {name}: base {:.1} ns, current {:.1} ns, {:+.2}% (range {:+.2}% to {:+.2}%)",
            summary["base_median_ns"].as_f64().unwrap_or_default(),
            summary["current_median_ns"].as_f64().unwrap_or_default(),
            change_percent(summary["ratio_median"].as_f64().unwrap_or_default(), 1.0),
            change_percent(summary["ratio_min"].as_f64().unwrap_or_default(), 1.0),
            change_percent(summary["ratio_max"].as_f64().unwrap_or_default(), 1.0),
        );
        summaries.push(summary);
    }
    println!(
        "  task-size: base {} bytes, current {} bytes per connection future",
        task_bytes[0], task_bytes[1]
    );

    let report = json!({
        "schema": SCHEMA,
        "against": args.against,
        "base_revision": revision,
        "settings": { "samples": args.samples, "shares": args.shares },
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "logical_cpus": std::thread::available_parallelism()?.get(),
            "rustc": command_output(&paths.root, "rustc", &["-Vv"])?,
        },
        "task_bytes": { "base": task_bytes[0], "current": task_bytes[1] },
        "workloads": summaries,
    });
    fs::write(
        dir.join("microbench.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!(
        "Microbench report: {}",
        dir.join("microbench.json").display()
    );
    Ok(0)
}
