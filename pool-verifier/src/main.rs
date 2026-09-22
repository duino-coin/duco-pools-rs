//! CLI orchestration for the pool verifier.
//!
//! A run has three phases: prepare and compile an instrumented disposable pool,
//! execute each selected scenario in an isolated directory, then persist reports
//! and diagnostics. Scenario behavior lives in `suites`/`expanded`; process and
//! miner mechanics live in `harness`.

mod benchmark;
mod expanded;
mod harness;
mod mock;
mod prepare;
mod suites;

use anyhow::{bail, Context, Result};
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct RunLock(PathBuf);
impl Drop for RunLock {
    fn drop(&mut self) {
        // Best-effort cleanup is appropriate in Drop: a stale lock is safer than
        // panicking while another error or Ctrl-C is already unwinding the run.
        let _ = fs::remove_file(&self.0);
    }
}

#[tokio::main]
async fn main() {
    let code = match execute().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("SETUP ERROR: {error:#}");
            2
        }
    };
    std::process::exit(code);
}

async fn execute() -> Result<i32> {
    // Keep argument parsing dependency-free; invalid or empty selections are setup
    // errors so a typo cannot look like a successful zero-test run.
    let mut benchmark_args = benchmark::Args::default();
    let mut selected = vec![];
    let mut slow = false;
    let mut list = false;
    let mut mutation = None;
    let mut selected_case = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--list" => list = true,
            "--mutation" => mutation = Some(args.next().context("--mutation needs a name")?),
            "--case" => selected_case = Some(args.next().context("--case needs a name")?),
            "--slow" => slow = true,
            "--suite" => selected.push(args.next().context("--suite needs a suite name")?),
            "--help" | "-h" => {
                println!("Pool verifier\n  cargo run --manifest-path pool-verifier/Cargo.toml -- [--list] [--suite NAME] [--slow]\n\nSuites: connections, protocol, counters, kolka, sync, lifecycle, internal; benchmark (opt-in, release CLI)\n--case NAME filters one check; --mutation reward|pending|auth|release changes only the disposable copy for sensitivity validation.\nRepeat --suite to select multiple suites. --slow includes the real 90-second idle timeout.\nBenchmark: cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite benchmark [--record-baseline]\nBenchmarks use the pool bench feature. --record-baseline explicitly saves/replaces measured results; normal runs compare without overwriting.\nOptions: --benchmark-samples N (5), --benchmark-shares N (10000 per client), --benchmark-warmup N (100), --benchmark-clients N (16).\nExit codes: 0 pass, 1 test failure, 2 setup error, 130 interrupted.");
                return Ok(0);
            }
            _ => {
                if !benchmark_args.parse(&arg, &mut args)? {
                    bail!("Unknown argument {arg}; use --help");
                }
            }
        }
    }
    if selected.iter().any(|suite| suite == "benchmark") {
        anyhow::ensure!(
            selected.iter().all(|suite| suite == "benchmark")
                && mutation.is_none()
                && selected_case.is_none()
                && !slow,
            "Run --suite benchmark separately without --mutation, --case, or --slow"
        );
        if list {
            benchmark::list();
            return Ok(0);
        }
        return benchmark::run(benchmark_args).await;
    }
    anyhow::ensure!(
        !benchmark_args.used,
        "Benchmark options require --suite benchmark"
    );
    for suite in &selected {
        anyhow::ensure!(
            suites::CASES
                .iter()
                .chain(expanded::CASES)
                .any(|c| c.suite == suite),
            "Unknown suite {suite}; use --list"
        );
    }
    let cases: Vec<_> = suites::CASES
        .iter()
        .chain(expanded::CASES)
        .filter(|c| selected.is_empty() || selected.iter().any(|s| s == c.suite))
        .filter(|c| {
            selected_case
                .as_ref()
                .is_none_or(|name| name == c.name || *name == format!("{}/{}", c.suite, c.name))
        })
        .filter(|c| list || slow || !c.slow)
        .collect();
    anyhow::ensure!(!cases.is_empty(), "No matching scenarios");
    if list {
        for c in cases {
            println!(
                "{}/{}{}",
                c.suite,
                c.name,
                if c.slow { " [--slow]" } else { "" }
            );
        }
        if selected.is_empty() && selected_case.is_none() {
            benchmark::list();
        }
        return Ok(0);
    }
    let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).canonicalize()?;
    let root = home
        .parent()
        .context("Verifier must live inside the pool repository")?
        .to_path_buf();
    let work = home.join("work");
    fs::create_dir_all(&work)?;
    // Runs share a disposable source tree and Cargo cache. Serializing ownership
    // prevents one run from rebuilding or mirroring while another launches cases.
    let lock_path = work.join("run.lock");
    let _file = fs::OpenOptions::new().write(true).create_new(true).open(&lock_path)
        .with_context(|| format!("Another verifier run may be active. If a previous run crashed, remove {} after checking that it has stopped", lock_path.display()))?;
    let _lock = RunLock(lock_path);
    // Close the file on Windows so the guard can delete it later.
    drop(_file);
    let cancelled = Arc::new(AtomicBool::new(false));
    let signal_flag = cancelled.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_flag.store(true, Ordering::SeqCst);
            eprintln!("\nInterrupt received; stopping owned test processes.");
        }
    });
    let root_build = root.clone();
    let home_build = home.clone();
    let build_cancelled = cancelled.clone();
    let runtime_tests = cases.iter().any(|c| c.suite != "internal");
    let internal_tests = cases.iter().any(|c| c.suite == "internal");
    let mutation_metadata = mutation.clone();
    // Cargo and filesystem preparation are blocking operations. Move them off the
    // Tokio worker threads while retaining the shared Ctrl-C flag.
    let built = tokio::task::spawn_blocking(move || {
        prepare::build(
            &root_build,
            &home_build,
            build_cancelled,
            mutation,
            runtime_tests,
            internal_tests,
        )
    })
    .await?;
    if cancelled.load(Ordering::SeqCst) {
        return Ok(130);
    }
    let binary = built?;
    let run_id = format!(
        "{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        std::process::id()
    );
    let run_dir = home.join("runs").join(run_id);
    fs::create_dir_all(&run_dir)?;
    fs::write(
        run_dir.join("metadata.json"),
        serde_json::to_vec_pretty(&json!({
            "mutation": mutation_metadata, "slow": slow, "platform": std::env::consts::OS,
            "cases": cases.iter().map(|c|format!("{}/{}",c.suite,c.name)).collect::<Vec<_>>()
        }))?,
    )?;
    fs::write(
        home.join("work/latest-run.txt"),
        run_dir.to_string_lossy().as_bytes(),
    )?;
    println!(
        "Running {} checks; artifacts: {}",
        cases.len(),
        run_dir.display()
    );
    let mut reports = vec![];
    let mut failures = 0;
    let mut setup_errors = 0;
    let mut interrupted = false;
    // Each runtime case gets a fresh pool process and artifact directory. Internal
    // cases similarly launch one exact injected test in a fresh working directory.
    for case in cases {
        let name = format!("{}/{}", case.suite, case.name);
        let dir = run_dir.join(format!("{}--{}", case.suite, case.name));
        let started = Instant::now();
        print!("  {name} ... ");
        use std::io::Write;
        std::io::stdout().flush()?;
        let (status, detail) = if case.suite == "internal" {
            fs::create_dir_all(&dir)?;
            match expanded::run_internal(case, &home, &dir).await {
                Ok(()) => ("PASS", String::new()),
                Err(error) => {
                    failures += 1;
                    ("FAIL", format!("{error:#}"))
                }
            }
        } else {
            match harness::Harness::start(&binary, dir.clone(), suites::options(case), &cancelled)
                .await
            {
                Err(error) => {
                    if cancelled.load(Ordering::SeqCst) {
                        (
                            "INTERRUPTED",
                            "Cancelled by Ctrl-C during startup.".to_owned(),
                        )
                    } else {
                        setup_errors += 1;
                        ("SETUP ERROR", format!("{error:#}"))
                    }
                }
                Ok(mut h) => {
                    let limit = if case.slow {
                        Duration::from_secs(115)
                    } else if case.name == "adaptive-mining-window" {
                        Duration::from_secs(90)
                    } else {
                        Duration::from_secs(35)
                    };
                    let result = tokio::select! {
                        result = tokio::time::timeout(limit, suites::run(case, &mut h)) => {
                            match result { Ok(result) => result, Err(_) => Err(anyhow::anyhow!("Scenario exceeded {limit:?}")) }
                        },
                        _ = async {
                            while !cancelled.load(Ordering::SeqCst) { tokio::time::sleep(Duration::from_millis(50)).await; }
                        } => {
                            interrupted = true;
                            Err(anyhow::anyhow!("Interrupted by Ctrl-C"))
                        }
                    };
                    // Capture state before shutdown, always attempt cleanup, then
                    // capture logs/mock diagnostics again. Cleanup failures are test
                    // failures too; no earlier error suppresses a later diagnostic.
                    let save_before = h.save().await;
                    let cleanup = h.stop().await;
                    let save_after = h.save().await;
                    let mut problems = vec![];
                    for result in [result, save_before, cleanup, save_after] {
                        if let Err(error) = result {
                            problems.push(format!("{error:#}"));
                        }
                    }
                    if cancelled.load(Ordering::SeqCst) {
                        (
                            "INTERRUPTED",
                            "Cancelled by Ctrl-C; partial scenario is not a test result."
                                .to_owned(),
                        )
                    } else if problems.is_empty() {
                        ("PASS", String::new())
                    } else {
                        failures += 1;
                        ("FAIL", problems.join("\n"))
                    }
                }
            }
        };
        let elapsed = started.elapsed().as_secs_f64();
        println!("{status} ({elapsed:.2}s)");
        if !detail.is_empty() {
            println!("    {detail}\n    artifacts: {}", dir.display());
        }
        let report = json!({ "case": name, "status": status, "detail": detail, "seconds": elapsed, "artifacts": dir });
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&report)?)?;
        reports.push(report);
        fs::write(
            run_dir.join("report.json"),
            serde_json::to_vec_pretty(&reports)?,
        )?;
        if cancelled.load(Ordering::SeqCst) {
            interrupted = true;
        }
        if interrupted {
            break;
        }
    }
    println!(
        "\n{} passed, {failures} failed, {setup_errors} setup errors. Report: {}",
        reports.iter().filter(|r| r["status"] == "PASS").count(),
        run_dir.join("report.json").display()
    );
    Ok(if interrupted {
        130
    } else if setup_errors > 0 {
        2
    } else if failures > 0 {
        1
    } else {
        0
    })
}
