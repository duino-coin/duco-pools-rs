//! CLI orchestration for the pool verifier.
//!
//! A functional run has three phases:
//!
//! 1. **Prepare** - copy the pool sources into `work/`, apply the test adapter,
//!    and compile it (`prepare`).
//! 2. **Execute** - run each selected scenario against a fresh pool process in
//!    its own artifact directory (`harness`, with scenarios in `suites` and
//!    `expanded`).
//! 3. **Report** - write per-case and per-run JSON reports and pick an exit code.
//!
//! The opt-in `benchmark` and `microbench` suites have their own pipelines in
//! `benchmark` and `microbench`

mod benchmark;
mod expanded;
mod harness;
mod microbench;
mod mock;
mod prepare;
mod session;
mod suites;

use anyhow::{bail, Context, Result};
use serde_json::json;
use session::{Paths, RunLock};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use suites::Case;

// Process exit codes. `check_sensitivity.py` and CI depend on these values.
const EXIT_PASS: i32 = 0;
const EXIT_TEST_FAILURE: i32 = 1;
const EXIT_SETUP_ERROR: i32 = 2;
const EXIT_INTERRUPTED: i32 = 130;

const HELP: &str = "\
Pool verifier
  cargo run --manifest-path pool-verifier/Cargo.toml -- [--list] [--suite NAME] [--slow]

Suites: connections, protocol, counters, kolka, sync, lifecycle, internal; benchmark, microbench (opt-in, release CLI)
--case NAME filters one check; --mutation reward|pending|auth|release changes only the disposable copy for sensitivity validation.
Repeat --suite to select multiple suites. --slow includes the real 90-second idle timeout.
Benchmark: cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite benchmark [--record-baseline]
Benchmarks use the pool bench feature. --record-baseline explicitly saves/replaces measured results; normal runs compare without overwriting.
Options: --benchmark-samples N (5), --benchmark-shares N (10000 per client), --benchmark-warmup N (100), --benchmark-clients N (16).
Microbench: cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite microbench [--against REV]
Times the share logic without sockets, working tree against REV (main), alternating both in one run.
Options: --microbench-samples N (15), --microbench-shares N (1000000).
Exit codes: 0 pass, 1 test failure, 2 setup error, 130 interrupted.";

#[tokio::main]
async fn main() {
    let code = match execute().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("SETUP ERROR: {error:#}");
            EXIT_SETUP_ERROR
        }
    };
    std::process::exit(code);
}

/// Parsed command line.
#[derive(Default)]
struct Cli {
    /// `--suite` values, in the order given. Empty means "all functional suites".
    suites: Vec<String>,
    /// `--case`: either a bare scenario name or `suite/name`.
    case: Option<String>,
    /// `--mutation`: a deliberate bug to inject into the disposable copy.
    mutation: Option<String>,
    /// `--slow`: include scenarios that wait on real production timeouts.
    slow: bool,
    /// `--list`: print the selection instead of running it.
    list: bool,
    /// Options only valid together with `--suite benchmark`.
    benchmark: benchmark::Args,
    /// options that only go with `--suite microbench`
    microbench: microbench::Args,
}

impl Cli {
    /// Parses arguments without a CLI library. Returns `None` for `--help`.
    ///
    /// Unknown arguments are errors so a typo cannot look like a successful
    /// zero-test run.
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Option<Self>> {
        let mut cli = Self::default();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--list" => cli.list = true,
                "--slow" => cli.slow = true,
                "--mutation" => {
                    cli.mutation = Some(args.next().context("--mutation needs a name")?)
                }
                "--case" => cli.case = Some(args.next().context("--case needs a name")?),
                "--suite" => cli
                    .suites
                    .push(args.next().context("--suite needs a suite name")?),
                "--help" | "-h" => return Ok(None),
                _ => {
                    if !cli.benchmark.parse(&arg, &mut args)?
                        && !cli.microbench.parse(&arg, &mut args)?
                    {
                        bail!("Unknown argument {arg}; use --help");
                    }
                }
            }
        }
        Ok(Some(cli))
    }

    fn wants_benchmark(&self) -> bool {
        self.suites.iter().any(|suite| suite == "benchmark")
    }

    fn wants_microbench(&self) -> bool {
        self.suites.iter().any(|suite| suite == "microbench")
    }
}

async fn execute() -> Result<i32> {
    let Some(cli) = Cli::parse(std::env::args().skip(1))? else {
        println!("{HELP}");
        return Ok(EXIT_PASS);
    };

    if cli.wants_benchmark() {
        // The benchmark builds a different pool binary and must not share a run
        // with functional checks or a deliberately mutated copy.
        anyhow::ensure!(
            cli.suites.iter().all(|suite| suite == "benchmark")
                && cli.mutation.is_none()
                && cli.case.is_none()
                && !cli.slow,
            "Run --suite benchmark separately without --mutation, --case, or --slow"
        );
        if cli.list {
            benchmark::list();
            return Ok(EXIT_PASS);
        }
        return benchmark::run(cli.benchmark).await;
    }
    anyhow::ensure!(
        !cli.benchmark.used,
        "Benchmark options require --suite benchmark"
    );

    if cli.wants_microbench() {
        // builds its own two test executables, same as the benchmark
        anyhow::ensure!(
            cli.suites.iter().all(|suite| suite == "microbench")
                && cli.mutation.is_none()
                && cli.case.is_none()
                && !cli.slow,
            "Run --suite microbench separately without --mutation, --case, or --slow"
        );
        if cli.list {
            microbench::list();
            return Ok(EXIT_PASS);
        }
        return microbench::run(cli.microbench).await;
    }
    anyhow::ensure!(
        !cli.microbench.used,
        "Microbench options require --suite microbench"
    );

    let cases = select_cases(&cli)?;
    if cli.list {
        print_list(&cli, &cases);
        return Ok(EXIT_PASS);
    }
    run_functional(&cli, cases).await
}

/// Every registered functional scenario, across both registries.
fn all_cases() -> impl Iterator<Item = &'static Case> {
    suites::CASES.iter().chain(expanded::CASES)
}

/// Applies `--suite`, `--case` and `--slow` to the scenario registry.
///
/// Unknown suites and empty selections are errors rather than zero-test passes.
fn select_cases(cli: &Cli) -> Result<Vec<&'static Case>> {
    for suite in &cli.suites {
        anyhow::ensure!(
            all_cases().any(|case| case.suite == suite),
            "Unknown suite {suite}; use --list"
        );
    }
    let cases: Vec<_> = all_cases()
        .filter(|case| cli.suites.is_empty() || cli.suites.iter().any(|s| s == case.suite))
        .filter(|case| {
            cli.case
                .as_ref()
                .is_none_or(|name| *name == case.name || *name == case.id())
        })
        // `--list` shows slow cases too (marked), so users can discover them.
        .filter(|case| cli.list || cli.slow || !case.slow)
        .collect();
    anyhow::ensure!(!cases.is_empty(), "No matching scenarios");
    Ok(cases)
}

fn print_list(cli: &Cli, cases: &[&Case]) {
    for case in cases {
        let marker = if case.slow { " [--slow]" } else { "" };
        println!("{}{marker}", case.id());
    }
    // An unfiltered listing also advertises the opt-in benchmark workloads.
    if cli.suites.is_empty() && cli.case.is_none() {
        benchmark::list();
        microbench::list();
    }
}

/// Builds the disposable pool, runs every selected case and writes reports.
async fn run_functional(cli: &Cli, cases: Vec<&'static Case>) -> Result<i32> {
    let paths = Paths::discover()?;
    let _lock = RunLock::acquire(&paths.work)?;
    let cancelled = session::interrupt_flag();

    // Cargo and filesystem preparation are blocking operations. Move them off
    // the Tokio worker threads while sharing the Ctrl-C flag.
    let runtime_tests = cases.iter().any(|case| !case.is_internal());
    let internal_tests = cases.iter().any(|case| case.is_internal());
    let (root, home, flag, mutation) = (
        paths.root.clone(),
        paths.home.clone(),
        cancelled.clone(),
        cli.mutation.clone(),
    );
    let built = tokio::task::spawn_blocking(move || {
        prepare::build(&root, &home, flag, mutation, runtime_tests, internal_tests)
    })
    .await?;
    if cancelled.load(Ordering::SeqCst) {
        return Ok(EXIT_INTERRUPTED);
    }
    let binary = built?;

    let run_dir = create_run_dir(&paths, cli, &cases)?;
    println!(
        "Running {} checks; artifacts: {}",
        cases.len(),
        run_dir.display()
    );

    let mut reports = vec![];
    let mut statuses = vec![];
    let mut interrupted = false;
    for case in cases {
        let dir = run_dir.join(format!("{}--{}", case.suite, case.name));
        let started = Instant::now();
        print!("  {} ... ", case.id());
        std::io::stdout().flush()?;

        let outcome = if case.is_internal() {
            fs::create_dir_all(&dir)?;
            Outcome::from_result(expanded::run_internal(case, &paths.home, &dir).await)
        } else {
            run_runtime_case(case, &binary, &dir, &cancelled).await
        };

        let elapsed = started.elapsed().as_secs_f64();
        println!("{} ({elapsed:.2}s)", outcome.status.label());
        if !outcome.detail.is_empty() {
            println!("    {}\n    artifacts: {}", outcome.detail, dir.display());
        }

        // Write the per-case report and rewrite the run report after every case,
        // so an interrupted or crashed run still leaves usable results.
        let report = json!({
            "case": case.id(),
            "status": outcome.status.label(),
            "detail": outcome.detail,
            "seconds": elapsed,
            "artifacts": dir,
        });
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&report)?)?;
        reports.push(report);
        statuses.push(outcome.status);
        fs::write(
            run_dir.join("report.json"),
            serde_json::to_vec_pretty(&reports)?,
        )?;

        if cancelled.load(Ordering::SeqCst) {
            interrupted = true;
            break;
        }
    }

    let count = |status| statuses.iter().filter(|s| **s == status).count();
    println!(
        "\n{} passed, {} failed, {} setup errors. Report: {}",
        count(Status::Pass),
        count(Status::Fail),
        count(Status::SetupError),
        run_dir.join("report.json").display()
    );
    Ok(if interrupted {
        EXIT_INTERRUPTED
    } else if count(Status::SetupError) > 0 {
        EXIT_SETUP_ERROR
    } else if count(Status::Fail) > 0 {
        EXIT_TEST_FAILURE
    } else {
        EXIT_PASS
    })
}

/// Creates `runs/<millis>-<pid>/`, records what is about to run, and points
/// `work/latest-run.txt` at it.
fn create_run_dir(paths: &Paths, cli: &Cli, cases: &[&Case]) -> Result<PathBuf> {
    let run_id = format!(
        "{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        std::process::id()
    );
    let run_dir = paths.home.join("runs").join(run_id);
    fs::create_dir_all(&run_dir)?;
    let metadata = json!({
        "mutation": cli.mutation,
        "slow": cli.slow,
        "platform": std::env::consts::OS,
        "cases": cases.iter().map(|case| case.id()).collect::<Vec<_>>(),
    });
    fs::write(
        run_dir.join("metadata.json"),
        serde_json::to_vec_pretty(&metadata)?,
    )?;
    fs::write(
        paths.work.join("latest-run.txt"),
        run_dir.to_string_lossy().as_bytes(),
    )?;
    Ok(run_dir)
}

/// Result category of one case, as written to the reports.
#[derive(Clone, Copy, PartialEq)]
enum Status {
    Pass,
    Fail,
    SetupError,
    Interrupted,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::SetupError => "SETUP ERROR",
            Self::Interrupted => "INTERRUPTED",
        }
    }
}

struct Outcome {
    status: Status,
    /// Human-readable failure explanation; empty on success.
    detail: String,
}

impl Outcome {
    fn new(status: Status, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
        }
    }

    fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => Self::new(Status::Pass, ""),
            Err(error) => Self::new(Status::Fail, format!("{error:#}")),
        }
    }
}

/// Upper bound for one runtime scenario, excluding pool startup.
fn time_limit(case: &Case) -> Duration {
    if case.slow {
        // Slow cases wait on 61- and 90-second production deadlines.
        Duration::from_secs(115)
    } else if case.name == "adaptive-mining-average" {
        // Solves 55 jobs at adaptively raised difficulty.
        Duration::from_secs(90)
    } else {
        Duration::from_secs(35)
    }
}

/// Runs one runtime scenario against a fresh pool process.
async fn run_runtime_case(
    case: &Case,
    binary: &Path,
    dir: &Path,
    cancelled: &AtomicBool,
) -> Outcome {
    let started =
        harness::Harness::start(binary, dir.to_path_buf(), suites::options(case), cancelled).await;
    let mut harness = match started {
        Ok(harness) => harness,
        Err(_) if cancelled.load(Ordering::SeqCst) => {
            return Outcome::new(Status::Interrupted, "Cancelled by Ctrl-C during startup.")
        }
        Err(error) => return Outcome::new(Status::SetupError, format!("{error:#}")),
    };

    let limit = time_limit(case);
    let result = tokio::select! {
        result = tokio::time::timeout(limit, suites::run(case, &mut harness)) => {
            result.unwrap_or_else(|_| Err(anyhow::anyhow!("Scenario exceeded {limit:?}")))
        }
        _ = session::interrupted(cancelled) => Err(anyhow::anyhow!("Interrupted by Ctrl-C")),
    };

    // The scenario result and every cleanup problem are reported together; no
    // earlier error suppresses a later diagnostic. Cleanup failures (for
    // example, an unclean shutdown) are test failures too.
    let mut problems: Vec<String> = result.err().map(|e| format!("{e:#}")).into_iter().collect();
    problems.extend(harness.finish().await.iter().map(|e| format!("{e:#}")));

    if cancelled.load(Ordering::SeqCst) {
        Outcome::new(
            Status::Interrupted,
            "Cancelled by Ctrl-C; partial scenario is not a test result.",
        )
    } else if problems.is_empty() {
        Outcome::new(Status::Pass, "")
    } else {
        Outcome::new(Status::Fail, problems.join("\n"))
    }
}
