//! Builds the disposable copy of the pool that the verifier exercises.
//!
//! The verifier never compiles or edits the repository's production sources in
//! place. A build goes through these steps:
//!
//! 1. Mirror `src/`, `Cargo.toml`, `Cargo.lock` and `build.rs` into
//!    `work/source-next` (the *stage*).
//! 2. Apply a small, fail-closed adapter to the stage:
//!    * redirect external services to the local mocks;
//!    * expose otherwise-private state on the dashboard (functional builds only);
//!    * add a stdin control channel for `refresh` / `shutdown`;
//!    * append the tests from `injected/` to their owning modules (functional
//!      builds only);
//!    * optionally inject one deliberate bug (`--mutation`).
//! 3. Mirror the finished stage into `work/source` and compile it there.
//!
//! None of these changes are copied back to the repository.

use anyhow::{bail, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

/// What to build into the disposable copy.
struct Plan {
    /// Name of a deliberate bug to inject (see [`apply_mutation`]).
    mutation: Option<String>,
    /// Build the release pool binary used by runtime scenarios.
    runtime_tests: bool,
    /// Build the test executable that contains the injected tests.
    internal_tests: bool,
    /// Benchmark copy: `--features bench`, no observation surface and no
    /// injected tests, so production hot paths stay intact for timing.
    benchmark: bool,
}

/// Prepares and compiles the functional copy. Returns the pool binary path.
pub fn build(
    root: &Path,
    home: &Path,
    cancelled: Arc<AtomicBool>,
    mutation: Option<String>,
    runtime_tests: bool,
    internal_tests: bool,
) -> Result<PathBuf> {
    let plan = Plan {
        mutation,
        runtime_tests,
        internal_tests,
        benchmark: false,
    };
    build_copy(root, home, &cancelled, &plan)
}

/// Prepares and compiles the benchmark copy. Only endpoint redirection and the
/// process control channel are installed, so production hot paths stay intact.
pub fn build_benchmark(root: &Path, home: &Path, cancelled: Arc<AtomicBool>) -> Result<PathBuf> {
    let plan = Plan {
        mutation: None,
        runtime_tests: true,
        internal_tests: false,
        benchmark: true,
    };
    build_copy(root, home, &cancelled, &plan)
}

fn build_copy(root: &Path, home: &Path, cancelled: &AtomicBool, plan: &Plan) -> Result<PathBuf> {
    let work = home.join("work");
    fs::create_dir_all(&work)?;
    let target = cargo_target_dir(root, &work, plan.benchmark);

    // Build a complete candidate tree before touching `work/source`. This keeps
    // the long-lived Cargo source/target pair coherent, and unchanged files keep
    // their timestamps across verifier runs.
    let stage = stage_sources(root, &work, plan.benchmark)?;
    patch_sync_endpoints(&stage)?;
    if !plan.benchmark {
        add_observation_surface(&stage)?;
    }
    add_control_channel(&stage)?;
    if !plan.benchmark {
        append_injected_tests(&stage)?;
    }
    if let Some(name) = plan.mutation.as_deref() {
        apply_mutation(&stage, name)?;
    }

    // Publish the fully adapted stage only after every anchor has been validated.
    let source = work.join("source");
    mirror(&stage, &source, &work)?;

    if plan.runtime_tests {
        build_release_binary(&work, &source, &target, plan.benchmark, cancelled)?;
    }
    if plan.internal_tests {
        build_internal_tests(&work, &source, &target, cancelled)?;
    }
    Ok(target
        .join("release")
        .join(format!("duco-pool-rs{}", std::env::consts::EXE_SUFFIX)))
}

// ---- Filesystem helpers -----------------------------------------------------

/// Replaces exactly one occurrence of `from` with `to`.
///
/// Adapter edits are anchored on recognizable production source, not on line
/// numbers. Requiring exactly one match means a pool refactor cannot silently
/// produce a partly instrumented binary or patch the wrong occurrence.
fn replace_once(text: &mut String, from: &str, to: &str, label: &str) -> Result<()> {
    let count = text.matches(from).count();
    if count != 1 {
        bail!("Test-copy patch '{label}' expected one match, found {count}. Pool source changed; update the adapter, not the pool.");
    }
    *text = text.replacen(from, to, 1);
    Ok(())
}

/// Reads a staged file with LF line endings, lets `edit` change it, and writes
/// it back. Normalizing line endings keeps multi-line anchors portable.
fn patch_file(
    stage: &Path,
    relative: &str,
    edit: impl FnOnce(&mut String) -> Result<()>,
) -> Result<()> {
    let path = stage.join(relative);
    let mut text = fs::read_to_string(&path)?.replace("\r\n", "\n");
    edit(&mut text)?;
    fs::write(path, text)?;
    Ok(())
}

/// Writes `data` only if it differs, so unchanged files keep their timestamps
/// and Cargo can reuse the target cache.
fn write_changed(path: &Path, data: &[u8]) -> Result<()> {
    if fs::read(path).ok().as_deref() != Some(data) {
        fs::write(path, data)?;
    }
    Ok(())
}

/// Recursively deletes `path`, which must lie strictly inside `work`.
///
/// `mirror` deletes stale entries. Every deletion is proven to stay below our
/// own work directory, and symlinks are rejected so a crafted tree cannot
/// redirect a deletion outside it.
fn remove_owned(path: &Path, work: &Path) -> Result<()> {
    let file_type = fs::symlink_metadata(path)?.file_type();
    anyhow::ensure!(
        !file_type.is_symlink(),
        "Refusing work-directory symlink: {}",
        path.display()
    );
    let resolved = path.canonicalize()?;
    let boundary = work.canonicalize()?;
    anyhow::ensure!(
        resolved.starts_with(&boundary) && resolved != boundary,
        "Unsafe cleanup path"
    );
    if file_type.is_dir() {
        for entry in fs::read_dir(path)? {
            remove_owned(&entry?.path(), work)?;
        }
        fs::remove_dir(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// Makes `to` an exact copy of `from`.
///
/// This is a synchronizing copy, not an overlay. Files removed from `from` also
/// disappear from `to`, so production deletions reach the disposable tree.
fn mirror(from: &Path, to: &Path, work: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    anyhow::ensure!(
        !fs::symlink_metadata(to)?.file_type().is_symlink(),
        "Refusing destination symlink"
    );
    for entry in fs::read_dir(to)? {
        let entry = entry?;
        if !from.join(entry.file_name()).exists() {
            remove_owned(&entry.path(), work)?;
        }
    }
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        anyhow::ensure!(
            !file_type.is_symlink(),
            "Refusing source symlink: {}",
            entry.path().display()
        );
        let destination = to.join(entry.file_name());
        if file_type.is_dir() {
            mirror(&entry.path(), &destination, work)?;
        } else {
            write_changed(&destination, &fs::read(entry.path())?)?;
        }
    }
    Ok(())
}

/// Chooses `CARGO_TARGET_DIR` for the copy.
fn cargo_target_dir(root: &Path, work: &Path, benchmark: bool) -> PathBuf {
    // The benchmark target is kept close to the repository root.
    let target = if benchmark {
        root.join("target/pool-verifier-benchmark")
    } else {
        work.join("target")
    };
    // On Windows, strip the `\\?\` verbatim prefix returned by canonicalize()
    // before passing the path to Cargo: MSVC misparses generated mimalloc
    // source paths that start with it.
    #[cfg(windows)]
    let target = {
        let display = target.to_string_lossy();
        let normal = if let Some(path) = display.strip_prefix(r"\\?\UNC\") {
            format!(r"\\{path}")
        } else {
            display
                .strip_prefix(r"\\?\")
                .unwrap_or(display.as_ref())
                .to_owned()
        };
        PathBuf::from(normal)
    };
    target
}

// ---- Step 1: stage the production sources ----------------------------------

/// Recreates `work/source-next` from the repository's current sources.
fn stage_sources(root: &Path, work: &Path, benchmark: bool) -> Result<PathBuf> {
    let stage = work.join("source-next");
    if stage.exists() {
        remove_owned(&stage, work)?;
    }
    fs::create_dir_all(&stage)?;
    mirror(&root.join("src"), &stage.join("src"), work)?;
    for name in ["Cargo.toml", "build.rs"] {
        fs::copy(root.join(name), stage.join(name))?;
    }
    if benchmark {
        // The benchmark copy lives under `work/benchmark/`, which the verifier's
        // own `[workspace]` does not exclude. An empty `[workspace]` table makes
        // the copy its own workspace root.
        let path = stage.join("Cargo.toml");
        let mut manifest = fs::read_to_string(&path)?;
        anyhow::ensure!(
            !manifest.contains("[workspace"),
            "Benchmark source must be a standalone pool package"
        );
        manifest.push_str("\n[workspace]\n");
        fs::write(path, manifest)?;
        anyhow::ensure!(
            root.join("Cargo.lock").is_file(),
            "Benchmark source needs Cargo.lock"
        );
    }
    // Prefer the repository lockfile; otherwise keep the one from the last build.
    let lock = if root.join("Cargo.lock").exists() {
        root.join("Cargo.lock")
    } else {
        work.join("source/Cargo.lock")
    };
    if lock.exists() {
        fs::copy(lock, stage.join("Cargo.lock"))?;
    }
    Ok(stage)
}

// ---- Step 2a: network redirection ------------------------------------------

/// Points the public-IP lookup and the sync URL at the mock (`POOL_VERIFY_HTTP`).
///
/// Only the endpoint expressions are replaced. Request construction, retry
/// logic, multipart encoding and accounting remain production code under test.
fn patch_sync_endpoints(stage: &Path) -> Result<()> {
    patch_file(stage, "src/sync/sync.rs", |sync| {
        replace_once(sync, "client.get(\"https://api.ipify.org/\")", "client.get(format!(\"{}/ip\", std::env::var(\"POOL_VERIFY_HTTP\").expect(\"mock HTTP URL\")))", "IP endpoint")?;

        // The sync URL is built by a multi-line `format!`. Isolate exactly that
        // statement, which ends at its closing `);`, and patch inside it.
        let start = sync
            .find("    let request_url = format!(")
            .context("Missing sync URL construction")?;
        let end_marker = "\n    );";
        let end = sync[start..]
            .find(end_marker)
            .context("Missing end of sync URL construction")?
            + start
            + end_marker.len();
        let original_url = &sync[start..end];
        anyhow::ensure!(
            original_url.contains("/pool_sync/?host={}")
                && original_url.contains("block_increase,")
                && original_url.contains("connections"),
            "Unrecognized sync URL construction"
        );
        let mut patched_url = original_url.to_owned();
        replace_once(
            &mut patched_url,
            "https://{}/pool_sync/",
            "{}/pool_sync/",
            "sync URL scheme",
        )?;
        // Three pool versions exist: the dedicated `server_endpoint` host, the
        // older `server_ip` host and the oldest hard-coded one. All are supported.
        let host = if patched_url.contains("        config.server_endpoint,") {
            "        config.server_endpoint,"
        } else if patched_url.contains("        config.server_ip,") {
            "        config.server_ip,"
        } else {
            "        \"server.duinocoin.com\", //TODO: temp fix"
        };
        replace_once(
            &mut patched_url,
            host,
            "        std::env::var(\"POOL_VERIFY_HTTP\").expect(\"mock HTTP URL\"),",
            "sync URL host",
        )?;
        sync.replace_range(start..end, &patched_url);
        Ok(())
    })
}

// ---- Step 2b: read-only observation surface --------------------------------

/// Adds a `verify` object to the already-authenticated dashboard `/data`
/// response, with private counters, balances, blocks and per-connection mining
/// observations.
///
/// Tests observe private state at stable iteration boundaries, so they never
/// re-implement the pool's calculations as their oracle.
fn add_observation_surface(stage: &Path) -> Result<()> {
    patch_file(stage, "src/web/web.rs", |web| {
        // Take one block snapshot before building the JSON. Locking separately
        // for `len` and serialization could expose two moments in one response.
        replace_once(
            web,
            "                            return Ok(warp::reply::json(&data_res));",
            r#"                            let verify_blocks = mining::mining::GLOBAL_BLOCKS.lock().await.clone();
                            let mut combined = serde_json::to_value(&data_res).unwrap();
                            combined["verify"] = serde_json::json!({
                                "due_cache": mining::mining::DUE_DIFF_CACHE.load(Relaxed),
                                "esp_cache": mining::mining::ESP8266NH_DIFF_CACHE.load(Relaxed),
                                "shares_total": mining::mining::VERIFY_SHARES_TOTAL.load(Relaxed),
                                "shares_pending": mining::mining::GLOBAL_SHARES_INCREASE.load(Relaxed),
                                "balances": serde_json::to_value(&*mining::mining::BALANCES_TO_UPDATE).unwrap(),
                                "blocks": verify_blocks.len(),
                                "block_info": serde_json::to_value(&verify_blocks).unwrap(),
                                "observations": serde_json::to_value(&*mining::mining::VERIFY_OBSERVATIONS).unwrap(),
                                "last_hash": mining::mining::LAST_BLOCK_HASH.load().as_str(),
                            });
                            return Ok(warp::reply::json(&combined));"#,
            "read-only state probe",
        )
    })?;

    // The snapshot above needs `Clone`, added only to the disposable type.
    patch_file(stage, "src/mining/stats.rs", |stats| {
        replace_once(
            stats,
            "#[derive(Serialize, Deserialize, Debug)]\npub struct BlockInfos",
            "#[derive(Serialize, Deserialize, Debug, Clone)]\npub struct BlockInfos",
            "clone read-only block snapshot",
        )
    })?;

    patch_file(stage, "src/mining/mining.rs", |mining| {
        replace_once(
            mining,
            "static LAST_BLOCK_HASH:",
            "pub static LAST_BLOCK_HASH:",
            "read-only hash visibility",
        )?;
        // Keeping only the latest observation per connection is enough for
        // assertions and bounds memory use during churn scenarios. The map is
        // keyed like MINER_STATS: ConnId serializes as the same 16-hex-digit key.
        mining.push_str("\npub static VERIFY_OBSERVATIONS: LazyLock<DashMap<ConnId, serde_json::Value>> = LazyLock::new(DashMap::new);\n");
        // The pool has no lifetime share total. Count accepted shares next to
        // the pending counter, so `shares_total` is never decremented by a sync.
        mining.push_str("\npub static VERIFY_SHARES_TOTAL: AtomicU64 = AtomicU64::new(0);\n");
        let pending_increment = "            GLOBAL_SHARES_INCREASE.fetch_add(\n                // config.update_miners_stats_every as u64,\n                1u64,\n                std::sync::atomic::Ordering::Relaxed,\n            );\n";
        replace_once(
            mining,
            pending_increment,
            &format!("{pending_increment}            VERIFY_SHARES_TOTAL.fetch_add(1, Relaxed);\n"),
            "accepted share total",
        )?;
        // Record the inputs and results of the mining calculation at the end of
        // each share iteration. Values are captured, never recalculated.
        replace_once(
            mining,
            "        is_first_share = false;",
            r#"
        VERIFY_OBSERVATIONS.insert(conn.id.clone(), serde_json::json!({
            "user": conn.miner_stat.username, "accepted": conn.miner_stat.accepted_shares,
            "rejected": conn.miner_stat.rejected_shares, "reward": reward,
            "hashrate_calc": hashrate_calc, "hashrate": hashrate, "difficulty": diff,
            "tier": req_difficulty, "worker": conn.miner_stat.worker_slot.id(),
            "reward_div": reward_div, "sample_count": conn.miner_stat.share_times_list.len(),
            "sample_sum": conn.miner_stat.total_share_times,
            "share_time": conn.miner_stat.share_time, "ping": conn.miner_stat.ping,
            "samples": conn.miner_stat.share_times_list, "expected_time": expected_share_time,
            "hash": new_hash_str, "nonce": random, "last_min_shares": conn.miner_stat.last_min_shares
        }));
        is_first_share = false;"#,
            "mining observation",
        )
    })
}

// ---- Step 2c: test control channel -----------------------------------------

/// Adds a stdin reader to the pool:
///
/// * `refresh` calls the production `resync_pool_rewards` and prints
///   `VERIFY_REFRESH_OK` (or `VERIFY_REFRESH_ERROR: ...`);
/// * `shutdown` sends on the same channel the Ctrl-C handler uses.
///
/// Both call the production operations used by the scheduler and Ctrl-C. They
/// do not implement alternate refresh or shutdown behavior.
fn add_control_channel(stage: &Path) -> Result<()> {
    patch_file(stage, "src/main.rs", |main| {
        replace_once(
            main,
            "    ctrlc::set_handler(move || {",
            r#"    // Test copy only: use the same shutdown channel as Ctrl-C on every OS.
    let verify_runtime = tokio::runtime::Handle::current();
    let verify_shutdown = tx.clone();
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::stdin().lock().lines() {
            if matches!(line.as_deref(), Ok("refresh")) {
                verify_runtime.block_on(async {
                    match sync::sync::resync_pool_rewards(CONFIG.get().unwrap()).await {
                        Ok(()) => println!("VERIFY_REFRESH_OK"),
                        Err(err) => println!("VERIFY_REFRESH_ERROR: {err}"),
                    }
                });
            }
            if matches!(line.as_deref(), Ok("shutdown")) {
                let _ = verify_shutdown.send("s".to_owned());
                break;
            }
        }
    });
    ctrlc::set_handler(move || {"#,
            "stdin shutdown",
        )
    })
}

// ---- Step 2d: in-module tests ----------------------------------------------

/// Appends each `injected/*.rs` file to the end of its production module.
///
/// Rust privacy is module based. Appending inside the owning module lets the
/// tests call private helpers without making them public in the real crate.
/// Every injected file is `#[cfg(test)]`, so the runtime binary does not
/// contain it.
fn append_injected_tests(stage: &Path) -> Result<()> {
    for (target, tests) in [
        ("src/kolka/kolka.rs", include_str!("../injected/kolka.rs")),
        (
            "src/mining/mining.rs",
            include_str!("../injected/mining.rs"),
        ),
        (
            "src/mining/id_allocator.rs",
            include_str!("../injected/allocator.rs"),
        ),
        ("src/cfg/config.rs", include_str!("../injected/config.rs")),
        ("src/data/data.rs", include_str!("../injected/data.rs")),
    ] {
        patch_file(stage, target, |text| {
            text.push_str(tests);
            Ok(())
        })?;
    }
    Ok(())
}

// ---- Step 2e: sensitivity mutations ----------------------------------------

/// Injects one known bug into the stage.
///
/// Mutations are negative controls. `check_sensitivity.py` uses them to prove
/// that a matching assertion fails. They are never applied to repository source.
fn apply_mutation(stage: &Path, name: &str) -> Result<()> {
    let (file, from, to) = match name {
        // Doubles every regular reward.
        "reward" => (
            "src/kolka/kolka.rs",
            "let mut output = hashrate.ln() / reward_div as f32;",
            "let mut output = 2.0 * hashrate.ln() / reward_div as f32;",
        ),
        // Discards shares accepted while an upload is in flight.
        "pending" => (
            "src/sync/sync.rs",
            "pending_shares.fetch_sub(block_increase, Relaxed);",
            "pending_shares.store(0, Relaxed);",
        ),
        // Accepts any dashboard key.
        "auth" => (
            "src/web/web.rs",
            "if ct_eq(provided_key, &dash_key_clone) {",
            "if true {",
        ),
        // Never releases a disconnected worker's slot.
        "release" => (
            "src/handler/handler.rs",
            "\n            alloc.release(",
            "\n            if false { alloc.release(",
        ),
        _ => bail!("Unknown mutation {name}"),
    };
    patch_file(stage, file, |text| {
        replace_once(text, from, to, "sensitivity mutation")?;
        if name == "release" {
            // Close the `if false {` block opened above.
            replace_once(
                text,
                "            alloc.is_empty()",
                "            }\n            alloc.is_empty()",
                "release mutation closure",
            )?;
        }
        Ok(())
    })
}

// ---- Step 3: compile -------------------------------------------------------

/// Runs a prepared Cargo command until it exits, killing it if `cancelled`
/// becomes set.
fn run_cargo(
    mut command: Command,
    log: &Path,
    cancelled: &AtomicBool,
    interrupted: &str,
) -> Result<ExitStatus> {
    let log = fs::File::create(log)?;
    let mut cargo = command
        // RUSTFLAGS from the caller's environment would change the build and
        // invalidate the shared target cache.
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()
        .context("Could not start Cargo")?;
    loop {
        if let Some(status) = cargo.try_wait()? {
            return Ok(status);
        }
        if cancelled.load(Ordering::SeqCst) {
            let _ = cargo.kill();
            let _ = cargo.wait();
            bail!("{interrupted}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Builds the ordinary release pool binary launched by runtime scenarios.
///
/// Reusing one target directory keeps rebuilds cheap after the first run.
fn build_release_binary(
    work: &Path,
    source: &Path,
    target: &Path,
    benchmark: bool,
    cancelled: &AtomicBool,
) -> Result<()> {
    let log = work.join("build.log");
    println!(
        "Refreshing pool copy and building release binary (cached; log: {})",
        log.display()
    );
    let mut command = Command::new("cargo");
    command.args(["build", "--release"]);
    if benchmark {
        command.args(["--locked", "--features", "bench"]);
    }
    command
        .arg("--manifest-path")
        .arg(source.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target);
    let status = run_cargo(command, &log, cancelled, "Interrupted during pool build")?;
    anyhow::ensure!(status.success(), "Pool build failed; see {}", log.display());
    Ok(())
}

/// Builds the test executable and records its path in `work/internal-binary.txt`.
///
/// Each internal scenario then runs exactly one injected test from it, in an
/// isolated directory.
fn build_internal_tests(
    work: &Path,
    source: &Path,
    target: &Path,
    cancelled: &AtomicBool,
) -> Result<()> {
    let log = work.join("internal-build.jsonl");
    let mut command = Command::new("cargo");
    command
        .args([
            "test",
            "--release",
            "--no-run",
            "--message-format=json",
            "--manifest-path",
        ])
        .arg(source.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target);
    let status = run_cargo(command, &log, cancelled, "Interrupted internal build")?;
    anyhow::ensure!(
        status.success(),
        "Internal test build failed; see work/internal-build.jsonl"
    );
    // `cargo test --no-run` emits one JSON message per artifact. The crate's
    // test-profile artifact is the executable that contains the injected tests.
    let test_binary = fs::read_to_string(&log)?
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find_map(|message| {
            let is_test =
                message["reason"] == "compiler-artifact" && message["profile"]["test"] == true;
            if is_test {
                message["executable"].as_str().map(str::to_owned)
            } else {
                None
            }
        })
        .context("Missing internal test executable")?;
    fs::write(work.join("internal-binary.txt"), test_binary)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patches_fail_closed_on_source_drift() {
        for input in ["absent", "needle needle"] {
            let mut text = input.to_string();
            assert!(replace_once(&mut text, "needle", "replacement", "test").is_err());
            assert_eq!(text, input);
        }
    }
}
