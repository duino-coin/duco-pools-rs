//! Builds the disposable copy of the pool that the verifier exercises.
//!
//! The verifier deliberately does not compile or edit the repository's production
//! sources in place. Instead, this module mirrors them into `work/source-next`,
//! applies a small, fail-closed adapter, and then mirrors the result into
//! `work/source` for Cargo to compile. The adapter redirects external services to
//! local mocks, exposes otherwise-private state for assertions, adds a stdin test
//! control channel, and appends the tests from `injected/` to their owning modules.
//! None of those changes are copied back to the repository root.

use anyhow::{bail, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

fn replace_once(text: &mut String, from: &str, to: &str, label: &str) -> Result<()> {
    // These edits depend on recognizable production source, not on line numbers.
    // Requiring exactly one match prevents a pool refactor from silently producing
    // a partly instrumented binary or patching the wrong occurrence.
    let count = text.matches(from).count();
    if count != 1 {
        bail!("Test-copy patch '{label}' expected one match, found {count}. Pool source changed; update the adapter, not the pool.");
    }
    *text = text.replacen(from, to, 1);
    Ok(())
}

fn write_changed(path: &Path, data: &[u8]) -> Result<()> {
    // Preserve timestamps for unchanged files so Cargo can reuse the target cache.
    if fs::read(path).ok().as_deref() != Some(data) {
        fs::write(path, data)?;
    }
    Ok(())
}

fn remove_owned(path: &Path, work: &Path) -> Result<()> {
    // `mirror` deletes stale entries. Prove every deletion stays below our own
    // work directory and reject symlinks so a crafted tree cannot escape it.
    let ty = fs::symlink_metadata(path)?.file_type();
    anyhow::ensure!(
        !ty.is_symlink(),
        "Refusing work-directory symlink: {}",
        path.display()
    );
    let resolved = path.canonicalize()?;
    let boundary = work.canonicalize()?;
    anyhow::ensure!(
        resolved.starts_with(&boundary) && resolved != boundary,
        "Unsafe cleanup path"
    );
    if ty.is_dir() {
        for entry in fs::read_dir(path)? {
            remove_owned(&entry?.path(), work)?;
        }
        fs::remove_dir(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn mirror(from: &Path, to: &Path, work: &Path) -> Result<()> {
    // This is a synchronizing copy, not an overlay: files removed from production
    // must also disappear from the disposable source tree on the next run.
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
        let ty = entry.file_type()?;
        anyhow::ensure!(
            !ty.is_symlink(),
            "Refusing source symlink: {}",
            entry.path().display()
        );
        if ty.is_dir() {
            mirror(&entry.path(), &to.join(entry.file_name()), work)?;
        } else {
            write_changed(&to.join(entry.file_name()), &fs::read(entry.path())?)?;
        }
    }
    Ok(())
}

pub fn build(
    root: &Path,
    home: &Path,
    cancelled: Arc<AtomicBool>,
    mutation: Option<String>,
    runtime_tests: bool,
    internal_tests: bool,
) -> Result<PathBuf> {
    build_copy(
        root,
        home,
        cancelled,
        mutation,
        runtime_tests,
        internal_tests,
        false,
    )
}

/// Keep production hot paths intact for timing. Only endpoint redirection and
/// the process control channel are installed in a benchmark copy.
pub fn build_benchmark(root: &Path, home: &Path, cancelled: Arc<AtomicBool>) -> Result<PathBuf> {
    build_copy(root, home, cancelled, None, true, false, true)
}

fn build_copy(
    root: &Path,
    home: &Path,
    cancelled: Arc<AtomicBool>,
    mutation: Option<String>,
    runtime_tests: bool,
    internal_tests: bool,
    benchmark: bool,
) -> Result<PathBuf> {
    // Build a complete candidate tree before updating `work/source`. This keeps
    // the long-lived Cargo source/target pair coherent and lets unchanged files
    // retain their timestamps across verifier runs.
    let work = home.join("work");
    fs::create_dir_all(&work)?;
    // Keep the benchmark target close to the repository root. Also convert the
    // verbatim path returned by Windows canonicalize() before passing it to
    // Cargo: MSVC misparses generated mimalloc source paths beginning with \\?\.
    let target = if benchmark {
        root.join("target/pool-verifier-benchmark")
    } else {
        work.join("target")
    };
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
    let stage = work.join("source-next");
    if stage.exists() {
        remove_owned(&stage, &work)?;
    }
    fs::create_dir_all(&stage)?;
    mirror(&root.join("src"), &stage.join("src"), &work)?;
    for name in ["Cargo.toml", "build.rs"] {
        fs::copy(root.join(name), stage.join(name))?;
    }
    if benchmark {
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
    let previous_lock = work.join("source/Cargo.lock");
    let lock = if root.join("Cargo.lock").exists() {
        root.join("Cargo.lock")
    } else {
        previous_lock
    };
    if lock.exists() {
        fs::copy(lock, stage.join("Cargo.lock"))?;
    }

    // ---- Network redirection -------------------------------------------------
    // The production sync code normally talks to public services. Replace only
    // the endpoint expressions; request construction, retry logic, multipart
    // encoding, and accounting remain production code under test.
    let sync_path = stage.join("src/sync/sync.rs");
    let mut sync = fs::read_to_string(&sync_path)?.replace("\r\n", "\n");
    replace_once(&mut sync, "client.get(\"https://api.ipify.org/\")", "client.get(format!(\"{}/ip\", std::env::var(\"POOL_VERIFY_HTTP\").expect(\"mock HTTP URL\")))", "IP endpoint")?;
    let start = sync
        .find("    let request_url = format!(")
        .context("Missing sync URL construction")?;
    let end = sync[start..]
        .find("    let form = reqwest::multipart::Form::new();")
        .context("Missing multipart boundary")?
        + start;
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
    if patched_url.contains("        config.server_ip,") {
        replace_once(
            &mut patched_url,
            "        config.server_ip,",
            "        std::env::var(\"POOL_VERIFY_HTTP\").expect(\"mock HTTP URL\"),",
            "sync URL host",
        )?;
    } else {
        replace_once(
            &mut patched_url,
            "        \"server.duinocoin.com\", //TODO: temp fix",
            "        std::env::var(\"POOL_VERIFY_HTTP\").expect(\"mock HTTP URL\"),",
            "sync URL host",
        )?;
    }
    sync.replace_range(start..end, &patched_url);
    fs::write(&sync_path, sync)?;

    // ---- Read-only observation surface --------------------------------------
    // Add verifier state to the already-authenticated dashboard response. Tests
    // observe private counters at stable iteration boundaries instead of
    // duplicating the pool's calculations as their oracle.
    if !benchmark {
        let web_path = stage.join("src/web/web.rs");
        let mut web = fs::read_to_string(&web_path)?.replace("\r\n", "\n");
        // Take one block snapshot before building the JSON. Locking separately for
        // `len` and serialization could expose two different moments in one response.
        replace_once(
            &mut web,
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
        )?;
        fs::write(web_path, web)?;
        let stats_path = stage.join("src/mining/stats.rs");
        let mut stats = fs::read_to_string(&stats_path)?.replace("\r\n", "\n");
        replace_once(
            &mut stats,
            "#[derive(Serialize, Deserialize, Debug)]\npub struct BlockInfos",
            "#[derive(Serialize, Deserialize, Debug, Clone)]\npub struct BlockInfos",
            "clone read-only block snapshot",
        )?;
        fs::write(stats_path, stats)?;
        let mining_path = stage.join("src/mining/mining.rs");
        let mut mining = fs::read_to_string(&mining_path)?.replace("\r\n", "\n");
        replace_once(
            &mut mining,
            "static LAST_BLOCK_HASH:",
            "pub static LAST_BLOCK_HASH:",
            "read-only hash visibility",
        )?;
        // One latest observation per connection is enough for assertions and bounds
        // the instrumentation's memory use during churn scenarios.
        // Keyed like MINER_STATS; ConnId serializes as the same 16-hex-digit key.
        mining.push_str("\npub static VERIFY_OBSERVATIONS: LazyLock<DashMap<ConnId, serde_json::Value>> = LazyLock::new(DashMap::new);\n");
        // The pool has no lifetime share total; count accepted shares next to the
        // pending counter so `shares_total` keeps its meaning (never decremented).
        mining.push_str("\npub static VERIFY_SHARES_TOTAL: AtomicU64 = AtomicU64::new(0);\n");
        let pending_increment = "            GLOBAL_SHARES_INCREASE.fetch_add(\n                // config.update_miners_stats_every as u64,\n                1u64,\n                std::sync::atomic::Ordering::Relaxed,\n            );\n";
        replace_once(
            &mut mining,
            pending_increment,
            &format!("{pending_increment}            VERIFY_SHARES_TOTAL.fetch_add(1, Relaxed);\n"),
            "accepted share total",
        )?;
        replace_once(
            &mut mining,
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
        )?;
        fs::write(mining_path, mining)?;
    }

    // ---- Test control channel ------------------------------------------------
    // Process tests need deterministic refresh and graceful shutdown triggers.
    // Stdin calls the same production operations used by the scheduler/Ctrl-C; it
    // does not implement alternate refresh or shutdown behavior.
    let main_path = stage.join("src/main.rs");
    let mut main = fs::read_to_string(&main_path)?.replace("\r\n", "\n");
    replace_once(
        &mut main,
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
    )?;
    fs::write(main_path, main)?;

    // ---- In-module tests -----------------------------------------------------
    // Rust privacy is module based. Appending each file inside the corresponding
    // production module lets focused tests call private helpers without making
    // those helpers public in the real crate. Every injected file is guarded by
    // `#[cfg(test)]`, so it is absent from the runtime release binary.
    if !benchmark {
        for (target, extra) in [
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
            let path = stage.join(target);
            let mut text = fs::read_to_string(&path)?;
            text.push_str(extra);
            fs::write(path, text)?;
        }
    }
    // Deliberate mutations are negative controls: each introduces a known bug in
    // the disposable copy so `check_sensitivity.py` can prove a matching
    // assertion fails. They are never applied to repository source.
    if let Some(name) = mutation.as_deref() {
        let (path, from, to) = match name {
            "reward" => (
                "src/kolka/kolka.rs",
                "let mut output = hashrate.ln() / reward_div as f32;",
                "let mut output = 2.0 * hashrate.ln() / reward_div as f32;",
            ),
            "pending" => (
                "src/sync/sync.rs",
                "pending_shares.fetch_sub(block_increase, Relaxed);",
                "pending_shares.store(0, Relaxed);",
            ),
            "auth" => (
                "src/web/web.rs",
                "if ct_eq(provided_key, &dash_key_clone) {",
                "if true {",
            ),
            "release" => (
                "src/handler/handler.rs",
                "\n            alloc.release(",
                "\n            if false { alloc.release(",
            ),
            _ => bail!("Unknown mutation {name}"),
        };
        let path = stage.join(path);
        let mut text = fs::read_to_string(&path)?.replace("\r\n", "\n");
        replace_once(&mut text, from, to, "sensitivity mutation")?;
        if name == "release" {
            replace_once(
                &mut text,
                "            alloc.is_empty()",
                "            }\n            alloc.is_empty()",
                "release mutation closure",
            )?;
        }
        fs::write(path, text)?;
    }
    // Publish the fully adapted stage only after all anchors have been validated.
    let source = work.join("source");
    mirror(&stage, &source, &work)?;
    if runtime_tests {
        // Runtime scenarios launch the ordinary release binary. Reusing a target
        // directory makes the source mirror cheap after the first verifier run.
        let log = fs::File::create(work.join("build.log"))?;
        println!(
            "Refreshing pool copy and building release binary (cached; log: {})",
            work.join("build.log").display()
        );
        let mut command = Command::new("cargo");
        command.args(["build", "--release"]);
        if benchmark {
            command.arg("--locked");
        }
        if benchmark {
            command.args(["--features", "bench"]);
        }
        let mut compiler = command
            .arg("--manifest-path")
            .arg(source.join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", &target)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .context("Could not start Cargo")?;
        let status = loop {
            if let Some(status) = compiler.try_wait()? {
                break status;
            }
            if cancelled.load(Ordering::SeqCst) {
                let _ = compiler.kill();
                let _ = compiler.wait();
                bail!("Interrupted during pool build");
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        anyhow::ensure!(
            status.success(),
            "Pool build failed; see {}",
            work.join("build.log").display()
        );
    }
    if internal_tests {
        // `cargo test --no-run` produces several JSON artifact messages. Record
        // the executable belonging to the crate's test profile so each internal
        // scenario can run one exact injected test in an isolated directory.
        let internal_log = fs::File::create(work.join("internal-build.jsonl"))?;
        let mut compiler = Command::new("cargo")
            .args([
                "test",
                "--release",
                "--no-run",
                "--message-format=json",
                "--manifest-path",
            ])
            .arg(source.join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", &target)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .stdin(Stdio::null())
            .stdout(internal_log.try_clone()?)
            .stderr(internal_log)
            .spawn()?;
        let status = loop {
            if let Some(status) = compiler.try_wait()? {
                break status;
            }
            if cancelled.load(Ordering::SeqCst) {
                let _ = compiler.kill();
                let _ = compiler.wait();
                bail!("Interrupted internal build");
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        anyhow::ensure!(
            status.success(),
            "Internal test build failed; see work/internal-build.jsonl"
        );
        let test_binary = fs::read_to_string(work.join("internal-build.jsonl"))?
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find_map(|v| {
                if v["reason"] == "compiler-artifact" && v["profile"]["test"] == true {
                    v["executable"].as_str().map(str::to_owned)
                } else {
                    None
                }
            })
            .context("Missing internal test executable")?;
        fs::write(work.join("internal-binary.txt"), test_binary)?;
    }
    Ok(target
        .join("release")
        .join(format!("duco-pool-rs{}", std::env::consts::EXE_SUFFIX)))
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
