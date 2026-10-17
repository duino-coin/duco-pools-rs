//! Additional runtime scenarios and the injected internal tests.
//!
//! This file extends the scenario registry in `suites` without making
//! production code responsible for test orchestration. `internal` cases each
//! run one test appended under `#[cfg(test)]` (see `injected/`). All other
//! cases run against a separate release pool process, like those in `suites`.

use crate::harness::{ensure_accepted, is_accepted, stat, Harness, Options, WAIT};
use crate::mock::{Fault, Mock};
use crate::suites::Case;
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::time::{sleep, timeout};

pub const CASES: &[Case] = &[
    Case::new("internal", "reward-vectors"),
    Case::new("internal", "tier-transitions"),
    Case::new("internal", "adaptive-vectors"),
    Case::new("internal", "fallback-and-sanitizers"),
    Case::new("internal", "seeded-allocator"),
    Case::new("internal", "config-defaults-roundtrip"),
    Case::new("internal", "bans-persistence"),
    // Disabled: "interval-two-counts" and "interval-five-counts" (see the
    // commented-out scenario in `run`).
    Case::new("counters", "interval-two-rewards"),
    Case::new("counters", "interval-five-rewards"),
    Case::new("counters", "partial-disconnect"),
    Case::new("counters", "partial-shutdown"),
    Case::new("counters", "publication-rejection-boundaries"),
    Case::new("counters", "reward-formula-mining"),
    Case::new("protocol", "fragmented-frames"),
    Case::new("protocol", "frame-boundaries"),
    Case::new("protocol", "empty-frame"),
    Case::new("protocol", "field-count-limits"),
    Case::new("protocol", "nonce-overflow"),
    Case::new("protocol", "motd-version"),
    Case::new("protocol", "invalid-hashrates"),
    Case::new("protocol", "estimated-hashrates"),
    Case::new("protocol", "modified-hashrate"),
    Case::new("protocol", "metadata-sanitization"),
    Case::new("protocol", "metadata-defaults"),
    Case::new("protocol", "iot-boundaries"),
    Case::new("protocol", "password-boundaries"),
    Case::new("protocol", "legacy-key"),
    Case::new("protocol", "hash-chain-block-details"),
    Case::new("kolka", "reverse-mixed-reuse"),
    Case::new("kolka", "overlapping-churn"),
    Case::new("kolka", "worker-limit-recovery"),
    Case::new("lifecycle", "adaptive-mining-average"),
    Case::new("lifecycle", "all-tier-initial-jobs"),
    Case::new("lifecycle", "dashboard-disabled"),
    Case::new("lifecycle", "dashboard-key-boundary"),
    Case::new("lifecycle", "dashboard-schema"),
    Case::new("lifecycle", "guessed-port"),
    Case::new("lifecycle", "preexisting-ip-ban"),
    Case::new("lifecycle", "ban-reconnect"),
    Case::new("lifecycle", "localhost-exemption"),
    Case::new("lifecycle", "refreshed-bans"),
    Case::new("lifecycle", "refreshed-rewards"),
    Case::new("lifecycle", "startup-invalid-config"),
    Case::new("lifecycle", "startup-missing-field"),
    Case::new("lifecycle", "startup-invalid-rewards"),
    Case::new("lifecycle", "startup-missing-tier"),
    Case::new("lifecycle", "startup-invalid-bans"),
    Case::new("lifecycle", "startup-ip-disconnect"),
    Case::new("lifecycle", "startup-master-reject"),
    Case::new("lifecycle", "startup-master-disconnect"),
    Case::new("lifecycle", "startup-master-invalid-utf8"),
    Case::new("lifecycle", "startup-master-delay"),
    Case::new("sync", "transport-disconnect-retry"),
    Case::new("sync", "transport-timeout-retry"),
    Case::new("sync", "ip-fallback"),
    Case::new("sync", "ip-no-cache"),
    Case::new("sync", "staging-workers-write-failure"),
    Case::new("sync", "staging-rewards-write-failure"),
    Case::new("sync", "shutdown-held-upload"),
    Case::slow("connections", "active-job-timeout"),
    Case::slow("lifecycle", "rate-window-reset"),
];

/// Scenarios that expect the pool to refuse to start. They are launched without
/// waiting for readiness. (`startup-master-delay` starts successfully;
/// `startup-and-auth` lives in `suites`.)
fn is_startup_failure(name: &str) -> bool {
    name.starts_with("startup-") && name != "startup-and-auth" && name != "startup-master-delay"
}

/// Adjusts the shared default [`Options`] where a scenario needs a distinct
/// fixture. A `null` config value removes that key entirely.
pub fn options(case: &Case, o: &mut Options) {
    o.case = case.name;
    match case.name {
        "interval-two-counts" | "interval-two-rewards" | "publication-rejection-boundaries" => {
            o.stats_every = 2
        }
        "interval-five-counts"
        | "interval-five-rewards"
        | "partial-disconnect"
        | "partial-shutdown" => o.stats_every = 5,
        // Keys of 20 characters or fewer disable `/data`; 21 is the shortest
        // accepted key.
        "dashboard-disabled" => {
            o.config["dashboard_key"] = json!("x".repeat(20));
            o.dashboard_disabled = true;
        }
        "dashboard-key-boundary" => o.config["dashboard_key"] = json!("x".repeat(21)),
        "guessed-port" => o.config["guessPort"] = json!(true),
        "worker-limit-recovery" => o.max_workers = 1,
        "rate-window-reset" => o.rate = 2,
        "preexisting-ip-ban" => o.initial_files.push((
            "config/bans.json".into(),
            json!({"bannedIPs":["127.0.0.2"],"bannedUsernames":[]}).to_string(),
        )),
        "startup-invalid-config" => o
            .initial_files
            .push(("config/config.json".into(), "{".into())),
        "startup-missing-field" => o.config["host"] = Value::Null,
        _ => {}
    }
    if is_startup_failure(case.name) {
        o.launch_only = true;
    }
    if matches!(
        case.name,
        "interval-two-rewards"
            | "interval-five-rewards"
            | "partial-disconnect"
            | "partial-shutdown"
            | "hash-chain-block-details"
            | "transport-disconnect-retry"
            | "transport-timeout-retry"
            | "shutdown-held-upload"
    ) {
        o.blocks = true;
    }
    if case.name == "shutdown-held-upload" {
        o.sync_secs = 1;
    }
}

/// Queues mock faults and fixture changes for scenarios that need them during
/// startup. Called before the pool launches, so its first requests cannot race
/// ahead of scenario setup.
pub fn configure_mock(name: &str, mock: &Mock) {
    let master = || mock.shared.master_reply.lock().unwrap();
    match name {
        "startup-invalid-rewards" => mock.respond("/poolrewards.json", 200, "{"),
        "startup-missing-tier" => {
            mock.shared
                .rewards
                .lock()
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove("DUE");
        }
        "startup-invalid-bans" => mock.respond("/bans.json", 200, "{}"),
        // Enough faults to exhaust the pool's IP-lookup retries.
        "startup-ip-disconnect" => {
            for _ in 0..4 {
                mock.fault("/ip", Fault::Disconnect);
            }
        }
        "startup-master-reject" => master().reply = b"LoginRejected".to_vec(),
        "startup-master-disconnect" => master().greeting.clear(),
        "startup-master-invalid-utf8" => master().greeting = vec![255],
        "startup-master-delay" => master().delay_ms = 200,
        // The first lookup (during login) succeeds. The second, for the
        // immediate startup sync, times out while no IP has been cached yet.
        "ip-no-cache" => {
            mock.fault("/ip", Fault::Delay(0));
            mock.fault("/ip", Fault::Delay(4000));
        }
        _ => {}
    }
}

/// Runs one injected test from the internal test executable in `dir`.
///
/// Rust test filtering matches substrings. The full path is passed with
/// `--exact`, and the runner must report exactly one test, so a renamed
/// injected test cannot silently pass.
pub async fn run_internal(case: &Case, home: &Path, dir: &Path) -> Result<()> {
    let test = match case.name {
        "reward-vectors" => "kolka::kolka::verifier_tests::reward_vectors",
        "tier-transitions" => "kolka::kolka::verifier_tests::tier_transitions",
        "adaptive-vectors" => "kolka::kolka::verifier_tests::adaptive_vectors",
        "fallback-and-sanitizers" => "mining::mining::verifier_tests::fallback_and_sanitizers",
        "seeded-allocator" => "mining::id_allocator::verifier_tests::seeded_sequences",
        "config-defaults-roundtrip" => "cfg::config::verifier_tests::defaults_and_roundtrip",
        "bans-persistence" => "data::data::verifier_tests::bans_persistence",
        _ => anyhow::bail!("Unknown internal case"),
    };
    let binary = std::fs::read_to_string(home.join("work/internal-binary.txt"))?;
    let log_path = dir.join("internal.log");
    let log = std::fs::File::create(&log_path)?;
    let mut child = tokio::process::Command::new(binary.trim())
        .args([test, "--exact", "--nocapture"])
        .current_dir(dir)
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()?;
    let status = match timeout(Duration::from_secs(20), child.wait()).await {
        Ok(status) => status?,
        Err(_) => {
            child.kill().await?;
            anyhow::bail!("Internal test timed out; child killed and reaped");
        }
    };
    let log = std::fs::read_to_string(log_path)?;
    anyhow::ensure!(
        log.contains("running 1 test"),
        "Internal test filter matched no test: {log}"
    );
    anyhow::ensure!(status.success(), "Internal assertion failed: {log}");
    Ok(())
}

/// Runs the scenario named by `case`.
pub async fn run(case: &Case, h: &mut Harness) -> Result<()> {
    match case.name {
        name if is_startup_failure(name) => startup_failure(h, name).await,
        "startup-master-delay" => startup_master_delay(h).await,
        // counters
        // "interval-two-counts" | "interval-five-counts" => interval_counts(case, h).await,
        "interval-two-rewards" | "interval-five-rewards" => interval_rewards(h).await,
        "partial-disconnect" => partial_batch(h, false).await,
        "partial-shutdown" => partial_batch(h, true).await,
        "publication-rejection-boundaries" => publication_rejection_boundaries(h).await,
        "reward-formula-mining" => reward_formula_mining(h).await,
        // protocol
        "fragmented-frames" => fragmented_frames(h).await,
        "frame-boundaries" => frame_boundaries(h).await,
        "empty-frame" => empty_frame(h).await,
        "field-count-limits" => field_count_limits(h).await,
        "nonce-overflow" => nonce_overflow(h).await,
        "motd-version" => motd_version(h).await,
        "invalid-hashrates" => invalid_hashrates(h).await,
        "estimated-hashrates" => estimated_hashrates(h).await,
        "modified-hashrate" => modified_hashrate(h).await,
        "metadata-sanitization" => metadata(h, Metadata::Sanitized).await,
        "metadata-defaults" => metadata(h, Metadata::Defaults).await,
        "legacy-key" => metadata(h, Metadata::LegacyKey).await,
        "iot-boundaries" => iot_boundaries(h).await,
        "password-boundaries" => password_boundaries(h).await,
        "hash-chain-block-details" => hash_chain_block_details(h).await,
        // kolka
        "reverse-mixed-reuse" => reverse_mixed_reuse(h).await,
        "overlapping-churn" => overlapping_churn(h).await,
        "worker-limit-recovery" => worker_limit_recovery(h).await,
        // lifecycle
        "adaptive-mining-average" => adaptive_mining_average(h).await,
        "all-tier-initial-jobs" => all_tier_initial_jobs(h).await,
        "dashboard-disabled" => dashboard_key(h, false).await,
        "dashboard-key-boundary" => dashboard_key(h, true).await,
        "dashboard-schema" => dashboard_schema(h).await,
        "guessed-port" => guessed_port(h).await,
        "preexisting-ip-ban" => ban(h, Ban::PreexistingIp).await,
        "ban-reconnect" => ban(h, Ban::Reconnect).await,
        "localhost-exemption" => ban(h, Ban::LocalhostExempt).await,
        "refreshed-bans" => ban(h, Ban::Refreshed).await,
        "refreshed-rewards" => refreshed_rewards(h).await,
        "rate-window-reset" => rate_window_reset(h).await,
        // sync
        "transport-disconnect-retry" => transport_retry(h, Fault::Disconnect).await,
        "transport-timeout-retry" => transport_retry(h, Fault::Delay(4500)).await,
        "ip-fallback" => ip_fallback(h).await,
        "ip-no-cache" => ip_no_cache(h).await,
        "staging-workers-write-failure" => staging_write_failure(h, "workers_verifier.json").await,
        "staging-rewards-write-failure" => staging_write_failure(h, "rewards_verifier.json").await,
        "shutdown-held-upload" => shutdown_held_upload(h).await,
        // connections
        "active-job-timeout" => active_job_timeout(h).await,
        _ => anyhow::bail!("Unimplemented scenario {}", case.name),
    }
}

// ---- Shared assertions --------------------------------------------------------

/// Float equality with a 2e-5 relative tolerance (at least 1e-6 absolute).
fn near(actual: f64, expected: f64, label: &str) -> Result<()> {
    anyhow::ensure!(
        actual.is_finite() && (actual - expected).abs() <= 1e-6f64.max(expected.abs() * 2e-5),
        "{label}: {actual} != {expected}"
    );
    Ok(())
}

/// Waits for the mining observation of `user` with exactly these counters and
/// returns it. Observations are recorded at the end of each share iteration,
/// so this is a stable point to compare counters and rewards.
async fn observation(h: &Harness, user: &str, accepted: u64, rejected: u64) -> Result<Value> {
    let matches =
        |v: &Value| v["user"] == user && v["accepted"] == accepted && v["rejected"] == rejected;
    let s = h
        .wait_state("completed mining observation", |s| {
            s["verify"]["observations"]
                .as_object()
                .is_some_and(|m| m.values().any(matches))
        })
        .await?;
    Ok(s["verify"]["observations"]
        .as_object()
        .unwrap()
        .values()
        .find(|v| matches(v))
        .unwrap()
        .clone())
}

/// Reads the `PoolLogin` JSON the pool sent to the fake master.
fn master_login(h: &Harness) -> Result<Value> {
    let requests = h.mock.diagnostics();
    let login = requests["master_requests"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|v| v.as_str().and_then(|s| s.strip_prefix("PoolLogin,")))
        .context("login")?;
    Ok(serde_json::from_str(login)?)
}

// ---- Startup -------------------------------------------------------------------

/// Invalid config, rewards, bans, IP lookup or master login must stop startup
/// with a recognizable diagnostic.
async fn startup_failure(h: &mut Harness, name: &str) -> Result<()> {
    let diagnostic = match name {
        "startup-invalid-rewards" | "startup-invalid-bans" => "failed to update pool rewards",
        "startup-missing-tier" => "Failed to get DUE",
        "startup-ip-disconnect" | "startup-master-invalid-utf8" => "failed to login",
        "startup-master-reject" | "startup-master-disconnect" => "Unknown error",
        // startup-invalid-config, startup-missing-field
        _ => "panicked",
    };
    h.expect_startup_failure(diagnostic).await
}

/// A slow master greeting delays startup but does not break it.
async fn startup_master_delay(h: &Harness) -> Result<()> {
    h.miner("alice", "AVR", "delayed").await?.share().await?;
    Ok(())
}

// ---- counters ------------------------------------------------------------------

// Disabled scenario, kept for reference. Unlike `interval_rewards`, it expects
// the pending counter to rise on every share even when stats are only
// published every N shares.
//
// async fn interval_counts(case: &Case, h: &Harness) -> Result<()> {
//     let interval = if case.name.contains("two") { 2 } else { 5 };
//     let mut m = h.miner("alice", "AVR", "batch").await?;
//     let mut errors = vec![];
//     for i in 1..=interval * 2 + 1 {
//         m.share().await?;
//         observation(h, "alice", i, 0).await?;
//         let s = h.state().await?;
//         for key in ["shares_total", "shares_pending"] {
//             if s["verify"][key] != i {
//                 errors.push(format!("after {i} accepted: {key}={}", s["verify"][key]));
//             }
//         }
//     }
//     anyhow::ensure!(
//         errors.is_empty(),
//         "Reporting interval loses accepted work: {}",
//         errors.join("; ")
//     );
//     Ok(())
// }

/// With stats published only every 2 or 5 shares, every per-share reward
/// still accrues to the balance, and every block is counted.
async fn interval_rewards(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "rewards").await?;
    let mut earned = 0.0;
    for i in 1..=12 {
        m.share().await?;
        earned += observation(h, "alice", i, 0).await?["reward"]
            .as_f64()
            .context("reward")?;
    }
    let s = h.state().await?;
    near(
        s["verify"]["balances"]["alice"].as_f64().unwrap_or(0.0),
        earned,
        "All per-share rewards must accrue between publications",
    )?;
    anyhow::ensure!(
        s["verify"]["blocks"] == 12,
        "Block accounting differs from accepted BLOCK responses"
    );
    Ok(())
}

/// Three shares, fewer than the publication interval of 5, must not be lost
/// when the miner disconnects (`partial-disconnect`) or when the pool shuts
/// down (`partial-shutdown`, checked in the final upload). Every share is a
/// block worth 2.0.
async fn partial_batch(h: &mut Harness, shutdown: bool) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "partial").await?;
    for i in 1..=3 {
        m.share().await?;
        observation(h, "alice", i, 0).await?;
    }
    if shutdown {
        let index = h.mock.uploads().len();
        h.mock.status(200);
        h.stop().await?;
        let (_, u) = h.upload_after(index, |u| u.completed).await?;
        anyhow::ensure!(
            u.query["blockIncrease"] == "3" && u.parts["rewards"].json["alice"] == 6.0,
            "Shutdown lost partial batch: {u:?}"
        );
    } else {
        m.close().await?;
        h.empty().await?;
        let s = h.state().await?;
        anyhow::ensure!(
            s["verify"]["shares_total"] == 3
                && s["verify"]["shares_pending"] == 3
                && s["verify"]["balances"]["alice"] == 6.0,
            "Disconnected partial batch lost: {}",
            s["verify"]
        );
    }
    Ok(())
}

/// With a publication interval of 2, rejected shares on both sides of a
/// publication boundary change no totals, balances or blocks.
async fn publication_rejection_boundaries(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "boundaries").await?;
    let mut rejects = 0;
    for i in 1..=6 {
        m.share().await?;
        observation(h, "alice", i, rejects).await?;
        let before = h.state().await?;
        let job = m.job().await?;
        anyhow::ensure!(
            m.answer(&job, false).await? == "BAD,Incorrect result",
            "Wrong rejection"
        );
        rejects += 1;
        observation(h, "alice", i, rejects).await?;
        let after = h.state().await?;
        for key in ["shares_total", "shares_pending", "balances", "blocks"] {
            anyhow::ensure!(
                before["verify"][key] == after["verify"][key],
                "Rejected share changed {key}"
            );
        }
    }
    Ok(())
}

/// For one tier per device class, the 11th share's reward matches an
/// independent formula: `2 * ln(observed hashrate) / 1000`, plus 2.0 for a
/// block. The balance must equal earlier block rewards plus that reward.
async fn reward_formula_mining(h: &Harness) -> Result<()> {
    for (index, tier) in ["AVR", "ESP32", "LOW", "EXTREME"].iter().enumerate() {
        let user = format!("formula{index}");
        let mut m = h.miner(&user, tier, "formula").await?;
        // The first ten shares only earn block rewards (reward warmup).
        let mut block_extra = 0.0;
        for _ in 0..10 {
            if m.share().await? == "BLOCK" {
                block_extra += 2.0;
            }
        }
        let reply = m.share().await?;
        let o = observation(h, &user, 11, 0).await?;
        let hashrate = o["hashrate_calc"].as_f64().context("calculated hashrate")?;
        let block_reward = if reply == "BLOCK" { 2.0 } else { 0.0 };
        let expected = 2.0 * hashrate.ln() / 1000.0 + block_reward;
        near(
            o["reward"].as_f64().context("reward")?,
            expected,
            "independent mining reward",
        )?;
        near(
            h.state().await?["verify"]["balances"][&user]
                .as_f64()
                .context("balance")?,
            block_extra + expected,
            "reward ledger",
        )?;
        m.close().await?;
        h.empty().await?;
    }
    Ok(())
}

// ---- protocol ------------------------------------------------------------------

/// Requests and answers split into tiny TCP writes are reassembled.
async fn fragmented_frames(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "fragments").await?;
    for chunk in [b"JO".as_slice(), b"B,alice,", b"AVR\r", b"\n"] {
        m.send(chunk).await?;
        tokio::task::yield_now().await;
    }
    let job = m.read_job().await?;
    let answer = format!("{},1000,Verifier,fragments,chip,wallet\r\n", job.nonce);
    for chunk in answer.as_bytes().chunks(3) {
        m.send(chunk).await?;
        tokio::task::yield_now().await;
    }
    ensure_accepted(&m.line().await?)?;
    m.share().await?;
    Ok(())
}

/// Frames of 1023 and 1024 bytes (including the newline) are accepted. A
/// 1025-byte frame closes the connection.
async fn frame_boundaries(h: &Harness) -> Result<()> {
    for (i, size) in [1023usize, 1024, 1025].into_iter().enumerate() {
        let mut m = h.miner("alice", "AVR", &format!("size{i}")).await?;
        // Pad the IoT field so the whole line is exactly `size` bytes.
        let prefix = "JOB,alice,AVR,,";
        let frame = format!("{prefix}{}\n", "x".repeat(size - prefix.len() - 1));
        m.send(frame.as_bytes()).await?;
        if size <= 1024 {
            let job = m.read_job().await?;
            ensure_accepted(&m.answer(&job, true).await?)?;
            m.close().await?;
        } else {
            m.expect_closed(WAIT).await?;
        }
        h.empty().await?;
    }
    Ok(())
}

/// An empty line closes the connection.
async fn empty_frame(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "empty").await?;
    m.send(b"\r\n").await?;
    m.expect_closed(WAIT).await?;
    h.empty().await
}

/// A job request with 6 fields is accepted. 7 or 9 fields get
/// `BAD,Incorrect data` and a disconnect.
async fn field_count_limits(h: &Harness) -> Result<()> {
    for fields in [6, 7, 9] {
        let mut m = h.miner("alice", "AVR", "fields").await?;
        let frame = format!("JOB,alice,AVR{}\n", ",x".repeat(fields - 3));
        m.send(frame.as_bytes()).await?;
        if fields == 6 {
            let job = m.read_job().await?;
            ensure_accepted(&m.answer(&job, true).await?)?;
            m.close().await?;
        } else {
            anyhow::ensure!(m.line().await? == "BAD,Incorrect data", "field-count limit");
            m.expect_closed(WAIT).await?;
        }
        h.empty().await?;
    }
    Ok(())
}

/// A nonce one above `u64::MAX` is treated as a malformed answer.
async fn nonce_overflow(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "overflow").await?;
    m.job().await?;
    let reply = m
        .answer_fields("18446744073709551616".into(), "1000", "chip")
        .await?;
    anyhow::ensure!(reply == "BAD,No response to job", "overflow feedback");
    m.expect_closed(WAIT).await?;
    h.empty().await
}

/// MOTD can be repeated, and `DEBG_VERSION` returns the pool's crate version
/// (raw bytes, no newline).
async fn motd_version(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "version").await?;
    for _ in 0..3 {
        m.send(b"MOTD\n").await?;
        anyhow::ensure!(m.line().await? == "verifier-motd", "MOTD");
    }
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.toml"))?;
    let version = manifest
        .lines()
        .find_map(|l| l.strip_prefix("version = \""))
        .context("package version")?
        .trim_end_matches('"');
    m.send(b"DEBG_VERSION\n").await?;
    anyhow::ensure!(
        m.bytes(version.len()).await? == version.as_bytes(),
        "Version bytes"
    );
    m.share().await?;
    Ok(())
}

/// Zero, negative, NaN and infinite hashrates get `BAD,Invalid data`, do not
/// change difficulty, and leave the connection usable. All five values are
/// checked before failing, to report every problem at once.
async fn invalid_hashrates(h: &Harness) -> Result<()> {
    let mut errors = vec![];
    for (i, rate) in ["0", "-1", "NaN", "inf", "-inf"].iter().enumerate() {
        let user = format!("invalid{i}");
        let mut m = h.miner(&user, "AVR", "hashrate").await?;
        m.share().await?;
        let job = m.job().await?;
        let reply = m
            .answer_fields(job.nonce.to_string(), rate, "chip-verifier")
            .await?;
        if reply != "BAD,Invalid data" {
            errors.push(format!("{rate}: {reply}"));
        }
        let job = m.job().await?;
        if job.difficulty != 1 {
            errors.push(format!("{rate} changed difficulty to {}", job.difficulty));
        }
        ensure_accepted(&m.answer(&job, true).await?)?;
        m.close().await?;
        h.empty().await?;
    }
    anyhow::ensure!(
        errors.is_empty(),
        "Invalid-rate handling: {}",
        errors.join("; ")
    );
    Ok(())
}

/// When the hashrate is missing or not a number, the pool uses its own
/// positive estimate.
async fn estimated_hashrates(h: &Harness) -> Result<()> {
    for (i, rate) in [None, Some("not-a-number")].iter().enumerate() {
        let user = format!("estimate{i}");
        let mut m = h.miner(&user, "AVR", "estimate").await?;
        let job = m.job().await?;
        sleep(Duration::from_millis(5)).await;
        let answer = match rate {
            None => format!("{}\n", job.nonce),
            Some(r) => format!("{},{r},Verifier,estimate,chip,wallet\n", job.nonce),
        };
        m.send(answer.as_bytes()).await?;
        ensure_accepted(&m.line().await?)?;
        let o = observation(h, &user, 1, 0).await?;
        let calculated = o["hashrate_calc"]
            .as_f64()
            .context("finite estimated rate")?;
        anyhow::ensure!(calculated > 0.0, "positive estimate");
        near(
            o["hashrate"].as_f64().context("rate")?,
            calculated,
            "estimated rate",
        )?;
        m.close().await?;
        h.empty().await?;
    }
    Ok(())
}

/// A reported hashrate far below the measured one (a slow answer that claims
/// 0.001 H/s) is rejected as `BAD,Modified hashrate`. One far above it (5 kH/s,
/// at most ~300 H/s measured) is accepted, overreporting gains nothing.
async fn modified_hashrate(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "ESP8266", "modified").await?;
    let job = m.job().await?;
    sleep(Duration::from_millis(1700)).await;
    let reply = m
        .answer_fields(job.nonce.to_string(), "0.001", "chip-verifier")
        .await?;
    anyhow::ensure!(
        reply == "BAD,Modified hashrate",
        "Underreported hashrate not rejected"
    );
    let job = m.job().await?;
    sleep(Duration::from_millis(1700)).await;
    ensure_accepted(
        &m.answer_fields(job.nonce.to_string(), "5000", "chip-verifier")
            .await?,
    )?;
    m.share().await?;
    observation(h, "alice", 2, 1).await?;
    Ok(())
}

enum Metadata {
    /// Disallowed characters are stripped from software and rig names.
    Sanitized,
    /// Missing fields get placeholder values.
    Defaults,
    /// `rig:key` is split into the rig name and a base64-encoded key.
    LegacyKey,
}

/// How miner-supplied metadata in a share answer is stored in the stats.
async fn metadata(h: &Harness, variant: Metadata) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "metadata").await?;
    let job = m.job().await?;
    let payload = match variant {
        Metadata::Defaults => format!("{},1000,!!!,!!!\n", job.nonce),
        Metadata::LegacyKey => format!("{},1000,Verifier,rig:legacy,chip,wallet\n", job.nonce),
        Metadata::Sanitized => {
            format!("{},1000,Mi_n!eré.(x)-,ri_g!é.(x)-,chip,wallet\n", job.nonce)
        }
    };
    m.send(payload.as_bytes()).await?;
    ensure_accepted(&m.line().await?)?;
    observation(h, "alice", 1, 0).await?;
    let s = h.state().await?;
    let stats = s["miner_stats"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    match variant {
        Metadata::Defaults => anyhow::ensure!(
            stats["sft"] == "Unknown miner" && stats["id"] == "None" && stats["wd"] == "None",
            "defaults: {stats}"
        ),
        Metadata::LegacyKey => anyhow::ensure!(
            stats["id"] == "rig" && stats["pw"] == "bGVnYWN5", // base64("legacy")
            "legacy key: {stats}"
        ),
        Metadata::Sanitized => anyhow::ensure!(
            stats["sft"] == "Miner.(x)-" && stats["id"] == "rig.(x)-",
            "sanitization: {stats}"
        ),
    }
    Ok(())
}

/// The IoT field is sanitized, and kept up to 48 characters. At 49 characters
/// it is replaced with an error marker.
async fn iot_boundaries(h: &Harness) -> Result<()> {
    let cases = [
        ("T:21@a_!é".to_owned(), "T:21@a".to_owned()),
        ("x".repeat(48), "x".repeat(48)),
        ("x".repeat(49), "Error:data too long".to_owned()),
    ];
    for (i, (text, expected)) in cases.iter().enumerate() {
        let mut m = h.miner(&format!("iot{i}"), "AVR", "iot").await?;
        m.send(format!("JOB,iot{i},AVR,key,{text}\n").as_bytes())
            .await?;
        let job = m.read_job().await?;
        ensure_accepted(&m.answer(&job, true).await?)?;
        let s = h
            .wait_state("IoT stats", |s| stat(s, "iot").is_some())
            .await?;
        anyhow::ensure!(
            stat(&s, "iot").unwrap()["it"] == expected.as_str(),
            "IoT boundary {i}"
        );
        m.close().await?;
        h.empty().await?;
    }
    Ok(())
}

/// Passwords and legacy `rig:key` keys are accepted up to 96 bytes and stored
/// base64-encoded. At 97 bytes they are rejected with `BAD,Password too long`.
async fn password_boundaries(h: &Harness) -> Result<()> {
    // Password in the job request.
    for size in [96, 97] {
        let mut m = h.miner("alice", "AVR", "password").await?;
        m.send(format!("JOB,alice,AVR,{}\n", "a".repeat(size)).as_bytes())
            .await?;
        if size == 96 {
            let job = m.read_job().await?;
            ensure_accepted(&m.answer(&job, true).await?)?;
            let s = h
                .wait_state("password encoded", |s| stat(s, "password").is_some())
                .await?;
            anyhow::ensure!(
                stat(&s, "password").unwrap()["pw"] == "YWFh".repeat(32), // base64("aaa") x 32
                "base64 boundary"
            );
            m.close().await?;
        } else {
            // Over-long passwords get an explicit rejection before the disconnect.
            let line = m.line().await?;
            anyhow::ensure!(
                line == "BAD,Password too long",
                "password rejection: {line:?}"
            );
            m.expect_closed(WAIT).await?;
        }
        h.empty().await?;
    }

    // The legacy `rig:key` key has the same limit. It is only checked when
    // stats are published, which happens after the share feedback was sent.
    for size in [96, 97] {
        let rig = format!("rig:{}", "b".repeat(size));
        let mut m = h.miner("alice", "AVR", &rig).await?;
        let job = m.job().await?;
        ensure_accepted(&m.answer(&job, true).await?)?;
        if size == 96 {
            // `stat` finds the entry by its published rig id, the part before ':'.
            let s = h
                .wait_state("legacy key encoded", |s| stat(s, "rig").is_some())
                .await?;
            anyhow::ensure!(
                stat(&s, "rig").unwrap()["pw"] == "YmJi".repeat(32), // base64("bbb") x 32
                "legacy key base64 boundary"
            );
            m.close().await?;
        } else {
            let line = m.line().await?;
            anyhow::ensure!(
                line == "BAD,Password too long",
                "legacy key rejection: {line:?}"
            );
            m.expect_closed(WAIT).await?;
        }
        h.empty().await?;
    }
    Ok(())
}

/// The first job builds on the configured initial hash. A found block records
/// finder, hash, algorithm, amount and a current timestamp, and the next job
/// builds on the found hash.
async fn hash_chain_block_details(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "chain").await?;
    let job = m.job().await?;
    anyhow::ensure!(
        job.previous == "05b7406b3909071b2d85b2d2958ad07a0d52bd12",
        "Initial block hash"
    );
    anyhow::ensure!(m.answer(&job, true).await? == "BLOCK", "forced block");
    observation(h, "alice", 1, 0).await?;
    let s = h.state().await?;
    let block = &s["verify"]["block_info"][0];
    anyhow::ensure!(
        block["finder"] == "alice"
            && block["hash"] == job.hash
            && block["algorithm"] == "DUCO-S1"
            && block["amount"] == 2.0,
        "block details: {block}"
    );
    // Despite its name, the field holds epoch seconds.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let when = block["time_since_epoch_millis"]
        .as_u64()
        .context("block time")?;
    anyhow::ensure!(
        now.abs_diff(when) <= 5,
        "block timestamp differs from current epoch seconds"
    );
    let next = m.job().await?;
    anyhow::ensure!(next.previous == job.hash, "hash chain did not advance");
    ensure_accepted(&m.answer(&next, true).await?)?;
    Ok(())
}

// ---- kolka ---------------------------------------------------------------------

/// The reverse of `suites::mixed_reuse`: a PC moves down into a slot released
/// by an ESP, and IDs restart cleanly afterwards.
async fn reverse_mixed_reuse(h: &Harness) -> Result<()> {
    let mut esp = h.miner("alice", "ESP32", "esp").await?;
    let mut pc = h.miner("alice", "LOW", "pc").await?;
    esp.share().await?;
    pc.share().await?;
    esp.close().await?;
    h.wait_state("ESP released", |s| s["connections"] == 1)
        .await?;
    pc.share().await?;
    h.wait_state("PC takes ESP slot", |s| {
        stat(s, "pc").is_some_and(|v| v["c"] == 0.5)
            && s["workers"]["alice"]["active_half_steps"] == 2
    })
    .await?;
    pc.close().await?;
    h.empty().await?;
    let mut fresh = h.miner("alice", "LOW", "new").await?;
    fresh.share().await?;
    h.wait_state("fresh PC ID", |s| {
        stat(s, "new").is_some_and(|v| v["c"] == 1.0)
    })
    .await?;
    Ok(())
}

/// 32 overlapping ESP/PC miners connect, share and disconnect (mixing resets
/// and closes) concurrently. All state must be cleaned up afterwards.
async fn overlapping_churn(h: &Harness) -> Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..32 {
        let tier = if i % 2 == 0 { "ESP32" } else { "AVR" };
        let mut m = h.miner("alice", tier, &format!("churn{i}")).await?;
        tasks.spawn(async move {
            m.share().await?;
            if i % 3 == 0 {
                m.reset()?;
            } else {
                m.close().await?;
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    h.empty().await
}

/// A miner rejected for the worker limit can mine on the same connection once
/// another worker leaves.
async fn worker_limit_recovery(h: &Harness) -> Result<()> {
    let mut first = h.miner("alice", "AVR", "first").await?;
    first.share().await?;
    let mut second = h.miner("alice", "AVR", "second").await?;
    let job = second.job().await?;
    anyhow::ensure!(
        second.answer(&job, true).await? == "BAD,Too many workers",
        "limit not applied"
    );
    first.close().await?;
    h.wait_state("one worker", |s| s["connections"] == 1)
        .await?;
    second.share().await?;
    observation(h, "alice", 1, 1).await?;
    Ok(())
}

// ---- lifecycle -----------------------------------------------------------------

/// Real adaptive difficulty over 55 shares on a LOW tier:
/// * the first five jobs keep the tier difficulty (11);
/// * afterwards the adaptive floor of 5000 applies;
/// * the nonce and share time of each accepted share enter exponential
///   averages (the first share seeds them, later ones weigh 0.2);
/// * the calculated hashrate is the ratio of the two averages.
async fn adaptive_mining_average(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "LOW", "adaptive").await?;
    // Independent model of the averages.
    let (mut expected_hashes, mut expected_time) = (0.0, 0.0);
    for i in 1..=55 {
        let job = m.job().await?;
        if i <= 5 {
            anyhow::ensure!(job.difficulty == 11, "Adjusted before four accepted shares");
        } else {
            anyhow::ensure!(job.difficulty >= 5000, "Adaptive floor not applied");
        }
        ensure_accepted(&m.answer(&job, true).await?)?;
        let o = observation(h, "alice", i, 0).await?;
        let weight = if i == 1 { 1.0 } else { 0.2 };
        expected_hashes +=
            (o["nonce"].as_f64().context("observed nonce")? - expected_hashes) * weight;
        expected_time +=
            (o["share_time"].as_f64().context("observed share time")? - expected_time) * weight;
        near(
            o["avg_hashes"].as_f64().context("hash average")?,
            expected_hashes,
            "hash average",
        )?;
        near(
            o["avg_share_time"].as_f64().context("share-time average")?,
            expected_time,
            "share-time average",
        )?;
        near(
            o["hashrate_calc"].as_f64().context("calculated hashrate")?,
            expected_hashes / expected_time,
            "averaged hashrate",
        )?;
    }
    Ok(())
}

/// Each tier's first job uses that tier's fixture difficulty (1..=14, in order).
async fn all_tier_initial_jobs(h: &Harness) -> Result<()> {
    let tiers = [
        "AVR",
        "MEGA",
        "ARM",
        "DUE",
        "ESP8266",
        "ESP8266H",
        "ESP8266N",
        "ESP8266NH",
        "ESP32",
        "ESP32S",
        "LOW",
        "MEDIUM",
        "NET",
        "EXTREME",
    ];
    for (i, tier) in tiers.iter().enumerate() {
        let mut m = h.miner("alice", tier, "tier").await?;
        let job = m.job().await?;
        anyhow::ensure!(job.difficulty == i as u64 + 1, "initial {tier}");
        ensure_accepted(&m.answer(&job, true).await?)?;
        m.close().await?;
        h.empty().await?;
    }
    Ok(())
}

/// A 20-character key disables `/data` entirely, while a 21-character key
/// works. A wrong key of the same length is always rejected.
async fn dashboard_key(h: &Harness, enabled: bool) -> Result<()> {
    let correct = h
        .client
        .get(format!("{}/data", h.dashboard))
        .query(&[("key", h.key.as_str())])
        .send()
        .await?;
    let expected = if enabled { 200 } else { 404 };
    anyhow::ensure!(correct.status() == expected, "key length boundary");
    let wrong = "y".repeat(h.key.len());
    let response = h
        .client
        .get(format!("{}/data", h.dashboard))
        .query(&[("key", wrong)])
        .send()
        .await?;
    anyhow::ensure!(response.status() == 404, "same-length invalid key accepted");
    Ok(())
}

/// Published miner stats have the expected field types and values.
async fn dashboard_schema(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "schema").await?;
    m.share().await?;
    let s = h
        .wait_state("schema published", |s| stat(s, "schema").is_some())
        .await?;
    let v = stat(&s, "schema").unwrap();
    for key in ["u", "sft", "id", "wd", "k", "pw", "it", "ip"] {
        anyhow::ensure!(v[key].is_string(), "{key} string: {v}");
    }
    for key in ["h", "s", "a", "r", "d", "ls", "pg", "rw", "c"] {
        anyhow::ensure!(
            v[key].as_f64().is_some_and(f64::is_finite),
            "{key} finite number: {v}"
        );
    }
    anyhow::ensure!(
        v["h"] == 1000.0
            && v["a"] == 1
            && v["r"] == 0
            && v["d"] == 1
            && v["c"] == 1.0
            && v["ip"] == "127.0.0.1",
        "schema values: {v}"
    );
    Ok(())
}

/// With `guessPort`, the pool picks a port in 1000..9999, serves miners on it,
/// and reports it in the master login.
async fn guessed_port(h: &Harness) -> Result<()> {
    anyhow::ensure!((1000..9999).contains(&h.port), "Guessed port range");
    h.miner("alice", "AVR", "port").await?.share().await?;
    let login = master_login(h)?;
    anyhow::ensure!(login["port"] == h.port, "Guessed port differs from login");
    Ok(())
}

enum Ban {
    /// `config/bans.json` already bans 127.0.0.2, so any user from it is refused.
    PreexistingIp,
    /// After a username ban, the saved IP ban also refuses a different user.
    Reconnect,
    /// A banned username from 127.0.0.1 is still accepted.
    LocalhostExempt,
    /// A ban added by a reward/ban refresh is persisted and enforced.
    Refreshed,
}

/// Ban enforcement and persistence.
async fn ban(h: &mut Harness, variant: Ban) -> Result<()> {
    let user = match variant {
        Ban::Refreshed => "fresh-ban",
        Ban::PreexistingIp => "alice",
        Ban::Reconnect | Ban::LocalhostExempt => "banned-verifier-user",
    };
    if let Ban::Refreshed = variant {
        h.miner("before", "AVR", "before").await?.share().await?;
        *h.mock.shared.bans.lock().unwrap() = json!(["fresh-ban"]);
        h.refresh().await?;
        let persisted: Value =
            serde_json::from_slice(&std::fs::read(h.dir.join("config/bans.json"))?)?;
        anyhow::ensure!(
            persisted["bannedUsernames"]
                .as_array()
                .unwrap()
                .contains(&json!("fresh-ban")),
            "Refreshed ban was not persisted"
        );
    }
    if let Ban::LocalhostExempt = variant {
        h.miner(user, "AVR", "local").await?.share().await?;
        return Ok(());
    }

    let mut m = h.miner_from(user, "AVR", "ban", Some("127.0.0.2")).await?;
    m.send(format!("JOB,{user},AVR\n").as_bytes()).await?;
    anyhow::ensure!(
        m.line().await? == "BAD,User banned",
        "Banned identity accepted"
    );
    m.expect_closed(WAIT).await?;
    if let Ban::Reconnect = variant {
        let mut m = h
            .miner_from("different-user", "AVR", "again", Some("127.0.0.2"))
            .await?;
        m.send(b"JOB,different-user,AVR\n").await?;
        anyhow::ensure!(
            m.line().await? == "BAD,User banned",
            "Saved IP ban not enforced"
        );
        m.expect_closed(WAIT).await?;
    }
    Ok(())
}

/// A refresh applies new tier difficulties to new miners and updates the
/// DUE / ESP8266NH difficulty caches.
async fn refreshed_rewards(h: &mut Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "old").await?;
    m.share().await?;
    {
        let mut rewards = h.mock.shared.rewards.lock().unwrap();
        rewards["AVR"]["difficulty"] = json!(3);
        rewards["DUE"]["difficulty"] = json!(7);
        rewards["ESP8266NH"]["difficulty"] = json!(12);
    }
    h.refresh().await?;
    let mut new = h.miner("bob", "AVR", "new").await?;
    let job = new.job().await?;
    anyhow::ensure!(job.difficulty == 3, "Refreshed reward difficulty not used");
    ensure_accepted(&new.answer(&job, true).await?)?;
    let s = h.state().await?;
    anyhow::ensure!(
        s["verify"]["due_cache"] == 7 && s["verify"]["esp_cache"] == 12,
        "Difficulty caches not refreshed"
    );
    Ok(())
}

/// The real 61-second rate window expires, and shares are accepted again (slow).
async fn rate_window_reset(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "window").await?;
    for _ in 0..3 {
        m.share().await?;
    }
    let job = m.job().await?;
    anyhow::ensure!(
        m.answer(&job, true).await? == "BAD,Too low difficulty",
        "Initial limit"
    );
    sleep(Duration::from_secs(61)).await;
    let job = m.job().await?;
    let reply = m.answer(&job, true).await?;
    anyhow::ensure!(
        is_accepted(&reply),
        "First share after expired rate window was rejected: {reply}"
    );
    m.share().await?;
    Ok(())
}

// ---- sync ----------------------------------------------------------------------

/// A dropped or timed-out sync connection keeps all pending state. The next
/// successful attempt uploads it and clears it.
async fn transport_retry(h: &Harness, fault: Fault) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "network").await?;
    m.share().await?;
    observation(h, "alice", 1, 0).await?;
    let before = h.state().await?["verify"].clone();
    let index = h.mock.uploads().len();
    h.mock.fault("/pool_sync/", fault);
    h.wait_log("Error while syncing:").await?;
    let after = h.state().await?;
    for key in ["shares_total", "shares_pending", "balances", "blocks"] {
        anyhow::ensure!(
            before[key] == after["verify"][key],
            "Transport failure lost {key}"
        );
    }
    h.mock.status(200);
    let (_, u) = h
        .upload_after(index, |u| u.completed && u.status == 200)
        .await?;
    anyhow::ensure!(
        u.query["blockIncrease"] == "1" && u.parts["rewards"].json["alice"] == 2.0,
        "retry lost work: {u:?}"
    );
    h.wait_state("retry acknowledged", |s| {
        s["verify"]["shares_pending"] == 0
            && s["verify"]["balances"].as_object().unwrap().is_empty()
    })
    .await?;
    Ok(())
}

/// When the IP lookup times out, sync falls back to the last known IP.
async fn ip_fallback(h: &Harness) -> Result<()> {
    let index = h.mock.uploads().len();
    h.mock.fault("/ip", Fault::Delay(4000));
    h.wait_log("Falling back to last ip collected").await?;
    let (_, u) = h.upload_after(index, |u| u.completed).await?;
    anyhow::ensure!(u.query["host"] == "127.0.0.1", "Cached IP lost");
    Ok(())
}

/// When the IP lookup fails before any IP is cached, no upload carries an
/// invalid host (faults are queued in [`configure_mock`]).
async fn ip_no_cache(h: &Harness) -> Result<()> {
    h.wait_log("Failed to get pool ip").await?;
    anyhow::ensure!(
        !h.mock.shared.fault_log.lock().unwrap().is_empty(),
        "No IP fault injected"
    );
    let uploads = h.mock.uploads();
    anyhow::ensure!(
        uploads.iter().all(|u| u.query["host"] == "127.0.0.1"),
        "Invalid IP uploaded"
    );
    Ok(())
}

/// If a staging file cannot be written (a directory is in the way), sync
/// fails without losing pending work. After the obstruction is removed, a
/// retry succeeds.
async fn staging_write_failure(h: &Harness, file: &str) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "disk").await?;
    m.share().await?;
    observation(h, "alice", 1, 0).await?;
    let path = h.dir.join("sync").join(file);
    if path.is_file() {
        std::fs::remove_file(&path)?;
    }
    std::fs::create_dir(&path)?;
    // Observe a real failed sync. The obstruction is removed before the result
    // is checked, so cleanup happens even if the assertion fails.
    let result = async {
        h.wait_log("Sync failed:").await?;
        let s = h.state().await?;
        anyhow::ensure!(
            s["verify"]["shares_pending"] == 1,
            "Staging failure lost pending share"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    std::fs::remove_dir(&path)?;
    result?;
    let index = h.mock.uploads().len();
    h.mock.status(200);
    h.upload_after(index, |u| u.completed && u.status == 200)
        .await?;
    h.wait_state("disk retry clears", |s| s["verify"]["shares_pending"] == 0)
        .await?;
    Ok(())
}

/// A shutdown during a held upload aborts it and still sends a final snapshot
/// with all pending work, including the share accepted during the upload.
async fn shutdown_held_upload(h: &mut Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "shutdown-held").await?;
    m.share().await?;
    observation(h, "alice", 1, 0).await?;
    h.mock.hold(true);
    let start = h.mock.uploads().len();
    let (index, _) = h.upload_after(start, |u| !u.completed).await?;
    m.share().await?;
    observation(h, "alice", 2, 0).await?;
    h.mock.status(200);
    h.request_shutdown().await?;
    // The aborted request must release the sync lock so a final snapshot is attempted.
    let (last, _) = h.upload_after(index + 1, |u| !u.completed).await?;
    h.mock.hold(false);
    h.mock.shared.gate.add_permits(100);
    h.wait_stopped().await?;
    m.expect_closed(WAIT).await?;
    let uploads = h.mock.uploads();
    let final_upload = &uploads[last];
    anyhow::ensure!(
        final_upload.query["blockIncrease"] == "2"
            && final_upload.parts["rewards"].json["alice"] == 4.0,
        "Final upload lost pending work: {final_upload:?}"
    );
    Ok(())
}

// ---- connections ---------------------------------------------------------------

/// An unanswered job hits the real 90-second deadline and is disconnected (slow).
async fn active_job_timeout(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "job-timeout").await?;
    m.job().await?;
    let start = tokio::time::Instant::now();
    m.expect_closed(Duration::from_secs(100)).await?;
    anyhow::ensure!(
        start.elapsed() >= Duration::from_secs(85),
        "Job timeout early"
    );
    h.empty().await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every registered scenario is unique and appears in `coverage.json`, and
    /// every inventory entry names a registered scenario.
    #[test]
    fn registry_and_inventory_are_consistent() {
        let mut ids = std::collections::BTreeSet::new();
        for case in crate::suites::CASES.iter().chain(CASES) {
            assert!(ids.insert(case.id()), "duplicate scenario");
        }
        let inventory: Value = serde_json::from_str(include_str!("../coverage.json")).unwrap();
        let mut mapped = std::collections::BTreeSet::new();
        for behavior in inventory["behaviors"].as_array().unwrap() {
            assert!(["asserted", "partial", "unverified", "excluded"]
                .contains(&behavior["coverage"].as_str().unwrap()));
            for id in behavior["cases"].as_array().unwrap() {
                let id = id.as_str().unwrap();
                assert!(ids.contains(id), "Unknown inventory case {id}");
                mapped.insert(id.to_owned());
            }
        }
        assert_eq!(
            ids, mapped,
            "Every executable scenario needs an inventory entry"
        );
    }

    #[tokio::test]
    async fn scripted_mock_and_transport_faults_are_observable() {
        let mock = Mock::start(false, false).await.unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let get_ip = || client.get(format!("{}/ip", mock.http)).send();

        // A scripted response is used once, then the default returns.
        mock.respond("/ip", 503, "unavailable");
        let response = get_ip().await.unwrap();
        assert_eq!(response.status(), 503);
        assert_eq!(response.text().await.unwrap(), "unavailable");
        assert_eq!(get_ip().await.unwrap().text().await.unwrap(), "127.0.0.1");

        // A transport fault drops the connection and is logged.
        mock.fault("/ip", Fault::Disconnect);
        assert!(get_ip().await.is_err());
        assert_eq!(mock.shared.fault_log.lock().unwrap().len(), 1);
    }
}
