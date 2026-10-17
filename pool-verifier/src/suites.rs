//! Scenario registry and the core black-box scenarios.
//!
//! Cases are registered as data ([`CASES`] here, plus `expanded::CASES`) and
//! dispatched by name in [`run`]. Each scenario is an arrange/act/assert flow
//! against a fresh [`Harness`]. Assertions use the real miner protocol, the
//! captured sync uploads, and the injected read-only dashboard observations.
//! They never replace the production path being checked.

use crate::harness::{is_accepted, stat, Harness, Options, WAIT};
use anyhow::{Context, Result};
use serde_json::Value;
use std::time::Duration;
use tokio::time::{sleep, Instant};

/// One registered scenario.
pub struct Case {
    pub suite: &'static str,
    pub name: &'static str,
    /// Waits on real production timeouts; only runs with `--slow`.
    pub slow: bool,
}

impl Case {
    pub const fn new(suite: &'static str, name: &'static str) -> Self {
        Self {
            suite,
            name,
            slow: false,
        }
    }

    pub const fn slow(suite: &'static str, name: &'static str) -> Self {
        Self {
            suite,
            name,
            slow: true,
        }
    }

    /// `suite/name`, as used by `--case`, reports, and `coverage.json`.
    pub fn id(&self) -> String {
        format!("{}/{}", self.suite, self.name)
    }

    /// Internal cases run one injected unit test instead of a pool process.
    pub fn is_internal(&self) -> bool {
        self.suite == "internal"
    }
}

pub const CASES: &[Case] = &[
    Case::new("connections", "greeting-motd-idle"),
    Case::new("connections", "disconnect-before-job"),
    Case::new("connections", "disconnect-before-share"),
    Case::new("connections", "active-disconnect"),
    Case::new("connections", "abrupt-reset"),
    Case::new("connections", "reconnect-cycles"),
    Case::new("connections", "concurrent-churn"),
    Case::new("protocol", "good-bad-recovery"),
    Case::new("protocol", "malformed-answer"),
    Case::new("protocol", "missing-username"),
    Case::new("protocol", "framing"),
    Case::new("protocol", "coalesced-frames"),
    Case::new("protocol", "oversized-frame"),
    Case::new("protocol", "invalid-utf8"),
    Case::new("protocol", "forced-block"),
    Case::new("counters", "accepted-shares"),
    Case::new("counters", "rejected-share-accounting"),
    Case::new("counters", "reward-warmup"),
    Case::new("counters", "worker-limit"),
    Case::new("counters", "weighted-worker-limit"),
    Case::new("kolka", "pc-compaction"),
    Case::new("kolka", "esp-compaction"),
    Case::new("kolka", "mixed-reuse"),
    Case::new("kolka", "user-isolation"),
    Case::new("sync", "payload-and-clearing"),
    Case::new("sync", "failed-upload-retry"),
    Case::new("sync", "disconnected-worker"),
    Case::new("sync", "inflight-shares"),
    Case::new("sync", "inflight-rewards"),
    Case::new("sync", "inflight-failed-retry"),
    Case::new("sync", "cleanup-missing-rewards"),
    Case::new("sync", "cleanup-missing-workers"),
    Case::new("lifecycle", "startup-and-auth"),
    Case::new("lifecycle", "share-rate-limit"),
    Case::new("lifecycle", "banned-user"),
    Case::new("lifecycle", "shutdown-final-sync"),
    Case::slow("connections", "idle-timeout"),
];

/// Fixture settings for a case.
///
/// All fixture differences live here and in `expanded::options`, so scenario
/// bodies stay focused on actions and assertions, and special production
/// settings are easy to audit.
pub fn options(case: &Case) -> Options {
    let mut o = Options::default();
    if case.suite == "sync" {
        o.sync_secs = 1;
    }
    if matches!(
        case.name,
        "forced-block"
            | "payload-and-clearing"
            | "inflight-rewards"
            | "inflight-failed-retry"
            | "cleanup-missing-rewards"
            | "cleanup-missing-workers"
            | "shutdown-final-sync"
    ) {
        o.blocks = true;
    }
    if matches!(case.name, "worker-limit" | "weighted-worker-limit") {
        o.max_workers = 1;
    }
    if case.name == "share-rate-limit" {
        o.rate = 2;
    }
    crate::expanded::options(case, &mut o);
    o
}

/// Runs the scenario named by `case`. Unknown names go to `expanded::run`.
pub async fn run(case: &Case, h: &mut Harness) -> Result<()> {
    match case.name {
        // connections
        "greeting-motd-idle" => greeting_motd_idle(h).await,
        "disconnect-before-job" => disconnect_before_job(h).await,
        "disconnect-before-share" => disconnect_before_share(h).await,
        "active-disconnect" => active_disconnect(h, false).await,
        "abrupt-reset" => active_disconnect(h, true).await,
        "reconnect-cycles" => reconnect_cycles(h).await,
        "concurrent-churn" => concurrent_churn(h).await,
        "idle-timeout" => idle_timeout(h).await,
        // protocol
        "good-bad-recovery" => good_bad_recovery(h).await,
        "malformed-answer" => malformed_answer(h).await,
        "missing-username" => missing_username(h).await,
        "framing" => framing(h).await,
        "coalesced-frames" => coalesced_frames(h).await,
        "oversized-frame" => invalid_frame(h, false).await,
        "invalid-utf8" => invalid_frame(h, true).await,
        "forced-block" => forced_block(h).await,
        // counters
        "accepted-shares" => accepted_shares(h).await,
        "rejected-share-accounting" => rejected_share_accounting(h).await,
        "reward-warmup" => reward_warmup(h).await,
        "worker-limit" => worker_limit(h, "AVR").await,
        "weighted-worker-limit" => worker_limit(h, "ESP32").await,
        // kolka
        "pc-compaction" => compaction(h, false).await,
        "esp-compaction" => compaction(h, true).await,
        "mixed-reuse" => mixed_reuse(h).await,
        "user-isolation" => user_isolation(h).await,
        // sync
        "payload-and-clearing" | "failed-upload-retry" => sync_roundtrip(h).await,
        "disconnected-worker" => disconnected_worker(h).await,
        "inflight-shares" | "inflight-rewards" => inflight(case, h).await,
        "inflight-failed-retry" | "cleanup-missing-rewards" | "cleanup-missing-workers" => {
            inflight_completion(case, h).await
        }
        // lifecycle
        "startup-and-auth" => startup_and_auth(h).await,
        "share-rate-limit" => share_rate_limit(h).await,
        "banned-user" => banned_user(h).await,
        "shutdown-final-sync" => shutdown_final_sync(h).await,
        _ => crate::expanded::run(case, h).await,
    }
}

// ---- Shared assertions --------------------------------------------------------

fn assert_eq_json(actual: &Value, expected: impl Into<Value>, label: &str) -> Result<()> {
    let expected = expected.into();
    anyhow::ensure!(
        *actual == expected,
        "{label}: expected {expected}, actual {actual}"
    );
    Ok(())
}

/// Float equality with a 1e-5 relative tolerance (absolute below 1.0).
fn close_float(actual: f64, expected: f64, label: &str) -> Result<()> {
    anyhow::ensure!(
        actual.is_finite() && (actual - expected).abs() <= 0.00001 * expected.abs().max(1.0),
        "{label}: expected {expected}, actual {actual}"
    );
    Ok(())
}

/// Waits until the dashboard shows `rig` with exactly these share counters.
async fn published(h: &Harness, rig: &str, accepted: u64, rejected: u64) -> Result<Value> {
    h.wait_state("miner counters published", |s| {
        stat(s, rig).is_some_and(|m| m["a"] == accepted && m["r"] == rejected)
    })
    .await
}

/// Waits until each `(rig, kolka_id)` pair is published and `user`'s total
/// worker weight equals `half_steps` (an ESP counts 1, a PC counts 2). Then it
/// proves that all of `user`'s workers have unique Kolka IDs.
async fn check_ids(
    h: &Harness,
    expected: &[(&str, f64)],
    user: &str,
    half_steps: u64,
) -> Result<()> {
    let s = h
        .wait_state("Kolka IDs and worker weights", |s| {
            expected
                .iter()
                .all(|(rig, id)| stat(s, rig).and_then(|m| m["c"].as_f64()) == Some(*id))
                && s["workers"][user]["active_half_steps"] == half_steps
        })
        .await?;
    let ids: Vec<_> = s["miner_stats"]
        .as_object()
        .context("stats object")?
        .values()
        .filter(|m| m["u"] == user)
        .map(|m| m["c"].as_f64().unwrap())
        .collect();
    for (i, id) in ids.iter().enumerate() {
        anyhow::ensure!(!ids[..i].contains(id), "Duplicate Kolka ID {id}");
    }
    Ok(())
}

// ---- connections ---------------------------------------------------------------

/// An idle connection counts as a connection but not as a worker. MOTD works,
/// and a later share and close clean up fully.
async fn greeting_motd_idle(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "idle").await?;
    h.wait_state("idle connection counted without a worker", |s| {
        s["connections"] == 1 && s["workers"].as_object().unwrap().is_empty()
    })
    .await?;
    m.send(b"MOTD\n").await?;
    anyhow::ensure!(m.line().await? == "verifier-motd", "Incorrect MOTD");
    m.share().await?;
    m.close().await?;
    h.empty().await
}

/// Closing right after the greeting leaves no state behind.
async fn disconnect_before_job(h: &Harness) -> Result<()> {
    h.miner("alice", "AVR", "early").await?.close().await?;
    h.empty().await
}

/// A job request allocates a worker slot before any share is published.
/// Closing then releases it.
async fn disconnect_before_share(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "job-only").await?;
    m.job().await?;
    h.wait_state("allocated before first share", |s| {
        s["workers"]["alice"]["active_half_steps"] == 2
            && s["miner_stats"].as_object().unwrap().is_empty()
    })
    .await?;
    m.close().await?;
    h.empty().await
}

/// A worker with published stats is fully removed after a graceful close
/// (`active-disconnect`) or a TCP reset (`abrupt-reset`).
async fn active_disconnect(h: &Harness, reset: bool) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "active").await?;
    m.share().await?;
    published(h, "active", 1, 0).await?;
    if reset {
        m.reset()?;
    } else {
        m.close().await?;
    }
    h.empty().await
}

/// Eight connect/share/close cycles always get Kolka ID 1 and always clean up.
async fn reconnect_cycles(h: &Harness) -> Result<()> {
    for _ in 0..8 {
        let mut m = h.miner("alice", "AVR", "reconnect").await?;
        m.share().await?;
        check_ids(h, &[("reconnect", 1.0)], "alice", 2).await?;
        m.close().await?;
        h.empty().await?;
    }
    Ok(())
}

/// Three rounds of eight parallel miners. The IDs must be exactly 1..=8, and a
/// mix of resets and closes must clean up completely.
async fn concurrent_churn(h: &Harness) -> Result<()> {
    for _ in 0..3 {
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..8 {
            let mut m = h.miner("alice", "AVR", &format!("parallel{i}")).await?;
            tasks.spawn(async move {
                m.share().await?;
                Ok::<_, anyhow::Error>(m)
            });
        }
        let mut miners = vec![];
        while let Some(result) = tasks.join_next().await {
            miners.push(result??);
        }

        let s = h
            .wait_state("eight active connections and workers", |s| {
                s["connections"] == 8
                    && s["workers"]["alice"]["active_half_steps"] == 16
                    && s["miner_stats"].as_object().unwrap().len() == 8
            })
            .await?;
        let mut ids: Vec<f64> = s["miner_stats"]
            .as_object()
            .unwrap()
            .values()
            .map(|m| m["c"].as_f64().unwrap())
            .collect();
        ids.sort_by(f64::total_cmp);
        anyhow::ensure!(
            ids == (1..=8).map(f64::from).collect::<Vec<_>>(),
            "Concurrent IDs not contiguous: {ids:?}"
        );

        // Disconnect concurrently: even miners reset, odd miners close.
        let mut close_tasks = tokio::task::JoinSet::new();
        for (i, m) in miners.into_iter().enumerate() {
            close_tasks.spawn(async move {
                if i % 2 == 0 {
                    m.reset()?;
                } else {
                    m.close().await?;
                }
                Ok::<_, anyhow::Error>(())
            });
        }
        while let Some(result) = close_tasks.join_next().await {
            result??;
        }
        h.empty().await?;
    }
    Ok(())
}

/// The real 90-second read deadline closes a silent connection (slow).
async fn idle_timeout(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "timeout").await?;
    let start = Instant::now();
    m.expect_closed(Duration::from_secs(100)).await?;
    anyhow::ensure!(
        start.elapsed() >= Duration::from_secs(85),
        "Idle connection closed too early"
    );
    h.empty().await
}

// ---- protocol ------------------------------------------------------------------

/// A wrong nonce gets `BAD,Incorrect result`, and the connection keeps working.
async fn good_bad_recovery(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "feedback").await?;
    // Shares may come back as BLOCK; wait for a plain GOOD.
    let mut saw_good = false;
    for _ in 0..4 {
        if m.share().await? == "GOOD" {
            saw_good = true;
            break;
        }
    }
    anyhow::ensure!(saw_good, "Could not observe GOOD within four valid shares");
    let job = m.job().await?;
    anyhow::ensure!(
        m.answer(&job, false).await? == "BAD,Incorrect result",
        "Incorrect nonce feedback"
    );
    m.share().await?;
    Ok(())
}

/// A non-numeric nonce is answered with `BAD,No response to job` and a disconnect.
async fn malformed_answer(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "malformed").await?;
    m.job().await?;
    m.send(b"not-a-nonce,1000,Verifier,malformed,chip,wallet\n")
        .await?;
    anyhow::ensure!(
        m.line().await? == "BAD,No response to job",
        "Incorrect malformed-answer feedback"
    );
    m.expect_closed(WAIT).await?;
    h.empty().await
}

/// A bare `JOB` without a username is rejected and disconnected.
async fn missing_username(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "missing").await?;
    m.send(b"JOB\n").await?;
    anyhow::ensure!(
        m.line().await? == "BAD,No username specified",
        "Missing username response"
    );
    m.expect_closed(WAIT).await?;
    h.empty().await
}

/// LF, CRLF and unterminated frames are all accepted.
async fn framing(h: &Harness) -> Result<()> {
    for (i, ending) in ["\n", "\r\n", ""].iter().enumerate() {
        let mut m = h.miner("alice", "AVR", &format!("frame{i}")).await?;
        let job = m.job_framed(ending).await?;
        let answer = format!("{},1000,Verifier,frame{i},chip,wallet{ending}", job.nonce);
        m.send(answer.as_bytes()).await?;
        let reply = m.line().await?;
        anyhow::ensure!(is_accepted(&reply), "Framing {ending:?}: {reply}");
        m.close().await?;
        h.empty().await?;
    }
    Ok(())
}

/// A job request and an answer in one TCP write are both processed.
async fn coalesced_frames(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "coalesced").await?;
    m.send(b"JOB,alice,AVR\n0,1000,Verifier,coalesced,chip,wallet\n")
        .await?;
    m.read_job().await?;
    anyhow::ensure!(
        m.line().await? == "BAD,Incorrect result",
        "Coalesced second frame lost"
    );
    m.share().await?;
    Ok(())
}

/// A frame over 1024 bytes, or invalid UTF-8, closes the connection.
async fn invalid_frame(h: &Harness, invalid_utf8: bool) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "invalid").await?;
    let bytes = if invalid_utf8 {
        vec![0xff, b'\n']
    } else {
        let mut b = vec![b'x'; 1025];
        b.push(b'\n');
        b
    };
    m.send(&bytes).await?;
    m.expect_closed(WAIT).await?;
    h.empty().await
}

/// With `block_chance = 1` a share is a BLOCK and credits the block reward.
async fn forced_block(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "block").await?;
    anyhow::ensure!(
        m.share().await? == "BLOCK",
        "block_chance=1 did not return BLOCK"
    );
    let s = published(h, "block", 1, 0).await?;
    close_float(
        s["verify"]["balances"]["alice"].as_f64().unwrap(),
        2.0,
        "block reward",
    )
}

// ---- counters ------------------------------------------------------------------

/// Accepted shares increment both the lifetime total and the pending counter.
async fn accepted_shares(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "counts").await?;
    for _ in 0..3 {
        m.share().await?;
    }
    let s = published(h, "counts", 3, 0).await?;
    assert_eq_json(&s["verify"]["shares_total"], 3, "total accepted shares")?;
    assert_eq_json(&s["verify"]["shares_pending"], 3, "pending accepted shares")
}

/// A rejected share is counted as rejected but changes no totals or balances.
async fn rejected_share_accounting(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "counts").await?;
    m.share().await?;
    let before = published(h, "counts", 1, 0).await?;
    let job = m.job().await?;
    anyhow::ensure!(
        m.answer(&job, false).await? == "BAD,Incorrect result",
        "Wrong rejection"
    );
    let s = published(h, "counts", 1, 1).await?;
    assert_eq_json(
        &s["verify"]["shares_total"],
        before["verify"]["shares_total"].clone(),
        "rejected share must not increase total",
    )?;
    assert_eq_json(
        &s["verify"]["shares_pending"],
        before["verify"]["shares_pending"].clone(),
        "rejected share must not increase pending",
    )?;
    anyhow::ensure!(
        s["verify"]["balances"] == before["verify"]["balances"],
        "Rejected share changed balances"
    );
    Ok(())
}

/// The first ten shares earn no regular reward (block rewards still count).
/// The eleventh earns a positive reward that accumulates in the balance.
async fn reward_warmup(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "reward").await?;
    let mut block_rewards = 0.0;
    for _ in 0..10 {
        if m.share().await? == "BLOCK" {
            block_rewards += 2.0;
        }
    }
    let s = published(h, "reward", 10, 0).await?;
    close_float(
        s["verify"]["balances"]["alice"].as_f64().unwrap(),
        block_rewards,
        "no regular reward during first ten shares",
    )?;

    let response = m.share().await?;
    let s = published(h, "reward", 11, 0).await?;
    let reward = stat(&s, "reward").unwrap()["rw"].as_f64().unwrap();
    let minimum = if response == "BLOCK" { 2.0 } else { 0.0 };
    anyhow::ensure!(
        reward.is_finite() && reward > minimum,
        "11th share did not earn regular reward: {reward}"
    );
    close_float(
        s["verify"]["balances"]["alice"].as_f64().unwrap(),
        block_rewards + reward,
        "reward accumulated",
    )
}

/// With `maxWorkers = 1`, one AVR (weight 1) or two ESP32s (weight 0.5 each)
/// fill the limit. One more is rejected with `BAD,Too many workers`, and its
/// release leaves the allowed workers working.
async fn worker_limit(h: &Harness, tier: &str) -> Result<()> {
    let allowed = if tier == "ESP32" { 2 } else { 1 };
    let mut miners = vec![];
    for i in 0..allowed {
        let mut m = h.miner("alice", tier, &format!("limit{i}")).await?;
        m.share().await?;
        miners.push(m);
    }
    let mut excess = h.miner("alice", tier, "excess").await?;
    let job = excess.job().await?;
    anyhow::ensure!(
        excess.answer(&job, true).await? == "BAD,Too many workers",
        "Worker limit not enforced"
    );
    excess.close().await?;
    h.wait_state("excess worker released", |s| {
        s["workers"]["alice"]["active_half_steps"] == 2
    })
    .await?;
    for miner in &mut miners {
        miner.share().await?;
    }
    Ok(())
}

// ---- kolka ---------------------------------------------------------------------

/// Kolka IDs compact after disconnects. The step is 1.0 for PC tiers and 0.5
/// for ESP tiers.
///
/// Sequence: a,b,c get 1,2,3 steps. When b leaves, c moves down to step 2.
/// When c leaves, a new d takes step 2. When everyone leaves, the next miner
/// starts again at step 1.
async fn compaction(h: &Harness, esp: bool) -> Result<()> {
    let tier = if esp { "ESP32" } else { "LOW" };
    let step = if esp { 0.5 } else { 1.0 };
    // Weight in half-steps of `n` workers of this tier.
    let weight = |n: u64| if esp { n } else { 2 * n };

    let mut a = h.miner("alice", tier, "a").await?;
    let mut b = h.miner("alice", tier, "b").await?;
    let mut c = h.miner("alice", tier, "c").await?;
    a.share().await?;
    b.share().await?;
    c.share().await?;
    check_ids(
        h,
        &[("a", step), ("b", step * 2.0), ("c", step * 3.0)],
        "alice",
        weight(3),
    )
    .await?;

    // Removing the middle worker lets the highest move down on its next job.
    b.close().await?;
    h.wait_state("middle worker disconnected", |s| s["connections"] == 2)
        .await?;
    c.share().await?;
    check_ids(h, &[("a", step), ("c", step * 2.0)], "alice", weight(2)).await?;

    // Removing the highest worker frees its slot for a newcomer.
    c.close().await?;
    h.wait_state("highest worker disconnected", |s| s["connections"] == 1)
        .await?;
    let mut d = h.miner("alice", tier, "d").await?;
    d.share().await?;
    check_ids(h, &[("a", step), ("d", step * 2.0)], "alice", weight(2)).await?;

    a.close().await?;
    d.close().await?;
    h.empty().await?;
    let mut fresh = h.miner("alice", tier, "fresh").await?;
    fresh.share().await?;
    check_ids(h, &[("fresh", step)], "alice", weight(1)).await
}

/// PC and ESP workers of one user share an allocator. Each kind can move into
/// a slot released by the other.
async fn mixed_reuse(h: &Harness) -> Result<()> {
    let mut pc = h.miner("alice", "LOW", "pc").await?;
    let mut esp = h.miner("alice", "ESP32", "esp").await?;
    pc.share().await?;
    esp.share().await?;
    check_ids(h, &[("pc", 1.0), ("esp", 1.5)], "alice", 3).await?;

    // The ESP moves into the released PC slot.
    pc.close().await?;
    h.wait_state("PC released", |s| s["connections"] == 1)
        .await?;
    esp.share().await?;
    check_ids(h, &[("esp", 1.0)], "alice", 1).await?;

    let mut new_pc = h.miner("alice", "LOW", "newpc").await?;
    new_pc.share().await?;
    check_ids(h, &[("esp", 1.0), ("newpc", 2.0)], "alice", 3).await?;

    // The PC moves into the released (original PC) slot.
    esp.close().await?;
    h.wait_state("ESP released original PC slot", |s| s["connections"] == 1)
        .await?;
    new_pc.share().await?;
    check_ids(h, &[("newpc", 1.0)], "alice", 2).await?;
    new_pc.close().await?;
    h.empty().await
}

/// Different users have independent allocators. Removing one leaves the other intact.
async fn user_isolation(h: &Harness) -> Result<()> {
    let mut a = h.miner("alice", "LOW", "alice").await?;
    let mut b = h.miner("bob", "ESP32", "bob").await?;
    a.share().await?;
    b.share().await?;
    check_ids(h, &[("alice", 1.0)], "alice", 2).await?;
    check_ids(h, &[("bob", 0.5)], "bob", 1).await?;
    a.close().await?;
    h.wait_state("alice allocation removed without affecting bob", |s| {
        s["workers"].get("alice").is_none() && s["workers"]["bob"]["active_half_steps"] == 1
    })
    .await?;
    b.share().await?;
    check_ids(h, &[("bob", 0.5)], "bob", 1).await
}

// ---- sync ----------------------------------------------------------------------

/// Checks the full sync payload, that a 503 keeps all state, and that the
/// later 200 retry clears pending data and staging files.
async fn sync_roundtrip(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "sync-worker").await?;
    m.share().await?;
    let before = published(h, "sync-worker", 1, 0).await?;
    // Upload 0 is the startup sync; wait for one that carries the share.
    let (_, upload) = h
        .upload_after(1, |u| {
            u.completed && u.query.get("blockIncrease").map(String::as_str) == Some("1")
        })
        .await?;

    // Multipart payload.
    anyhow::ensure!(
        upload.parts["workers"].filename == "workers_verifier.json"
            && upload.parts["rewards"].filename == "rewards_verifier.json",
        "Wrong multipart filenames"
    );
    let workers = upload.parts["workers"]
        .json
        .as_object()
        .context("worker map")?;
    anyhow::ensure!(workers.len() == 1, "Expected one uploaded worker");
    let worker = workers.values().next().unwrap();
    for (key, expected) in [
        ("u", "alice"),
        ("al", "DUCO-S1"),
        ("p", "verifier"),
        ("id", "sync-worker"),
        ("sft", "Verifier"),
        ("wd", "wallet-verifier"),
        ("k", "chip-verifier"),
        ("pw", "dGVzdC1rZXk="), // base64("test-key")
        ("it", "temperature:21"),
    ] {
        anyhow::ensure!(
            worker[key] == expected,
            "Worker field {key}: expected {expected}, actual {}",
            worker[key]
        );
    }
    // Counters and rewards must match what the dashboard published.
    let dashboard = stat(&before, "sync-worker").unwrap();
    for key in ["a", "r", "c", "d", "rw"] {
        anyhow::ensure!(
            worker[key] == dashboard[key],
            "Worker field {key} differs from dashboard"
        );
    }

    // Query string.
    for (key, expected) in [
        ("host", "127.0.0.1"),
        ("identifier", "verifier-id"),
        ("name", "verifier"),
        ("version", "0.3"),
        ("connections", "1"),
        ("blockIncrease", "1"),
    ] {
        anyhow::ensure!(
            upload.query.get(key).map(String::as_str) == Some(expected),
            "Sync query {key}: expected {expected}, actual {:?}",
            upload.query.get(key)
        );
    }
    anyhow::ensure!(
        upload.query.get("port") == Some(&h.port.to_string()),
        "Incorrect sync port"
    );
    for key in ["cpu", "ram"] {
        let number: f64 = upload
            .query
            .get(key)
            .with_context(|| format!("Missing {key}"))?
            .parse()?;
        anyhow::ensure!(number.is_finite(), "Non-finite {key}");
    }
    anyhow::ensure!(
        upload.parts["rewards"].json == before["verify"]["balances"],
        "Rewards payload differs from pending balances"
    );

    // The mock answers 503 by default, which must keep both the in-memory
    // state and the recoverable staging files.
    let retained = h.state().await?;
    anyhow::ensure!(
        retained["verify"] == before["verify"],
        "Failed sync cleared pending state"
    );
    for name in ["workers_verifier.json", "rewards_verifier.json"] {
        anyhow::ensure!(
            h.dir.join("sync").join(name).exists(),
            "Failed sync removed {name}"
        );
    }

    // A successful retry sends the same rewards, then clears everything pending.
    let index = h.mock.uploads().len();
    h.mock.status(200);
    let (_, retry) = h
        .upload_after(index, |u| u.completed && u.status == 200)
        .await?;
    anyhow::ensure!(
        retry.parts["rewards"].json == upload.parts["rewards"].json,
        "Retry changed pending rewards"
    );
    h.wait_state(
        "successful sync clears pending data, preserves total",
        |s| {
            s["verify"]["shares_pending"] == 0
                && s["verify"]["balances"].as_object().unwrap().is_empty()
                && s["verify"]["shares_total"] == 1
        },
    )
    .await?;
    let deadline = Instant::now() + WAIT;
    while h.dir.join("sync/workers_verifier.json").exists()
        || h.dir.join("sync/rewards_verifier.json").exists()
    {
        anyhow::ensure!(
            Instant::now() < deadline,
            "Successful sync did not remove staging files"
        );
        sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// After a worker disconnects, later uploads neither list it nor count its connection.
async fn disconnected_worker(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "gone").await?;
    m.share().await?;
    // Make sure the worker was uploaded at least once while connected.
    h.upload_after(1, |u| {
        u.parts
            .get("workers")
            .is_some_and(|p| !p.json.as_object().unwrap().is_empty())
    })
    .await?;
    m.close().await?;
    h.empty().await?;
    let start = h.mock.uploads().len();
    let (_, upload) = h.upload_after(start, |_| true).await?;
    anyhow::ensure!(
        upload.parts["workers"].json.as_object().unwrap().is_empty(),
        "Disconnected worker was uploaded"
    );
    anyhow::ensure!(
        upload.query.get("connections").map(String::as_str) == Some("0"),
        "Disconnected connection still counted"
    );
    Ok(())
}

/// Work accepted while an upload is in flight must survive that upload's
/// success and appear in the next upload:
/// * `inflight-shares`: the share count;
/// * `inflight-rewards`: the reward delta.
async fn inflight(case: &Case, h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "inflight").await?;
    m.share().await?;
    published(h, "inflight", 1, 0).await?;

    // Hold the next upload that carries the first share...
    h.mock.hold(true);
    let start = h.mock.uploads().len();
    let (index, snapshot) = h
        .upload_after(start, |u| {
            !u.completed && u.query.get("blockIncrease").map(String::as_str) == Some("1")
        })
        .await?;
    // ...submit a second share while it is in flight...
    m.share().await?;
    let before = published(h, "inflight", 2, 0).await?;
    // ...then let it succeed.
    h.mock.status(200);
    h.mock.release();
    h.upload_after(index, |u| u.completed && u.status == 200)
        .await?;

    // Wait for the clearing to finish, then inspect the next real upload.
    // Nothing here relies on timing alone.
    let (_, next) = h.upload_after(index + 1, |u| !u.completed).await?;
    match case.name {
        "inflight-shares" => {
            anyhow::ensure!(
                next.query.get("blockIncrease").map(String::as_str) == Some("1"),
                "Share accepted during upload was lost: expected next blockIncrease=1, actual {:?}",
                next.query.get("blockIncrease")
            );
        }
        "inflight-rewards" => {
            let previous = snapshot.parts["rewards"].json["alice"]
                .as_f64()
                .context("Initial reward")?;
            let expected = before["verify"]["balances"]["alice"].as_f64().unwrap() - previous;
            let actual = next.parts["rewards"].json["alice"].as_f64().unwrap_or(0.0);
            close_float(
                actual,
                expected,
                "reward accepted during upload must remain pending",
            )?;
        }
        _ => unreachable!(),
    }
    h.mock.hold(false);
    h.mock.release();
    Ok(())
}

/// While an upload is in flight, an existing user (alice) earns more and a
/// new user (bob) appears. When the upload completes, exactly the snapshotted
/// work is acknowledged and the remainder is sent next:
/// * `inflight-failed-retry`: the upload fails (503), so nothing is acknowledged;
/// * `cleanup-missing-rewards` / `-workers`: the upload succeeds, but deleting
///   a staging file fails.
///
/// A final successful retry must acknowledge the remainder exactly once.
async fn inflight_completion(case: &Case, h: &Harness) -> Result<()> {
    let mut alice = h.miner("alice", "AVR", "completion-alice").await?;
    alice.share().await?;
    published(h, "completion-alice", 1, 0).await?;
    h.mock.hold(true);
    let start = h.mock.uploads().len();
    let (index, snapshot) = h
        .upload_after(start, |u| {
            !u.completed && u.query.get("blockIncrease").map(String::as_str) == Some("1")
        })
        .await?;

    // Work that arrives while the snapshot is in flight.
    alice.share().await?;
    alice.share().await?;
    published(h, "completion-alice", 3, 0).await?;
    let mut bob = h.miner("bob", "AVR", "completion-bob").await?;
    bob.share().await?;
    let before = published(h, "completion-bob", 1, 0).await?;

    let failed_upload = case.name == "inflight-failed-retry";
    // Make the real success branch fail at one staging-file deletion.
    match case.name {
        "cleanup-missing-rewards" => {
            std::fs::remove_file(h.dir.join("sync/rewards_verifier.json"))?;
        }
        "cleanup-missing-workers" => {
            std::fs::remove_file(h.dir.join("sync/workers_verifier.json"))?;
        }
        _ => {}
    }
    let status = if failed_upload { 503 } else { 200 };
    h.mock.status(status);
    h.mock.release();
    h.upload_after(index, |u| u.completed && u.status == status)
        .await?;
    let (next_index, next) = h.upload_after(index + 1, |u| !u.completed).await?;

    // Share counter: everything pending, minus the snapshot if it was acknowledged.
    let previous: u64 = snapshot.query["blockIncrease"].parse()?;
    let acknowledged = if failed_upload { 0 } else { previous };
    let expected = before["verify"]["shares_pending"]
        .as_u64()
        .context("Pending count")?
        - acknowledged;
    anyhow::ensure!(
        next.query["blockIncrease"] == expected.to_string(),
        "blockIncrease after {status}: expected {expected}, actual {}",
        next.query["blockIncrease"]
    );
    // Rewards: the same per user. Users with nothing left disappear.
    let mut expected_rewards = before["verify"]["balances"].clone();
    if !failed_upload {
        for (user, amount) in snapshot.parts["rewards"].json.as_object().unwrap() {
            let remainder = expected_rewards[user].as_f64().unwrap() - amount.as_f64().unwrap();
            if remainder == 0.0 {
                expected_rewards.as_object_mut().unwrap().remove(user);
            } else {
                expected_rewards[user] = remainder.into();
            }
        }
    }
    anyhow::ensure!(
        next.parts["rewards"].json == expected_rewards,
        "Existing-user updates or new-user rewards lost after {status}: expected {expected_rewards}, actual {}",
        next.parts["rewards"].json
    );
    if failed_upload {
        anyhow::ensure!(
            h.state().await?["verify"] == before["verify"],
            "Failed upload changed pending state"
        );
    }

    // A successful retry acknowledges the remainder once, also after a cleanup failure.
    h.mock.status(200);
    h.mock.release();
    h.upload_after(next_index, |u| u.completed && u.status == 200)
        .await?;
    let (_, empty) = h.upload_after(next_index + 1, |u| !u.completed).await?;
    anyhow::ensure!(
        empty.query["blockIncrease"] == "0"
            && empty.parts["rewards"].json.as_object().unwrap().is_empty(),
        "Acknowledged work was uploaded again"
    );
    h.mock.hold(false);
    h.mock.release();
    Ok(())
}

// ---- lifecycle -----------------------------------------------------------------

/// Startup fetches the reward curve, bans and IP, logs in to the master, and persists
/// bans. `/ping` works, and `/data` requires the right key.
async fn startup_and_auth(h: &Harness) -> Result<()> {
    let requests = h.mock.diagnostics();
    let gets = requests["get_requests"].as_array().unwrap();
    for path in ["/curve.json", "/bans.json", "/ip"] {
        anyhow::ensure!(
            gets.contains(&Value::String(path.into())),
            "Missing startup GET {path}"
        );
    }

    let login = requests["master_requests"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|v| v.as_str().and_then(|s| s.strip_prefix("PoolLogin,")))
        .context("Missing master login")?;
    let login: Value = serde_json::from_str(login)?;
    anyhow::ensure!(
        login["identifier"] == "verifier-id"
            && login["name"] == "verifier"
            && login["host"] == "127.0.0.1"
            && login["port"] == h.port,
        "Incorrect login payload: {login}"
    );

    let bans: Value = serde_json::from_slice(&std::fs::read(h.dir.join("config/bans.json"))?)?;
    anyhow::ensure!(
        bans["bannedUsernames"]
            .as_array()
            .unwrap()
            .contains(&Value::String("banned-verifier-user".into())),
        "Bans not persisted"
    );

    let ping: Value = h
        .client
        .get(format!("{}/ping", h.dashboard))
        .send()
        .await?
        .json()
        .await?;
    anyhow::ensure!(
        ping["success"] == true && ping["result"] == "Pong!",
        "Bad ping response"
    );

    // Missing and wrong keys both look like a missing page.
    for key in [None, Some("wrong-key")] {
        let mut request = h.client.get(format!("{}/data", h.dashboard));
        if let Some(key) = key {
            request = request.query(&[("key", key)]);
        }
        anyhow::ensure!(
            request.send().await?.status() == 404,
            "Dashboard accepted invalid/missing key"
        );
    }
    h.state().await?;
    Ok(())
}

/// With `maxSharesPerMinute = 2`, the fourth share within a minute is rejected
/// as `BAD,Too low difficulty`.
async fn share_rate_limit(h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "rate").await?;
    // The first share starts the minute window; the next two are within the limit.
    for _ in 0..3 {
        m.share().await?;
    }
    let job = m.job().await?;
    anyhow::ensure!(
        m.answer(&job, true).await? == "BAD,Too low difficulty",
        "Share-rate limit was not enforced"
    );
    published(h, "rate", 3, 1).await?;
    Ok(())
}

/// A banned username is rejected and disconnected, and its IP is banned and
/// persisted. The client connects from 127.0.0.2 because 127.0.0.1 is exempt.
async fn banned_user(h: &Harness) -> Result<()> {
    let mut m = h
        .miner_from("banned-verifier-user", "AVR", "banned", Some("127.0.0.2"))
        .await?;
    m.send(b"JOB,banned-verifier-user,AVR\n").await?;
    anyhow::ensure!(
        m.line().await? == "BAD,User banned",
        "Banned user was not rejected"
    );
    m.expect_closed(WAIT).await?;
    h.empty().await?;
    let bans: Value = serde_json::from_slice(&std::fs::read(h.dir.join("config/bans.json"))?)?;
    anyhow::ensure!(
        bans["bannedIPs"]
            .as_array()
            .unwrap()
            .contains(&Value::String("127.0.0.2".into())),
        "Banned IP not saved"
    );
    Ok(())
}

/// Graceful shutdown disconnects miners, uploads pending work in a final sync,
/// and logs out from the master.
async fn shutdown_final_sync(h: &mut Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "shutdown").await?;
    m.share().await?;
    published(h, "shutdown", 1, 0).await?;
    let before = h.mock.uploads().len();
    h.mock.status(200);
    h.stop().await?;
    m.expect_closed(WAIT).await?;
    let (_, upload) = h.upload_after(before, |u| u.completed).await?;
    anyhow::ensure!(
        upload.query.get("blockIncrease").map(String::as_str) == Some("1"),
        "Final sync omitted pending share"
    );
    close_float(
        upload.parts["rewards"].json["alice"]
            .as_f64()
            .context("Final reward")?,
        2.0,
        "final reward",
    )?;
    anyhow::ensure!(
        h.mock.diagnostics()["master_requests"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "PoolLogout,verifier-id"),
        "No master logout"
    );
    Ok(())
}
