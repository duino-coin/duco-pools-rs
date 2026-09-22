//! Core black-box scenarios for the pool process.
//!
//! Cases are registered as data and dispatched by name below. Assertions use the
//! real miner protocol, captured sync uploads, and the injected read-only dashboard
//! observations; they do not replace the production path being checked.

use crate::harness::{stat, Harness, Miner, Options, WAIT};
use anyhow::{Context, Result};
use serde_json::Value;
use std::time::Duration;
use tokio::time::{sleep, Instant};

pub struct Case {
    pub suite: &'static str,
    pub name: &'static str,
    pub slow: bool,
}
macro_rules! case {
    ($suite:literal, $name:literal) => {
        Case {
            suite: $suite,
            name: $name,
            slow: false,
        }
    };
}
pub const CASES: &[Case] = &[
    case!("connections", "greeting-motd-idle"),
    case!("connections", "disconnect-before-job"),
    case!("connections", "disconnect-before-share"),
    case!("connections", "active-disconnect"),
    case!("connections", "abrupt-reset"),
    case!("connections", "reconnect-cycles"),
    case!("connections", "concurrent-churn"),
    case!("protocol", "good-bad-recovery"),
    case!("protocol", "malformed-answer"),
    case!("protocol", "missing-username"),
    case!("protocol", "framing"),
    case!("protocol", "coalesced-frames"),
    case!("protocol", "oversized-frame"),
    case!("protocol", "invalid-utf8"),
    case!("protocol", "forced-block"),
    case!("counters", "accepted-shares"),
    case!("counters", "rejected-share-accounting"),
    case!("counters", "reward-warmup"),
    case!("counters", "chip-id-required"),
    case!("counters", "chip-id-change"),
    case!("counters", "worker-limit"),
    case!("counters", "weighted-worker-limit"),
    case!("kolka", "pc-compaction"),
    case!("kolka", "esp-compaction"),
    case!("kolka", "mixed-reuse"),
    case!("kolka", "user-isolation"),
    case!("sync", "payload-and-clearing"),
    case!("sync", "failed-upload-retry"),
    case!("sync", "disconnected-worker"),
    case!("sync", "inflight-shares"),
    case!("sync", "inflight-rewards"),
    case!("sync", "inflight-blocks"),
    case!("sync", "inflight-failed-retry"),
    case!("sync", "cleanup-missing-rewards"),
    case!("sync", "cleanup-missing-workers"),
    case!("lifecycle", "startup-and-auth"),
    case!("lifecycle", "difficulty-fallback"),
    case!("lifecycle", "difficulty-upshift"),
    case!("lifecycle", "difficulty-downshift"),
    case!("lifecycle", "share-rate-limit"),
    case!("lifecycle", "banned-user"),
    case!("lifecycle", "shutdown-final-sync"),
    Case {
        suite: "connections",
        name: "idle-timeout",
        slow: true,
    },
];

pub fn options(case: &Case) -> Options {
    // Centralizing fixture differences keeps individual scenario bodies focused on
    // actions and assertions, and makes special production settings easy to audit.
    let mut o = Options::default();
    if case.suite == "sync" {
        o.sync_secs = 1;
    }
    if matches!(
        case.name,
        "forced-block"
            | "payload-and-clearing"
            | "inflight-rewards"
            | "inflight-blocks"
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
    if matches!(case.name, "difficulty-upshift" | "difficulty-downshift") {
        o.transition = true;
    }
    crate::expanded::options(case, &mut o);
    o
}

fn assert_eq_json(actual: &Value, expected: impl Into<Value>, label: &str) -> Result<()> {
    let expected = expected.into();
    anyhow::ensure!(
        *actual == expected,
        "{label}: expected {expected}, actual {actual}"
    );
    Ok(())
}
fn close_float(actual: f64, expected: f64, label: &str) -> Result<()> {
    anyhow::ensure!(
        actual.is_finite() && (actual - expected).abs() <= 0.00001 * expected.abs().max(1.0),
        "{label}: expected {expected}, actual {actual}"
    );
    Ok(())
}
async fn published(h: &Harness, rig: &str, accepted: u64, rejected: u64) -> Result<Value> {
    h.wait_state("miner counters published", |s| {
        stat(s, rig).is_some_and(|m| m["a"] == accepted && m["r"] == rejected)
    })
    .await
}
async fn check_ids(
    h: &Harness,
    expected: &[(&str, f64)],
    user: &str,
    half_steps: u64,
) -> Result<()> {
    // Dashboard stats are keyed independently of rig names, so collect the values
    // and explicitly prove that all active workers received unique Kolka IDs.
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
async fn advance(miners: &mut [Miner]) -> Result<()> {
    // Each job triggers recycling; another completed share publishes the new ID.
    for miner in miners {
        miner.share().await?;
    }
    Ok(())
}

pub async fn run(case: &Case, h: &mut Harness) -> Result<()> {
    // Each arm is a complete arrange/act/assert flow against a fresh Harness.
    match case.name {
        "greeting-motd-idle" => {
            let mut m = h.miner("alice", "AVR", "idle").await?;
            h.wait_state("idle connection counted without a worker", |s| {
                s["connections"] == 1 && s["workers"].as_object().unwrap().is_empty()
            })
            .await?;
            m.send(b"MOTD\n").await?;
            anyhow::ensure!(m.line().await? == "verifier-motd", "Incorrect MOTD");
            m.share().await?;
            m.close().await?;
            h.empty().await?;
        }
        "disconnect-before-job" => {
            h.miner("alice", "AVR", "early").await?.close().await?;
            h.empty().await?;
        }
        "disconnect-before-share" => {
            let mut m = h.miner("alice", "AVR", "job-only").await?;
            m.job().await?;
            h.wait_state("allocated before first share", |s| {
                s["workers"]["alice"]["active_half_steps"] == 2
                    && s["miner_stats"].as_object().unwrap().is_empty()
            })
            .await?;
            m.close().await?;
            h.empty().await?;
        }
        "active-disconnect" | "abrupt-reset" => {
            let mut m = h.miner("alice", "AVR", "active").await?;
            m.share().await?;
            published(h, "active", 1, 0).await?;
            if case.name == "abrupt-reset" {
                m.reset()?;
            } else {
                m.close().await?;
            }
            h.empty().await?;
        }
        "reconnect-cycles" => {
            for _ in 0..8 {
                let mut m = h.miner("alice", "AVR", "reconnect").await?;
                m.share().await?;
                check_ids(h, &[("reconnect", 1.0)], "alice", 2).await?;
                m.close().await?;
                h.empty().await?;
            }
        }
        "concurrent-churn" => {
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
                    ids == (1..=8).map(|i| i as f64).collect::<Vec<_>>(),
                    "Concurrent IDs not contiguous: {ids:?}"
                );
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
        }
        "good-bad-recovery" => {
            let mut m = h.miner("alice", "AVR", "feedback").await?;
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
        }
        "malformed-answer" => {
            let mut m = h.miner("alice", "AVR", "malformed").await?;
            m.job().await?;
            m.send(b"not-a-nonce,1000,Verifier,malformed,chip,wallet\n")
                .await?;
            anyhow::ensure!(
                m.line().await? == "BAD,No response to job",
                "Incorrect malformed-answer feedback"
            );
            m.expect_closed(WAIT).await?;
            h.empty().await?;
        }
        "missing-username" => {
            let mut m = h.miner("alice", "AVR", "missing").await?;
            m.send(b"JOB\n").await?;
            anyhow::ensure!(
                m.line().await? == "BAD,No username specified",
                "Missing username response"
            );
            m.expect_closed(WAIT).await?;
            h.empty().await?;
        }
        "framing" => {
            for (i, ending) in ["\n", "\r\n", ""].iter().enumerate() {
                let mut m = h.miner("alice", "AVR", &format!("frame{i}")).await?;
                let job = m.job_framed(ending).await?;
                let answer = format!("{},1000,Verifier,frame{i},chip,wallet{ending}", job.nonce);
                m.send(answer.as_bytes()).await?;
                let reply = m.line().await?;
                anyhow::ensure!(
                    reply == "GOOD" || reply == "BLOCK",
                    "Framing {ending:?}: {reply}"
                );
                m.close().await?;
                h.empty().await?;
            }
        }
        "coalesced-frames" => {
            let mut m = h.miner("alice", "AVR", "coalesced").await?;
            m.send(b"JOB,alice,AVR\n0,1000,Verifier,coalesced,chip,wallet\n")
                .await?;
            m.read_job().await?;
            anyhow::ensure!(
                m.line().await? == "BAD,Incorrect result",
                "Coalesced second frame lost"
            );
            m.share().await?;
        }
        "oversized-frame" | "invalid-utf8" => {
            let mut m = h.miner("alice", "AVR", "invalid").await?;
            let bytes = if case.name == "invalid-utf8" {
                vec![0xff, b'\n']
            } else {
                let mut b = vec![b'x'; 1025];
                b.push(b'\n');
                b
            };
            m.send(&bytes).await?;
            m.expect_closed(WAIT).await?;
            h.empty().await?;
        }
        "forced-block" => {
            let mut m = h.miner("alice", "AVR", "block").await?;
            anyhow::ensure!(
                m.share().await? == "BLOCK",
                "block_chance=1 did not return BLOCK"
            );
            let s = published(h, "block", 1, 0).await?;
            assert_eq_json(&s["verify"]["blocks"], 1, "pending blocks")?;
            close_float(
                s["verify"]["balances"]["alice"].as_f64().unwrap(),
                2.0,
                "block reward",
            )?;
        }
        "accepted-shares" => {
            let mut m = h.miner("alice", "AVR", "counts").await?;
            for _ in 0..3 {
                m.share().await?;
            }
            let s = published(h, "counts", 3, 0).await?;
            assert_eq_json(&s["verify"]["shares_total"], 3, "total accepted shares")?;
            assert_eq_json(&s["verify"]["shares_pending"], 3, "pending accepted shares")?;
        }
        "rejected-share-accounting" => {
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
        }
        "reward-warmup" => {
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
            anyhow::ensure!(
                reward.is_finite() && reward > if response == "BLOCK" { 2.0 } else { 0.0 },
                "11th share did not earn regular reward: {reward}"
            );
            close_float(
                s["verify"]["balances"]["alice"].as_f64().unwrap(),
                block_rewards + reward,
                "reward accumulated",
            )?;
        }
        "chip-id-required" | "chip-id-change" => {
            let mut m = h.miner("alice", "AVR", "chip").await?;
            for i in 0..11 {
                let job = m.job().await?;
                let chip = if case.name == "chip-id-required" {
                    ""
                } else if i == 10 {
                    "changed-chip"
                } else {
                    "original-chip"
                };
                let reply = m.answer_fields(job.nonce.to_string(), "1000", chip).await?;
                anyhow::ensure!(
                    reply == "GOOD" || reply == "BLOCK",
                    "Unexpected chip-ID feedback {reply}"
                );
            }
            let s = published(h, "chip", 11, 1).await?;
            // Existing protocol counts the valid nonce as accepted, but rejects reward eligibility.
            close_float(
                stat(&s, "chip").unwrap()["rw"].as_f64().unwrap(),
                0.0,
                "ineligible chip must not earn regular reward",
            )?;
        }
        "worker-limit" | "weighted-worker-limit" => {
            let tier = if case.name == "worker-limit" {
                "AVR"
            } else {
                "ESP32"
            };
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
            advance(&mut miners).await?;
        }
        "pc-compaction" | "esp-compaction" => {
            let esp = case.name == "esp-compaction";
            let tier = if esp { "ESP32" } else { "LOW" };
            let step = if esp { 0.5 } else { 1.0 };
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
                if esp { 3 } else { 6 },
            )
            .await?;
            b.close().await?;
            h.wait_state("middle worker disconnected", |s| s["connections"] == 2)
                .await?;
            c.share().await?;
            check_ids(
                h,
                &[("a", step), ("c", step * 2.0)],
                "alice",
                if esp { 2 } else { 4 },
            )
            .await?;
            c.close().await?;
            h.wait_state("highest worker disconnected", |s| s["connections"] == 1)
                .await?;
            let mut d = h.miner("alice", tier, "d").await?;
            d.share().await?;
            check_ids(
                h,
                &[("a", step), ("d", step * 2.0)],
                "alice",
                if esp { 2 } else { 4 },
            )
            .await?;
            a.close().await?;
            d.close().await?;
            h.empty().await?;
            let mut first = h.miner("alice", tier, "fresh").await?;
            first.share().await?;
            check_ids(h, &[("fresh", step)], "alice", if esp { 1 } else { 2 }).await?;
        }
        "mixed-reuse" => {
            let mut a = h.miner("alice", "LOW", "pc").await?;
            let mut b = h.miner("alice", "ESP32", "esp").await?;
            a.share().await?;
            b.share().await?;
            check_ids(h, &[("pc", 1.0), ("esp", 1.5)], "alice", 3).await?;
            a.close().await?;
            h.wait_state("PC released", |s| s["connections"] == 1)
                .await?;
            b.share().await?;
            check_ids(h, &[("esp", 1.0)], "alice", 1).await?;
            let mut c = h.miner("alice", "LOW", "newpc").await?;
            c.share().await?;
            check_ids(h, &[("esp", 1.0), ("newpc", 2.0)], "alice", 3).await?;
            b.close().await?;
            h.wait_state("ESP released original PC slot", |s| s["connections"] == 1)
                .await?;
            c.share().await?;
            check_ids(h, &[("newpc", 1.0)], "alice", 2).await?;
            c.close().await?;
            h.empty().await?;
        }
        "user-isolation" => {
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
            check_ids(h, &[("bob", 0.5)], "bob", 1).await?;
        }
        "payload-and-clearing" | "failed-upload-retry" => sync_roundtrip(case, h).await?,
        "disconnected-worker" => {
            let mut m = h.miner("alice", "AVR", "gone").await?;
            m.share().await?;
            h.upload_after(1, |u| {
                !u.parts
                    .get("workers")
                    .map(|p| p.json.as_object().unwrap().is_empty())
                    .unwrap_or(true)
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
        }
        "inflight-shares" | "inflight-rewards" | "inflight-blocks" => inflight(case, h).await?,
        "inflight-failed-retry" | "cleanup-missing-rewards" | "cleanup-missing-workers" => {
            inflight_completion(case, h).await?;
        }
        "startup-and-auth" => {
            let requests = h.mock.diagnostics();
            let gets = requests["get_requests"].as_array().unwrap();
            for path in ["/poolrewards.json", "/bans.json", "/ip"] {
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
            let bans: Value =
                serde_json::from_slice(&std::fs::read(h.dir.join("config/bans.json"))?)?;
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
        }
        "difficulty-fallback" => {
            let mut m = h.miner("alice", "UNKNOWN-TIER", "fallback").await?;
            let job = m.job().await?;
            anyhow::ensure!(
                job.difficulty == 13,
                "Unknown tier did not fall back to NET: {}",
                job.difficulty
            );
            let response = m.answer(&job, true).await?;
            anyhow::ensure!(
                response == "GOOD" || response == "BLOCK",
                "Fallback job rejected: {response}"
            );
        }
        "difficulty-upshift" | "difficulty-downshift" => {
            let up = case.name == "difficulty-upshift";
            let mut m = h
                .miner("alice", if up { "AVR" } else { "MEGA" }, "transition")
                .await?;
            let job = m.job().await?;
            let feedback = m
                .answer_fields(job.nonce.to_string(), if up { "300" } else { "50" }, "chip")
                .await?;
            anyhow::ensure!(
                feedback
                    == if up {
                        "BAD,Too low starting difficulty"
                    } else {
                        "BAD,Too high starting difficulty"
                    },
                "Wrong transition feedback {feedback}"
            );
            let next = m.job().await?;
            anyhow::ensure!(
                next.difficulty == if up { 2 } else { 1 },
                "Wrong next difficulty {}",
                next.difficulty
            );
            let reply = m
                .answer_fields(
                    next.nonce.to_string(),
                    if up { "300" } else { "50" },
                    "chip",
                )
                .await?;
            anyhow::ensure!(
                reply == "GOOD" || reply == "BLOCK",
                "New tier did not accept appropriate rate: {reply}"
            );
        }
        "share-rate-limit" => {
            let mut m = h.miner("alice", "AVR", "rate").await?;
            // First share starts the minute window; next two are within the configured limit.
            for _ in 0..3 {
                m.share().await?;
            }
            let job = m.job().await?;
            anyhow::ensure!(
                m.answer(&job, true).await? == "BAD,Too low difficulty",
                "Share-rate limit was not enforced"
            );
            published(h, "rate", 3, 1).await?;
        }
        "banned-user" => {
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
            let bans: Value =
                serde_json::from_slice(&std::fs::read(h.dir.join("config/bans.json"))?)?;
            anyhow::ensure!(
                bans["bannedIPs"]
                    .as_array()
                    .unwrap()
                    .contains(&Value::String("127.0.0.2".into())),
                "Banned IP not saved"
            );
        }
        "shutdown-final-sync" => {
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
        }
        "idle-timeout" => {
            let mut m = h.miner("alice", "AVR", "timeout").await?;
            let start = Instant::now();
            m.expect_closed(Duration::from_secs(100)).await?;
            anyhow::ensure!(
                start.elapsed() >= Duration::from_secs(85),
                "Idle connection closed too early"
            );
            h.empty().await?;
        }
        _ => crate::expanded::run(case, h).await?,
    }
    Ok(())
}

async fn sync_roundtrip(case: &Case, h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "sync-worker").await?;
    m.share().await?;
    let before = published(h, "sync-worker", 1, 0).await?;
    let (_, upload) = h
        .upload_after(1, |u| {
            u.completed && u.query.get("blockIncrease").map(String::as_str) == Some("1")
        })
        .await?;
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
        ("pw", "dGVzdC1rZXk="),
        ("it", "temperature:21"),
    ] {
        anyhow::ensure!(
            worker[key] == expected,
            "Worker field {key}: expected {expected}, actual {}",
            worker[key]
        );
    }
    for key in ["a", "r", "c", "d", "rw"] {
        anyhow::ensure!(
            worker[key] == stat(&before, "sync-worker").unwrap()[key],
            "Worker field {key} differs from dashboard"
        );
    }
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
    if case.name == "payload-and-clearing" {
        anyhow::ensure!(
            upload.query.get("bigBlocks").map(String::as_str) == Some("1"),
            "Block count missing"
        );
    }
    // A 503 must preserve both the in-memory state and recoverable files.
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
                && s["verify"]["blocks"] == 0
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

async fn inflight(case: &Case, h: &Harness) -> Result<()> {
    let mut m = h.miner("alice", "AVR", "inflight").await?;
    m.share().await?;
    published(h, "inflight", 1, 0).await?;
    h.mock.hold(true);
    let start = h.mock.uploads().len();
    let (index, snapshot) = h
        .upload_after(start, |u| {
            !u.completed && u.query.get("blockIncrease").map(String::as_str) == Some("1")
        })
        .await?;
    m.share().await?;
    let before = published(h, "inflight", 2, 0).await?;
    h.mock.status(200);
    h.mock.release();
    h.upload_after(index, |u| u.completed && u.status == 200)
        .await?;
    // Observe clearing completion, then inspect the next real upload; no timing-only assertion.
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
        "inflight-blocks" => {
            anyhow::ensure!(
                next.query.get("bigBlocks").map(String::as_str) == Some("1"),
                "Block found during upload was lost: expected next bigBlocks=1, actual {:?}",
                next.query.get("bigBlocks")
            );
        }
        _ => unreachable!(),
    }
    h.mock.hold(false);
    h.mock.release();
    Ok(())
}

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

    alice.share().await?;
    alice.share().await?;
    published(h, "completion-alice", 3, 0).await?;
    let mut bob = h.miner("bob", "AVR", "completion-bob").await?;
    bob.share().await?;
    let before = published(h, "completion-bob", 1, 0).await?;
    let failed_upload = case.name == "inflight-failed-retry";
    match case.name {
        // Force the actual success branch to fail at each staging-file deletion.
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

    for (query, pending) in [("blockIncrease", "shares_pending"), ("bigBlocks", "blocks")] {
        let previous: u64 = snapshot.query[query].parse()?;
        let expected = before["verify"][pending]
            .as_u64()
            .context("Pending count")?
            - if failed_upload { 0 } else { previous };
        anyhow::ensure!(
            next.query[query] == expected.to_string(),
            "{query} after {status}: expected {expected}, actual {}",
            next.query[query]
        );
    }
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

    // A successful retry must acknowledge the remainder once, including after cleanup failure.
    h.mock.status(200);
    h.mock.release();
    h.upload_after(next_index, |u| u.completed && u.status == 200)
        .await?;
    let (_, empty) = h.upload_after(next_index + 1, |u| !u.completed).await?;
    anyhow::ensure!(
        empty.query["blockIncrease"] == "0"
            && empty.query["bigBlocks"] == "0"
            && empty.parts["rewards"].json.as_object().unwrap().is_empty(),
        "Acknowledged work was uploaded again"
    );
    h.mock.hold(false);
    h.mock.release();
    Ok(())
}
