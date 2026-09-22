//! Additional runtime and injected-test assertions owned by the verifier.
//!
//! This file extends the original scenario registry without making production code
//! responsible for test orchestration. Internal cases map to tests appended under
//! `#[cfg(test)]`; all other cases run against a separate release pool process.
use crate::harness::{stat, Harness, Options, WAIT};
use crate::mock::{Fault, Mock};
use crate::suites::Case;
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::time::{sleep, timeout};
macro_rules! c {
    ($s:literal,$n:literal) => {
        Case {
            suite: $s,
            name: $n,
            slow: false,
        }
    };
}
pub const CASES: &[Case] = &[
    c!("internal", "reward-vectors"),
    c!("internal", "tier-transitions"),
    c!("internal", "adaptive-vectors"),
    c!("internal", "fallback-and-sanitizers"),
    c!("internal", "seeded-allocator"),
    c!("internal", "config-defaults-roundtrip"),
    c!("internal", "bans-persistence"),
    // c!("counters", "interval-two-counts"),
    // c!("counters", "interval-five-counts"),
    c!("counters", "interval-two-rewards"),
    c!("counters", "interval-five-rewards"),
    c!("counters", "partial-disconnect"),
    c!("counters", "partial-shutdown"),
    c!("counters", "publication-rejection-boundaries"),
    c!("counters", "reward-formula-mining"),
    c!("protocol", "fragmented-frames"),
    c!("protocol", "frame-boundaries"),
    c!("protocol", "empty-frame"),
    c!("protocol", "field-count-limits"),
    c!("protocol", "nonce-overflow"),
    c!("protocol", "motd-version"),
    c!("protocol", "invalid-hashrates"),
    c!("protocol", "estimated-hashrates"),
    c!("protocol", "modified-hashrate"),
    c!("protocol", "metadata-sanitization"),
    c!("protocol", "metadata-defaults"),
    c!("protocol", "iot-boundaries"),
    c!("protocol", "password-boundaries"),
    c!("protocol", "legacy-key"),
    c!("protocol", "hash-chain-block-details"),
    c!("kolka", "reverse-mixed-reuse"),
    c!("kolka", "overlapping-churn"),
    c!("kolka", "worker-limit-recovery"),
    c!("lifecycle", "adaptive-mining-window"),
    c!("lifecycle", "all-tier-initial-jobs"),
    c!("lifecycle", "dashboard-disabled"),
    c!("lifecycle", "dashboard-key-boundary"),
    c!("lifecycle", "dashboard-schema"),
    c!("lifecycle", "guessed-port"),
    c!("lifecycle", "preexisting-ip-ban"),
    c!("lifecycle", "ban-reconnect"),
    c!("lifecycle", "localhost-exemption"),
    c!("lifecycle", "refreshed-bans"),
    c!("lifecycle", "refreshed-rewards"),
    c!("lifecycle", "startup-invalid-config"),
    c!("lifecycle", "startup-missing-field"),
    c!("lifecycle", "startup-invalid-rewards"),
    c!("lifecycle", "startup-missing-tier"),
    c!("lifecycle", "startup-invalid-bans"),
    c!("lifecycle", "startup-ip-disconnect"),
    c!("lifecycle", "startup-master-reject"),
    c!("lifecycle", "startup-master-disconnect"),
    c!("lifecycle", "startup-master-invalid-utf8"),
    c!("lifecycle", "startup-master-delay"),
    c!("sync", "transport-disconnect-retry"),
    c!("sync", "transport-timeout-retry"),
    c!("sync", "ip-fallback"),
    c!("sync", "ip-no-cache"),
    c!("sync", "staging-workers-write-failure"),
    c!("sync", "staging-rewards-write-failure"),
    c!("sync", "shutdown-held-upload"),
    Case {
        suite: "connections",
        name: "active-job-timeout",
        slow: true,
    },
    Case {
        suite: "lifecycle",
        name: "rate-window-reset",
        slow: true,
    },
];

pub fn options(case: &Case, o: &mut Options) {
    // Mutate the shared default Options only where a scenario needs a distinct
    // fixture. A null config value means the key will be omitted entirely.
    o.case = case.name;
    match case.name {
        "interval-two-counts" | "interval-two-rewards" | "publication-rejection-boundaries" => {
            o.stats_every = 2
        }
        "interval-five-counts"
        | "interval-five-rewards"
        | "partial-disconnect"
        | "partial-shutdown" => o.stats_every = 5,
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
    if case.name.starts_with("startup-")
        && case.name != "startup-and-auth"
        && case.name != "startup-master-delay"
    {
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

pub fn configure_mock(name: &str, mock: &Mock) {
    // Queue startup faults before the child launches; otherwise its first request
    // could race ahead of scenario setup.
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
        "startup-ip-disconnect" => {
            for _ in 0..4 {
                mock.fault("/ip", Fault::Disconnect);
            }
        }
        "startup-master-reject" => {
            mock.shared.master_reply.lock().unwrap().reply = b"LoginRejected".to_vec()
        }
        "startup-master-disconnect" => mock.shared.master_reply.lock().unwrap().greeting.clear(),
        "startup-master-invalid-utf8" => {
            mock.shared.master_reply.lock().unwrap().greeting = vec![255]
        }
        "startup-master-delay" => mock.shared.master_reply.lock().unwrap().delay_ms = 200,
        // Login succeeds; the immediate startup sync has no cached IP yet.
        "ip-no-cache" => {
            mock.fault("/ip", Fault::Delay(0));
            mock.fault("/ip", Fault::Delay(4000));
        }
        _ => {}
    }
}

pub async fn run_internal(case: &Case, home: &Path, dir: &Path) -> Result<()> {
    // Rust test filtering is string based. Use full exact paths and verify the
    // runner reports one match so a renamed injected test cannot silently pass.
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
    let log = std::fs::File::create(dir.join("internal.log"))?;
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
    let log = std::fs::read_to_string(dir.join("internal.log"))?;
    anyhow::ensure!(
        log.contains("running 1 test"),
        "Internal test filter matched no test: {log}"
    );
    anyhow::ensure!(status.success(), "Internal assertion failed: {log}");
    Ok(())
}

fn near(actual: f64, expected: f64, label: &str) -> Result<()> {
    anyhow::ensure!(
        actual.is_finite() && (actual - expected).abs() <= 1e-6f64.max(expected.abs() * 2e-5),
        "{label}: {actual} != {expected}"
    );
    Ok(())
}
async fn observation(h: &Harness, user: &str, accepted: u64, rejected: u64) -> Result<Value> {
    let s = h
        .wait_state("completed mining observation", |s| {
            s["verify"]["observations"].as_object().is_some_and(|m| {
                m.values().any(|v| {
                    v["user"] == user && v["accepted"] == accepted && v["rejected"] == rejected
                })
            })
        })
        .await?;
    Ok(s["verify"]["observations"]
        .as_object()
        .unwrap()
        .values()
        .find(|v| v["user"] == user && v["accepted"] == accepted && v["rejected"] == rejected)
        .unwrap()
        .clone())
}
fn accepted(reply: &str) -> Result<()> {
    anyhow::ensure!(
        reply == "GOOD" || reply == "BLOCK",
        "Expected acceptance: {reply}"
    );
    Ok(())
}

pub async fn run(case: &Case, h: &mut Harness) -> Result<()> {
    match case.name {
        name if name.starts_with("startup-") && name != "startup-master-delay" => {
            let diagnostic = match name {
                "startup-invalid-rewards" | "startup-invalid-bans" => {
                    "failed to update pool rewards"
                }
                "startup-missing-tier" => "Failed to get DUE",
                "startup-ip-disconnect" | "startup-master-invalid-utf8" => "failed to login",
                "startup-master-reject" | "startup-master-disconnect" => "Unknown error",
                _ => "panicked",
            };
            h.expect_startup_failure(diagnostic).await?;
        }
        "startup-master-delay" => {
            h.miner("alice", "AVR", "delayed").await?.share().await?;
        }
        // "interval-two-counts" | "interval-five-counts" => {
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
        // }
        "interval-two-rewards" | "interval-five-rewards" => {
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
        }
        "partial-disconnect" | "partial-shutdown" => {
            let mut m = h.miner("alice", "AVR", "partial").await?;
            for i in 1..=3 {
                m.share().await?;
                observation(h, "alice", i, 0).await?;
            }
            if case.name == "partial-disconnect" {
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
            } else {
                let index = h.mock.uploads().len();
                h.mock.status(200);
                h.stop().await?;
                let (_, u) = h.upload_after(index, |u| u.completed).await?;
                anyhow::ensure!(
                    u.query["blockIncrease"] == "3" && u.parts["rewards"].json["alice"] == 6.0,
                    "Shutdown lost partial batch: {u:?}"
                );
            }
        }
        "publication-rejection-boundaries" => {
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
        }
        "reward-formula-mining" => {
            for (index, tier) in ["AVR", "ESP32", "LOW", "EXTREME"].iter().enumerate() {
                let user = format!("formula{index}");
                let mut m = h.miner(&user, tier, "formula").await?;
                let mut block_extra = 0.0;
                for _ in 0..10 {
                    if m.share().await? == "BLOCK" {
                        block_extra += 2.0;
                    }
                }
                let reply = m.share().await?;
                let o = observation(h, &user, 11, 0).await?;
                let expected = 2.0
                    * o["hashrate_calc"]
                        .as_f64()
                        .context("calculated hashrate")?
                        .ln()
                    / 1000.0
                    + if reply == "BLOCK" { 2.0 } else { 0.0 };
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
        }
        "fragmented-frames" => {
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
            accepted(&m.line().await?)?;
            m.share().await?;
        }
        "frame-boundaries" => {
            for (i, size) in [1023usize, 1024, 1025].into_iter().enumerate() {
                let mut m = h.miner("alice", "AVR", &format!("size{i}")).await?;
                let prefix = "JOB,alice,AVR,,";
                let frame = format!("{prefix}{}\n", "x".repeat(size - prefix.len() - 1));
                m.send(frame.as_bytes()).await?;
                if size <= 1024 {
                    let job = m.read_job().await?;
                    accepted(&m.answer(&job, true).await?)?;
                    m.close().await?;
                } else {
                    m.expect_closed(WAIT).await?;
                }
                h.empty().await?;
            }
        }
        "empty-frame" => {
            let mut m = h.miner("alice", "AVR", "empty").await?;
            m.send(b"\r\n").await?;
            m.expect_closed(WAIT).await?;
            h.empty().await?;
        }
        "field-count-limits" => {
            for fields in [6, 7, 9] {
                let mut m = h.miner("alice", "AVR", "fields").await?;
                let frame = format!("JOB,alice,AVR{}\n", ",x".repeat(fields - 3));
                m.send(frame.as_bytes()).await?;
                if fields == 6 {
                    let job = m.read_job().await?;
                    accepted(&m.answer(&job, true).await?)?;
                    m.close().await?;
                } else {
                    anyhow::ensure!(m.line().await? == "BAD,Incorrect data", "field-count limit");
                    m.expect_closed(WAIT).await?;
                }
                h.empty().await?;
            }
        }
        "nonce-overflow" => {
            let mut m = h.miner("alice", "AVR", "overflow").await?;
            m.job().await?;
            anyhow::ensure!(
                m.answer_fields("18446744073709551616".into(), "1000", "chip")
                    .await?
                    == "BAD,No response to job",
                "overflow feedback"
            );
            m.expect_closed(WAIT).await?;
            h.empty().await?;
        }
        "motd-version" => {
            let mut m = h.miner("alice", "AVR", "version").await?;
            for _ in 0..3 {
                m.send(b"MOTD\n").await?;
                anyhow::ensure!(m.line().await? == "verifier-motd", "MOTD");
            }
            let manifest = std::fs::read_to_string(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.toml"),
            )?;
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
        }
        "invalid-hashrates" => {
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
                accepted(&m.answer(&job, true).await?)?;
                m.close().await?;
                h.empty().await?;
            }
            anyhow::ensure!(
                errors.is_empty(),
                "Invalid-rate handling: {}",
                errors.join("; ")
            );
        }
        "estimated-hashrates" => {
            for (i, rate) in [None, Some("not-a-number")].iter().enumerate() {
                let user = format!("estimate{i}");
                let mut m = h.miner(&user, "AVR", "estimate").await?;
                let job = m.job().await?;
                sleep(Duration::from_millis(5)).await;
                let msg = match rate {
                    None => format!("{}\n", job.nonce),
                    Some(r) => format!("{},{r},Verifier,estimate,chip,wallet\n", job.nonce),
                };
                m.send(msg.as_bytes()).await?;
                accepted(&m.line().await?)?;
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
        }
        "modified-hashrate" => {
            let mut m = h.miner("alice", "ESP8266", "modified").await?;
            let job = m.job().await?;
            sleep(Duration::from_millis(1700)).await;
            anyhow::ensure!(
                m.answer_fields(job.nonce.to_string(), "100000", "chip-verifier")
                    .await?
                    == "BAD,Modified hashrate",
                "Modified hashrate not rejected"
            );
            m.share().await?;
            observation(h, "alice", 1, 1).await?;
        }
        "metadata-sanitization" | "metadata-defaults" | "legacy-key" => {
            let mut m = h.miner("alice", "AVR", "metadata").await?;
            let job = m.job().await?;
            let payload = match case.name {
                "metadata-defaults" => format!("{},1000,!!!,!!!\n", job.nonce),
                "legacy-key" => format!("{},1000,Verifier,rig:legacy,chip,wallet\n", job.nonce),
                _ => format!("{},1000,Mi_n!eré.(x)-,ri_g!é.(x)-,chip,wallet\n", job.nonce),
            };
            m.send(payload.as_bytes()).await?;
            accepted(&m.line().await?)?;
            observation(h, "alice", 1, 0).await?;
            let s = h.state().await?;
            let stats = s["miner_stats"]
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap();
            match case.name {
                "metadata-defaults" => anyhow::ensure!(
                    stats["sft"] == "Unknown miner"
                        && stats["id"] == "None"
                        && stats["wd"] == "None",
                    "defaults: {stats}"
                ),
                "legacy-key" => anyhow::ensure!(
                    stats["id"] == "rig" && stats["pw"] == "bGVnYWN5",
                    "legacy key: {stats}"
                ),
                _ => anyhow::ensure!(
                    stats["sft"] == "Miner.(x)-" && stats["id"] == "rig.(x)-",
                    "sanitization: {stats}"
                ),
            }
        }
        "iot-boundaries" => {
            for (i, text) in ["T:21@a_!é".into(), "x".repeat(48), "x".repeat(49)]
                .iter()
                .enumerate()
            {
                let mut m = h.miner(&format!("iot{i}"), "AVR", "iot").await?;
                m.send(format!("JOB,iot{i},AVR,key,{text}\n").as_bytes())
                    .await?;
                let job = m.read_job().await?;
                accepted(&m.answer(&job, true).await?)?;
                let s = h
                    .wait_state("IoT stats", |s| stat(s, "iot").is_some())
                    .await?;
                let expected = match i {
                    0 => "T:21@a",
                    1 => text.as_str(),
                    _ => "Error:data too long",
                };
                anyhow::ensure!(
                    stat(&s, "iot").unwrap()["it"] == expected,
                    "IoT boundary {i}"
                );
                m.close().await?;
                h.empty().await?;
            }
        }
        "password-boundaries" => {
            for size in [96, 97] {
                let mut m = h.miner("alice", "AVR", "password").await?;
                m.send(format!("JOB,alice,AVR,{}\n", "a".repeat(size)).as_bytes())
                    .await?;
                if size == 96 {
                    let job = m.read_job().await?;
                    accepted(&m.answer(&job, true).await?)?;
                    let s = h
                        .wait_state("password encoded", |s| stat(s, "password").is_some())
                        .await?;
                    anyhow::ensure!(
                        stat(&s, "password").unwrap()["pw"] == "YWFh".repeat(32),
                        "base64 boundary"
                    );
                    m.close().await?;
                } else {
                    m.expect_closed(WAIT).await?;
                }
                h.empty().await?;
            }
        }
        "hash-chain-block-details" => {
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
            accepted(&m.answer(&next, true).await?)?;
        }
        "reverse-mixed-reuse" => {
            let mut a = h.miner("alice", "ESP32", "esp").await?;
            let mut b = h.miner("alice", "LOW", "pc").await?;
            a.share().await?;
            b.share().await?;
            a.close().await?;
            h.wait_state("ESP released", |s| s["connections"] == 1)
                .await?;
            b.share().await?;
            h.wait_state("PC takes ESP slot", |s| {
                stat(s, "pc").is_some_and(|v| v["c"] == 0.5)
                    && s["workers"]["alice"]["active_half_steps"] == 2
            })
            .await?;
            b.close().await?;
            h.empty().await?;
            let mut c = h.miner("alice", "LOW", "new").await?;
            c.share().await?;
            h.wait_state("fresh PC ID", |s| {
                stat(s, "new").is_some_and(|v| v["c"] == 1.0)
            })
            .await?;
        }
        "overlapping-churn" => {
            let mut tasks = tokio::task::JoinSet::new();
            for i in 0..32 {
                let mut m = h
                    .miner(
                        "alice",
                        if i % 2 == 0 { "ESP32" } else { "AVR" },
                        &format!("churn{i}"),
                    )
                    .await?;
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
            while let Some(r) = tasks.join_next().await {
                r??;
            }
            h.empty().await?;
        }
        "worker-limit-recovery" => {
            let mut a = h.miner("alice", "AVR", "first").await?;
            a.share().await?;
            let mut b = h.miner("alice", "AVR", "second").await?;
            let j = b.job().await?;
            anyhow::ensure!(
                b.answer(&j, true).await? == "BAD,Too many workers",
                "limit not applied"
            );
            a.close().await?;
            h.wait_state("one worker", |s| s["connections"] == 1)
                .await?;
            b.share().await?;
            observation(h, "alice", 1, 1).await?;
        }
        "adaptive-mining-window" => {
            let mut m = h.miner("alice", "LOW", "adaptive").await?;
            let mut expected_samples = std::collections::VecDeque::new();
            for i in 1..=55 {
                let job = m.job().await?;
                if i <= 5 {
                    anyhow::ensure!(job.difficulty == 11, "Adjusted before four accepted shares");
                } else {
                    anyhow::ensure!(job.difficulty >= 5000, "Adaptive floor not applied");
                }
                accepted(&m.answer(&job, true).await?)?;
                let o = observation(h, "alice", i, 0).await?;
                let samples = o["samples"].as_array().context("samples")?;
                let expected_sample =
                    o["share_time"].as_f64().context("observed share time")? * 1.5;
                near(
                    samples.last().unwrap().as_f64().context("new sample")?,
                    expected_sample,
                    "share-time sample scaling",
                )?;
                expected_samples.push_back(expected_sample);
                if expected_samples.len() > 50 {
                    expected_samples.pop_front();
                }
                anyhow::ensure!(o["sample_count"] == i.min(50), "rolling window size");
                near(
                    o["sample_sum"].as_f64().unwrap(),
                    expected_samples.iter().sum(),
                    "rolling window sum",
                )?;
            }
        }
        "all-tier-initial-jobs" => {
            for (i, tier) in [
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
            ]
            .iter()
            .enumerate()
            {
                let mut m = h.miner("alice", tier, "tier").await?;
                let j = m.job().await?;
                anyhow::ensure!(j.difficulty == i as u64 + 1, "initial {tier}");
                accepted(&m.answer(&j, true).await?)?;
                m.close().await?;
                h.empty().await?;
            }
        }
        "dashboard-disabled" | "dashboard-key-boundary" => {
            let good = h
                .client
                .get(format!("{}/data", h.dashboard))
                .query(&[("key", h.key.as_str())])
                .send()
                .await?;
            let expected = if case.name == "dashboard-disabled" {
                404
            } else {
                200
            };
            anyhow::ensure!(good.status() == expected, "key length boundary");
            let wrong = "y".repeat(h.key.len());
            let r = h
                .client
                .get(format!("{}/data", h.dashboard))
                .query(&[("key", wrong)])
                .send()
                .await?;
            anyhow::ensure!(r.status() == 404, "same-length invalid key accepted");
        }
        "dashboard-schema" => {
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
        }
        "guessed-port" => {
            anyhow::ensure!((1000..9999).contains(&h.port), "Guessed port range");
            h.miner("alice", "AVR", "port").await?.share().await?;
            let requests = h.mock.diagnostics();
            let login = requests["master_requests"]
                .as_array()
                .unwrap()
                .iter()
                .find_map(|v| v.as_str().and_then(|s| s.strip_prefix("PoolLogin,")))
                .context("login")?;
            let login: Value = serde_json::from_str(login)?;
            anyhow::ensure!(login["port"] == h.port, "Guessed port differs from login");
        }
        "preexisting-ip-ban" | "ban-reconnect" | "localhost-exemption" | "refreshed-bans" => {
            if case.name == "refreshed-bans" {
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
            let user = if case.name == "refreshed-bans" {
                "fresh-ban"
            } else if case.name == "preexisting-ip-ban" {
                "alice"
            } else {
                "banned-verifier-user"
            };
            if case.name == "localhost-exemption" {
                h.miner(user, "AVR", "local").await?.share().await?;
            } else {
                let mut m = h.miner_from(user, "AVR", "ban", Some("127.0.0.2")).await?;
                m.send(format!("JOB,{user},AVR\n").as_bytes()).await?;
                anyhow::ensure!(
                    m.line().await? == "BAD,User banned",
                    "Banned identity accepted"
                );
                m.expect_closed(WAIT).await?;
                if case.name == "ban-reconnect" {
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
            }
        }
        "refreshed-rewards" => {
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
            let j = new.job().await?;
            anyhow::ensure!(j.difficulty == 3, "Refreshed reward difficulty not used");
            accepted(&new.answer(&j, true).await?)?;
            let s = h.state().await?;
            anyhow::ensure!(
                s["verify"]["due_cache"] == 7 && s["verify"]["esp_cache"] == 12,
                "Difficulty caches not refreshed"
            );
        }
        "transport-disconnect-retry" | "transport-timeout-retry" => {
            let mut m = h.miner("alice", "AVR", "network").await?;
            m.share().await?;
            observation(h, "alice", 1, 0).await?;
            let before = h.state().await?["verify"].clone();
            let index = h.mock.uploads().len();
            h.mock.fault(
                "/pool_sync/",
                if case.name.contains("timeout") {
                    Fault::Delay(4500)
                } else {
                    Fault::Disconnect
                },
            );
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
        }
        "ip-fallback" => {
            let index = h.mock.uploads().len();
            h.mock.fault("/ip", Fault::Delay(4000));
            h.wait_log("Falling back to last ip collected").await?;
            let (_, u) = h.upload_after(index, |u| u.completed).await?;
            anyhow::ensure!(u.query["host"] == "127.0.0.1", "Cached IP lost");
        }
        "ip-no-cache" => {
            h.wait_log("Failed to get pool ip").await?;
            anyhow::ensure!(
                !h.mock.shared.fault_log.lock().unwrap().is_empty(),
                "No IP fault injected"
            );
            let u = h.mock.uploads();
            anyhow::ensure!(
                u.iter().all(|u| u.query["host"] == "127.0.0.1"),
                "Invalid IP uploaded"
            );
        }
        "staging-workers-write-failure" | "staging-rewards-write-failure" => {
            let mut m = h.miner("alice", "AVR", "disk").await?;
            m.share().await?;
            observation(h, "alice", 1, 0).await?;
            let name = if case.name.contains("workers") {
                "workers_verifier.json"
            } else {
                "rewards_verifier.json"
            };
            let path = h.dir.join("sync").join(name);
            if path.is_file() {
                std::fs::remove_file(&path)?;
            }
            std::fs::create_dir(&path)?;
            // Observe a real failed sync, then restore the fixture before cleanup even if assertion fails.
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
        }
        "shutdown-held-upload" => {
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
            // Aborted request must release the system lock so a final snapshot is attempted.
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
        }
        "active-job-timeout" => {
            let mut m = h.miner("alice", "AVR", "job-timeout").await?;
            m.job().await?;
            let start = tokio::time::Instant::now();
            m.expect_closed(Duration::from_secs(100)).await?;
            anyhow::ensure!(
                start.elapsed() >= Duration::from_secs(85),
                "Job timeout early"
            );
            h.empty().await?;
        }
        "rate-window-reset" => {
            let mut m = h.miner("alice", "AVR", "window").await?;
            for _ in 0..3 {
                m.share().await?;
            }
            let j = m.job().await?;
            anyhow::ensure!(
                m.answer(&j, true).await? == "BAD,Too low difficulty",
                "Initial limit"
            );
            sleep(Duration::from_secs(61)).await;
            let j = m.job().await?;
            let reply = m.answer(&j, true).await?;
            anyhow::ensure!(
                reply == "GOOD" || reply == "BLOCK",
                "First share after expired rate window was rejected: {reply}"
            );
            m.share().await?;
        }
        _ => anyhow::bail!("Unimplemented scenario {}", case.name),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_and_inventory_are_consistent() {
        let mut ids = std::collections::BTreeSet::new();
        for case in crate::suites::CASES.iter().chain(CASES) {
            assert!(
                ids.insert(format!("{}/{}", case.suite, case.name)),
                "duplicate scenario"
            );
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
        mock.respond("/ip", 503, "unavailable");
        let response = client
            .get(format!("{}/ip", mock.http))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        assert_eq!(response.text().await.unwrap(), "unavailable");
        assert_eq!(
            client
                .get(format!("{}/ip", mock.http))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "127.0.0.1"
        );
        mock.fault("/ip", Fault::Disconnect);
        assert!(client
            .get(format!("{}/ip", mock.http))
            .send()
            .await
            .is_err());
        assert_eq!(mock.shared.fault_log.lock().unwrap().len(), 1);
    }
}
