use crate::cfg::config::PoolConfig;
use crate::data::data::{ConnId, Connection};
use crate::mining::id_allocator::WorkerAllocator;
use crate::mining::stats::{BlockInfos, MinerStatSlim};
use crate::sync::sync::Difficulty;
use crate::{kolka, CONFIG, KOLKA, POOL_REWARDS};
use arc_swap::ArcSwap;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use compact_str::CompactString;
use dashmap::DashMap;
use log::{debug, info};
use rand::{rng, RngExt};
use sha1::Digest;
use std::str::FromStr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
// I hate that these are pub static, but I honestly can't be arsed to pass them as params..

pub static USR_WORKERS_ALLOC: LazyLock<DashMap<CompactString, WorkerAllocator>> =
    LazyLock::new(DashMap::new);

pub static BALANCES_TO_UPDATE: LazyLock<DashMap<CompactString, f32>> = LazyLock::new(DashMap::new);

pub static MINER_STATS: LazyLock<DashMap<ConnId, MinerStatSlim>> = LazyLock::new(DashMap::new);

static LAST_BLOCK_HASH: LazyLock<ArcSwap<String>> = LazyLock::new(|| {
    let config = CONFIG.get().expect("config not initialised");
    ArcSwap::from_pointee(config.initial_blockhash.clone())
});

pub static GLOBAL_BLOCKS: LazyLock<Mutex<Vec<BlockInfos>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

pub static GLOBAL_SHARES_INCREASE: AtomicU64 = AtomicU64::new(0);

pub static ESP8266NH_DIFF_CACHE: AtomicU32 = AtomicU32::new(0);

pub static DUE_DIFF_CACHE: AtomicU32 = AtomicU32::new(0);

// longest password (or legacy mining key) accepted, its base64 form fits PASSWORD_B64_BYTES
const MAX_PASSWORD_BYTES: usize = 96;
const PASSWORD_B64_BYTES: usize = MAX_PASSWORD_BYTES.div_ceil(3) * 4;

// every local that lives across an .await is stored in the connection's future,
// so scratch buffers are kept in the sync blocks/functions that use them
pub async fn handle(
    conn: &mut Connection,
    mut read: String, // holds the first JOB request, then reused for every job/answer line
    config: &PoolConfig,
    using_avr: bool,
) -> Result<(), anyhow::Error> {
    let mut random;
    let mut hex_buf = [0u8; 40]; // stack-allocated, reused every iteration
    let mut req_difficulty = CompactString::with_capacity(16);
    let mut expected_share_time = 0;
    let mut feedback_sent = SystemTime::now();
    let mut start_hashrate = -1f32;
    let mut diff = 1u32;
    let mut reported_hashrate;
    let mut hashrate_calc: f32;
    let mut reward: f32;
    let mut hashrate_is_estimated;
    let mut hashrate: f32;
    let mut is_first_share = true;

    let kolka = KOLKA.get().unwrap();
    let pool_rewards = &POOL_REWARDS;
    let mut data: [&str; 8] = [""; 8];
    let mut data_len: usize;

    // the handler already validated this line; scan it the same way (trimmed)
    data_len = scan_fields(read.trim(), &mut data);

    let mut share_accepted: bool;

    loop {
        // set when the requested tier was already looked up for this share
        let mut prefetched_info = None;
        if is_first_share {
            req_difficulty.clear();
            if data_len < 3
                || data[2].is_empty()
                || !set_req_difficulty(&mut req_difficulty, data[2])
            {
                req_difficulty.push_str("NET");
            }
            // if !pool_rewards.contains_key(&req_difficulty) {
            //     req_difficulty.clear();
            //     req_difficulty.push_str("NET");
            // }
            // diff = kolka::kolka::get_diff(&req_difficulty);

            // why??
            if req_difficulty.as_str() == "ESP32" {
                conn.miner_stat.is_req_diff_esp32 = true;
            }
            let mut alloc = USR_WORKERS_ALLOC
                .entry(conn.miner_stat.username.clone())
                .or_default();
            let kolka_id = alloc.allocate(conn.miner_stat.is_req_diff_esp32);

            // conn.miner_stat.amount_of_workers = kolka_id;
            conn.miner_stat.worker_slot = kolka_id;

            // debug!("{}, Assigned {}", conn.id, conn.miner_stat.amount_of_workers);
            debug!("{}, Assigned {}", conn.id, conn.miner_stat.worker_slot.id());
        } else {
            data = [""; 8];
            conn.read_to_string(&mut read).await?;

            conn.miner_stat.ping = SystemTime::now()
                .duration_since(feedback_sent)?
                .as_secs_f32();

            data_len = scan_fields(&read, &mut data);
            req_difficulty.clear();

            if !conn.miner_stat.override_diff.is_empty() {
                if !set_req_difficulty(&mut req_difficulty, conn.miner_stat.override_diff.as_str())
                {
                    req_difficulty.push_str("NET");
                }
            } else if using_avr {
                req_difficulty.push_str("AVR");
            } else {
                if data_len < 3
                    || data[2].is_empty()
                    || !set_req_difficulty(&mut req_difficulty, data[2])
                {
                    req_difficulty.push_str("NET");
                }

                prefetched_info = pool_rewards.get(req_difficulty.as_str()).map(|r| *r);
                if prefetched_info.is_none() {
                    req_difficulty.clear();
                    req_difficulty.push_str("NET");
                }
            }
        }

        if data_len >= 4 {
            if !encode_password(data[3], &mut conn.miner_stat.password) {
                return reject_password(conn).await;
            }
        } else {
            conn.miner_stat.password.clear();
        }

        // IoT reading stuff
        if data_len >= 5 {
            if data[4].len() > 48 {
                conn.miner_stat.iot_reading = CompactString::const_new("Error:data too long");
            } else {
                clean_iot_into(data[4], &mut conn.miner_stat.iot_reading);
            }
        }

        // let user_workers = get_user_workers(&conn.miner_stat.username);
        let (user_workers, new_id) = USR_WORKERS_ALLOC
            .get_mut(&conn.miner_stat.username)
            .map(|mut alloc| {
                let workers = alloc.active_workers();
                // let new_id = alloc.try_recycle(conn.miner_stat.amount_of_workers, conn.miner_stat.is_req_diff_esp32);
                let new_id = alloc.try_recycle(conn.miner_stat.worker_slot);
                (workers, new_id)
            })
            .unwrap_or((0.0, None));

        if check_workers(user_workers, config) {
            conn.miner_stat.reject_shares = Some("BAD,Too many workers\n");
        }
        if let Some(id) = new_id {
            // conn.miner_stat.amount_of_workers = id;
            conn.miner_stat.worker_slot = id;
        }

        let diff_info = match prefetched_info {
            Some(info) => info,
            None => resolve_reward_info(
                pool_rewards,
                req_difficulty.as_str(),
                conn.miner_stat.override_diff.as_str(),
            )
            .ok_or_else(|| anyhow::anyhow!("failed to resolve difficulty info"))?,
        };
        if is_first_share {
            diff = diff_info.difficulty as u32;
        }

        random = rand::rng().random_range(0u64..diff as u64 * 100u64) + 1;

        #[cfg(feature = "bench")]
        {
            std::hint::black_box(random);
            random = 1;
        }

        // the request fields aren't needed anymore, so `read` is free for the job line
        data = [""; 8];
        {
            // scoped so the guard (and the hash version it pins) isn't held across awaits
            let last_block_hash = LAST_BLOCK_HASH.load();
            let mut hasher = sha1::Sha1::new_with_prefix(last_block_hash.as_bytes());

            hasher.update(itoa::Buffer::new().format(random).as_bytes());

            hex::encode_to_slice(hasher.finalize(), &mut hex_buf)?;

            read.clear();
            read.push_str(last_block_hash.as_str());
        }
        let new_hash_str = std::str::from_utf8(&hex_buf)?;
        read.push(',');
        read.push_str(new_hash_str);
        read.push(',');
        read.push_str(itoa::Buffer::new().format(diff));
        read.push('\n');
        conn.write(&read).await?;

        let job_sent = SystemTime::now();
        conn.read_to_string(&mut read).await?;

        data_len = scan_fields(&read, &mut data);
        let answer_received = SystemTime::now();

        conn.miner_stat.share_time = answer_received.duration_since(job_sent)?.as_secs_f32();
        if conn.miner_stat.share_time > conn.miner_stat.ping {
            conn.miner_stat.share_time -= conn.miner_stat.ping;
        }

        // 1.5x to account that the average will be half the target (I don't know what this means, I copied the original comment)
        let val = conn.miner_stat.share_time * 1.5;
        conn.miner_stat.total_share_times += val;
        if let Some(oldest) = conn.miner_stat.share_times_list.push(val) {
            conn.miner_stat.total_share_times -= oldest;
        }

        let mut wrong_reported_hashrate = false;
        if data_len >= 2 {
            reported_hashrate = match data[1].parse::<f32>() {
                Ok(rep) => rep,
                Err(_) => {
                    wrong_reported_hashrate = true;
                    0f32
                }
            };
        } else {
            wrong_reported_hashrate = true;
            reported_hashrate = 0f32;
        }

        if (reported_hashrate <= 0f32 && !wrong_reported_hashrate)
            || reported_hashrate.is_nan()
            || reported_hashrate.is_infinite()
        {
            conn.miner_stat.reject_shares = Some("BAD,Invalid data\n");
        }

        {
            let temp_share_time = conn.miner_stat.share_time + conn.miner_stat.ping; // it benefits the user
            if temp_share_time > 0f32 {
                hashrate_calc = random as f32 / temp_share_time;
            } else {
                hashrate_calc = if wrong_reported_hashrate {
                    start_hashrate
                } else {
                    reported_hashrate
                }; // I'd be damned if the sharetime's actually ever EXACTLY 0...
            }
        }

        if wrong_reported_hashrate {
            reported_hashrate = hashrate_calc;
        }

        conn.miner_stat.last_min_shares += 1;

        let nonce = if using_avr {
            u64::from_str_radix(data[0], 2)?
        } else {
            match u64::from_str(data[0]) {
                Ok(ok) => ok,
                Err(err) => {
                    debug!("Nonce error: {err:?} ({:?})", data);
                    conn.write("BAD,No response to job\n").await?;
                    return Err(anyhow::Error::from_boxed(Box::from("Shutting down.")));
                    // used so we can handle closes centrally
                    // (some request a job and after receiving the job they immediately request another one)
                }
            }
        };

        {
            let now_secs = UNIX_EPOCH.elapsed()?.as_secs();
            if now_secs - conn.miner_stat.last_share_reset >= 60 {
                conn.miner_stat.last_share_reset = now_secs;
                conn.miner_stat.last_min_shares = 0;
            }
        }
        #[cfg(not(feature = "bench"))]
        // if the pool was built with the bench feature enabled, we discard this codeblock
        {
            if conn.miner_stat.last_min_shares > config.max_shares_per_minute {
                conn.miner_stat.reject_shares = Some("BAD,Too low difficulty\n");
            }
        }

        hashrate_is_estimated = false;
        if reported_hashrate <= 0f32 {
            hashrate_is_estimated = true;
            hashrate = hashrate_calc;
        } else {
            hashrate = reported_hashrate;
        }

        if is_first_share {
            conn.miner_stat.this_miner_chipid.clear();
            if data_len >= 5 {
                conn.miner_stat.this_miner_chipid.push_str(data[4]);
            }
        }

        let hashrate_perc_diff_for_calc = perc_diff(hashrate, hashrate_calc);

        if diff > DUE_DIFF_CACHE.load(Relaxed)
            && diff <= ESP8266NH_DIFF_CACHE.load(Relaxed)
            && hashrate_perc_diff_for_calc > 15f32
            && conn.miner_stat.share_time >= 1.5
        {
            conn.miner_stat.reject_shares = Some("BAD,Modified hashrate\n");
        }

        // we don't care whether they overreport their hashrate
        if conn.miner_stat.share_time >= 1.5
            && hashrate_perc_diff_for_calc > 15f32
            && hashrate < hashrate_calc
        {
            // disabled for now..
            // the difference is sometimes (currently) inexplicably high, therefore we rarely want to reject shares for it..
            // if hashrate_perc_diff_for_calc >= 50f32 && false {
            //     conn.miner_stat.reject_shares = Some("BAD,Modified hashrate\n");
            // }
            hashrate = hashrate_calc;
        }

        let avg_sharetime =
            conn.miner_stat.total_share_times / conn.miner_stat.share_times_list.len() as f32;
        let actual_expected_share_time =
            random * expected_share_time as u64 / (diff as u64 * 100 + 1);
        if (conn.miner_stat.accepted_shares == 4
            || (
                conn.miner_stat.accepted_shares > 4
                    && (perc_diff(hashrate, start_hashrate) > 15f32
                        || (1f32 - (avg_sharetime / actual_expected_share_time as f32)) * 100f32
                            > 25f32)
                // if the hashrate has changed substantially or if the sharetime is substantially lower than targeted, adjust
            ))
            && is_pc_diff_tier(req_difficulty.as_str())
        {
            start_hashrate = hashrate;

            diff = kolka::kolka::v3(
                // avg_from_array(&conn.miner_stat.share_times_list),
                avg_sharetime,
                expected_share_time,
                hashrate,
            );
        }
        let reward_div = diff_info.reward;
        let max_hashrate = diff_info.max_hashrate;
        let min_hashrate = diff_info.min_hashrate;
        expected_share_time = diff_info.expected_share_time;
        let block_probability = diff_info.block_chance;

        share_accepted = false;
        reward = 0f32;
        if let Some(response) = conn.miner_stat.reject_shares.take() {
            conn.write(response).await?;

            conn.miner_stat.rejected_shares += 1;
        } else if hashrate < min_hashrate as f32 {
            conn.miner_stat.override_diff =
                CompactString::from(kolka::kolka::v2_reverse(req_difficulty.as_str()));
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too high starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
        } else if hashrate >= max_hashrate as f32 {
            conn.miner_stat.override_diff =
                CompactString::from(kolka::kolka::v2(req_difficulty.as_str()));
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too low starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
        } else if nonce == random {
            conn.miner_stat.accepted_shares += 1;
            if conn.miner_stat.accepted_shares > 10 {
                // why is this a thing?
                if diff <= ESP8266NH_DIFF_CACHE.load(Relaxed) {
                    #[allow(clippy::if_same_then_else)]
                    if conn.miner_stat.this_miner_chipid.is_empty() {
                        conn.miner_stat.rejected_shares += 1;
                    } else if data_len >= 5 && data[4] != conn.miner_stat.this_miner_chipid.as_str()
                    {
                        conn.miner_stat.rejected_shares += 1;
                    } else {
                        reward = kolka::kolka::v1(
                            kolka,
                            hashrate_calc,
                            diff,
                            // conn.miner_stat.amount_of_workers,
                            conn.miner_stat.worker_slot.id(),
                            reward_div,
                        );
                    }
                } else {
                    reward = kolka::kolka::v1(
                        kolka,
                        hashrate_calc,
                        diff,
                        // conn.miner_stat.amount_of_workers,
                        conn.miner_stat.worker_slot.id(),
                        reward_div,
                    );
                }
            }

            // if (rand::rng().random_range(0f32..1f32) * block_probability as f32).floor() == 1f32 {
            if rng().random_range(0u32..block_probability) == 0 {
                reward += config.block_reward;

                let block_infos = BlockInfos::new(
                    answer_received.duration_since(UNIX_EPOCH)?.as_secs(),
                    conn.miner_stat.username.clone(),
                    reward,
                    CompactString::const_new("DUCO-S1"),
                    new_hash_str.to_string(),
                );
                GLOBAL_BLOCKS.lock().await.push(block_infos);
                conn.write("BLOCK\n").await?;
                info!("Block found by {}", conn.miner_stat.username);
                share_accepted = true;
            } else {
                conn.write("GOOD\n").await?;
                share_accepted = true;
            }
        } else {
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Incorrect result\n").await?;
        }

        if share_accepted {
            // only clone the username key when the entry doesn't exist yet
            match BALANCES_TO_UPDATE.get_mut(&conn.miner_stat.username) {
                Some(mut balance) => *balance += reward,
                None => {
                    *BALANCES_TO_UPDATE
                        .entry(conn.miner_stat.username.clone())
                        .or_insert(0f32) += reward
                }
            }

            GLOBAL_SHARES_INCREASE.fetch_add(
                // config.update_miners_stats_every as u64,
                1u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if conn.miner_stat.accepted_shares > 0
            && conn
                .miner_stat
                .accepted_shares
                .is_multiple_of(config.update_miners_stats_every as u64)
        {
            let (rig_part, key_part) = if data_len >= 4 {
                match data[3].split_once(':') {
                    Some((r, k)) => (r, Some(k)),
                    None => (data[3], None),
                }
            } else {
                ("", None)
            };

            // the only fallible step, done before the published entry is touched
            if let Some(key) = key_part {
                // mining key backwards compatibility for old miners
                if !encode_password(key, &mut conn.miner_stat.password) {
                    return reject_password(conn).await;
                }
            }

            let published = Published {
                software: if data_len >= 3 { Some(data[2]) } else { None },
                rig: rig_part,
                wallet_id: if data_len >= 6 { data[5] } else { "None" },
                hashrate: if hashrate_is_estimated {
                    hashrate
                } else {
                    reported_hashrate
                },
                difficulty: diff,
                reward,
            };

            // one guard for all fields; nothing below fails or awaits
            match MINER_STATS.get_mut(&conn.id) {
                Some(mut e) => {
                    e.update_from(&conn.miner_stat);
                    published.write_into(&mut e);
                }
                None => {
                    let mut stat = conn.miner_stat.convert_slim();
                    published.write_into(&mut stat);
                    MINER_STATS.insert(conn.id, stat);
                }
            }

            LAST_BLOCK_HASH.store(Arc::new(new_hash_str.to_string()));
        }

        is_first_share = false;
        // because feedback_sent is only used to calculate the ping, we want to set it as late as possible
        feedback_sent = SystemTime::now();
    }
}

// fields that only exist for the published stats, written straight into the MINER_STATS entry
struct Published<'a> {
    software: Option<&'a str>, // raw field, None if the answer had no software field
    rig: &'a str,              // raw rig part (before the legacy ':key')
    wallet_id: &'a str,
    hashrate: f32,
    difficulty: u32,
    reward: f32,
}

impl Published<'_> {
    fn write_into(&self, stat: &mut MinerStatSlim) {
        match self.software {
            Some(software) => {
                clean_name_into(software, &mut stat.software);
                if stat.software.is_empty() {
                    stat.software.push_str("Unknown miner");
                }
            }
            None => {
                stat.software.clear();
                stat.software.push_str("Unknown miner");
            }
        }

        stat.wallet_id.clear();
        stat.wallet_id
            .push_str(self.wallet_id.trim_end_matches("\n"));

        clean_name_into(self.rig, &mut stat.id);
        if stat.id.is_empty() {
            stat.id.push_str("None");
        }

        stat.hashrate = self.hashrate;
        stat.difficulty = self.difficulty;
        stat.reward = self.reward;
    }
}

// false (and `out` untouched) if `raw` is longer than MAX_PASSWORD_BYTES
fn encode_password(raw: &str, out: &mut CompactString) -> bool {
    if raw.len() > MAX_PASSWORD_BYTES {
        return false;
    }
    let mut buf = [0u8; PASSWORD_B64_BYTES];
    // can't fail, the length was checked above
    let len = BASE64_STANDARD
        .encode_slice(raw.as_bytes(), &mut buf)
        .unwrap();
    out.clear();
    // base64 output is always valid utf-8
    out.push_str(std::str::from_utf8(&buf[..len]).unwrap());
    true
}

// explicit rejection instead of silently dropping the connection
async fn reject_password(conn: &mut Connection) -> Result<(), anyhow::Error> {
    conn.write("BAD,Password too long\n").await?;
    // used so we can handle closes centrally
    Err(anyhow::Error::from_boxed(Box::from("Shutting down.")))
}

fn perc_diff(hashrate: f32, hashrate_calc: f32) -> f32 {
    (100f32 * ((hashrate - hashrate_calc) / ((hashrate + hashrate_calc) / 2f32)).abs()).floor()
}

fn is_pc_diff_tier(req_difficulty: &str) -> bool {
    matches!(req_difficulty, "EXTREME" | "NET" | "MEDIUM" | "LOW")
}

fn set_req_difficulty(out: &mut CompactString, value: &str) -> bool {
    if value.len() > 16 {
        out.clear();
        return false;
    }

    out.clear();
    out.push_str(value);
    true
}

fn check_workers(max: f32, config: &PoolConfig) -> bool {
    // u64 can't be lower than zero
    if config.max_workers == 0 {
        return false;
    }

    if max > config.max_workers as f32 {
        return true;
    }
    false
}

const fn build_allowed() -> [bool; 128] {
    let mut table = [false; 128];
    table[b'.' as usize] = true;
    table[b'(' as usize] = true;
    table[b')' as usize] = true;
    table[b'-' as usize] = true;
    table[b' ' as usize] = true;
    table
}

const fn build_allowed_iot() -> [bool; 128] {
    let mut table = [false; 128];
    table[b'.' as usize] = true;
    table[b'(' as usize] = true;
    table[b')' as usize] = true;
    table[b':' as usize] = true;
    table[b'@' as usize] = true;
    table[b'-' as usize] = true;
    table[b' ' as usize] = true;
    table
}

const ALLOWED_TABLE: [bool; 128] = build_allowed();
const ALLOWED_IOT_TABLE: [bool; 128] = build_allowed_iot();

fn clean_name_into(input: &str, out: &mut CompactString) {
    out.clear();

    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || (b < 128 && ALLOWED_TABLE[b as usize]) {
            out.push(char::from(b));
        }
    }
}

fn clean_iot_into(input: &str, out: &mut CompactString) {
    out.clear();

    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || (b < 128 && ALLOWED_IOT_TABLE[b as usize]) {
            out.push(char::from(b));
        }
    }
}

// split s by commas into dst, missing fields are ""
pub(crate) fn scan_fields<'a>(s: &'a str, dst: &mut [&'a str; 8]) -> usize {
    let bytes = s.as_bytes();

    let mut start = 0;
    let mut field = 0;

    for i in 0..bytes.len() {
        if bytes[i] == b',' {
            dst[field] = &s[start..i];
            field += 1;

            if field == dst.len() {
                return field;
            }

            start = i + 1;
        }
    }

    if field < dst.len() {
        dst[field] = &s[start..];
        field += 1;
    }

    field
}

fn resolve_reward_info(
    pool_rewards: &DashMap<String, Difficulty>,
    req_difficulty: &str,
    override_difficulty: &str,
) -> Option<Difficulty> {
    if let Some(info) = pool_rewards.get(req_difficulty).map(|r| *r) {
        return Some(info);
    }

    if !override_difficulty.is_empty() {
        if let Some(info) = pool_rewards.get(override_difficulty).map(|r| *r) {
            return Some(info);
        }
    }

    pool_rewards.get("NET").map(|r| *r)
}

#[cfg(test)]
mod tests {
    use super::scan_fields;

    #[test]
    fn scan_fields_handles_empty_and_trailing_fields() {
        let mut out = [""; 8];
        let n = scan_fields("a,,c,\n", &mut out);
        assert_eq!(n, 4);
        assert_eq!(out[0], "a");
        assert_eq!(out[1], "");
        assert_eq!(out[2], "c");
        assert_eq!(out[3], "\n");
    }

    #[test]
    fn scan_fields_caps_at_destination_len() {
        let mut out = [""; 8];
        let n = scan_fields("0,1,2,3,4,5,6,7,8,9", &mut out);
        assert_eq!(n, 8);
        assert_eq!(out[0], "0");
        assert_eq!(out[7], "7");
    }
}
