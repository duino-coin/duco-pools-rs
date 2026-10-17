use crate::cfg::config::PoolConfig;
use crate::data::data::{ConnId, Connection};
use crate::mining::id_allocator::WorkerAllocator;
use crate::mining::stats::MinerStatSlim;
use crate::{kolka, CONFIG, CURVE_VALUES};
use arc_swap::ArcSwap;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use compact_str::CompactString;
use dashmap::DashMap;
use log::{debug, info};
use rand::{rng, RngExt};
use sha1::Digest;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
// I hate that these are pub static, but I honestly can't be arsed to pass them as params..

pub static USR_WORKERS_ALLOC: LazyLock<DashMap<CompactString, WorkerAllocator>> =
    LazyLock::new(DashMap::new);

pub static BALANCES_TO_UPDATE: LazyLock<DashMap<CompactString, f32>> = LazyLock::new(DashMap::new);

pub static MINER_STATS: LazyLock<DashMap<ConnId, MinerStatSlim>> = LazyLock::new(DashMap::new);

static LAST_BLOCK_HASH: LazyLock<ArcSwap<String>> = LazyLock::new(|| {
    let config = CONFIG.get().expect("config not initialised");
    ArcSwap::from_pointee(config.initial_blockhash.clone())
});

pub static GLOBAL_SHARES_INCREASE: AtomicU64 = AtomicU64::new(0);

// longest password (or legacy mining key) accepted, its base64 form fits PASSWORD_B64_BYTES
const MAX_PASSWORD_BYTES: usize = 96;
const PASSWORD_B64_BYTES: usize = MAX_PASSWORD_BYTES.div_ceil(3) * 4;

// weight of the newest share in the hashrate average, ~ the last 9 shares
const HASHRATE_EMA_WEIGHT: f32 = 0.2;
// keeps the hashrate finite if the round trip estimate covers the whole share time
const MIN_SHARE_TIME: f32 = 0.001;
// accepted shares without a regular reward, the hashrate estimate settles meanwhile
const WARMUP_SHARES: u64 = 10;

// every local that lives across an .await is stored in the connection's future,
// so scratch buffers are kept in the sync blocks/functions that use them
pub async fn handle(
    conn: &mut Connection,
    mut read: String, // holds the first JOB request, then reused for every job/answer line
    config: &PoolConfig,
) -> Result<(), anyhow::Error> {
    let mut random;
    let mut hex_buf = [0u8; 40]; // stack-allocated, reused every iteration
    let mut feedback_sent = Instant::now();
    // smallest gap between our feedback and the next request, the gap is a round trip plus
    // the miner's idle time, so its minimum is close to the round trip alone.
    // it never grows: delaying requests can't shrink the measured share time
    let mut rtt: Option<f32> = None;
    // averages of the hashes and seconds per accepted share, their ratio is the hashrate
    let mut avg_hashes = 0f32;
    let mut avg_share_time = 0f32;
    let mut diff = 1u32;
    let mut reported_hashrate;
    let mut hashrate_calc: f32;
    let mut reward: f32;
    let mut hashrate_is_estimated;
    let mut hashrate: f32;
    let mut is_first_share = true;

    let mut data: [&str; 8] = [""; 8];
    let mut data_len: usize;

    // the handler already validated this line; scan it the same way (trimmed)
    data_len = scan_fields(read.trim(), &mut data);

    let mut share_accepted: bool;

    loop {
        // set when the requested tier was already looked up for this share
        if is_first_share {
            if data_len >= 3 && data[2] == "ESP32" {
                conn.miner_stat.is_req_diff_esp32 = true;
            }

            let mut alloc = USR_WORKERS_ALLOC
                .entry(conn.miner_stat.username.clone())
                .or_default();
            let kolka_id = alloc.allocate(conn.miner_stat.is_req_diff_esp32);

            conn.miner_stat.worker_slot = kolka_id;

            debug!("{}, Assigned {}", conn.id, conn.miner_stat.worker_slot.id());
        } else {
            data = [""; 8];
            conn.read_to_string(&mut read).await?;

            conn.miner_stat.ping = feedback_sent.elapsed().as_secs_f32();
            let ping = conn.miner_stat.ping;
            rtt = Some(rtt.map_or(ping, |rtt| rtt.min(ping)));

            data_len = scan_fields(&read, &mut data);
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

        let (user_workers, new_id) = USR_WORKERS_ALLOC
            .get_mut(&conn.miner_stat.username)
            .map(|mut alloc| {
                let workers = alloc.active_workers();

                let new_id = alloc.try_recycle(conn.miner_stat.worker_slot);
                (workers, new_id)
            })
            .unwrap_or((0.0, None));

        if check_workers(user_workers, config) {
            conn.miner_stat.reject_shares = Some("BAD,Too many workers\n");
        }
        if let Some(id) = new_id {
            conn.miner_stat.worker_slot = id;
        }

        if is_first_share {
            diff = 100;
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

        let job_sent = Instant::now();
        conn.read_to_string(&mut read).await?;

        data_len = scan_fields(&read, &mut data);
        let answer_received = SystemTime::now();

        conn.miner_stat.share_time =
            (job_sent.elapsed().as_secs_f32() - rtt.unwrap_or(0f32)).max(MIN_SHARE_TIME);

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

        let invalid_hashrate = (reported_hashrate <= 0f32 && !wrong_reported_hashrate)
            || reported_hashrate.is_nan()
            || reported_hashrate.is_infinite();
        if invalid_hashrate {
            conn.miner_stat.reject_shares = Some("BAD,Invalid data\n");
        }

        // saturating: a client flooding answers within one minute must not wrap past the limit
        conn.miner_stat.last_min_shares = conn.miner_stat.last_min_shares.saturating_add(1);

        let nonce = match u64::from_str(data[0]) {
            Ok(ok) => ok,
            Err(err) => {
                debug!("Nonce error: {err:?} ({:?})", data);
                conn.write("BAD,No response to job\n").await?;
                return Err(anyhow::Error::from_boxed(Box::from("Shutting down.")));
                // used so we can handle closes centrally
                // (some request a job and after receiving the job they immediately request another one)
            }
        };

        // only correct answers are real work, fast wrong ones would inflate the average
        if nonce == random {
            // the first one seeds the averages
            let weight = if avg_share_time > 0f32 {
                HASHRATE_EMA_WEIGHT
            } else {
                1f32
            };
            avg_hashes += (random as f32 - avg_hashes) * weight;
            avg_share_time += (conn.miner_stat.share_time - avg_share_time) * weight;
        }
        hashrate_calc = if avg_share_time > 0f32 {
            avg_hashes / avg_share_time
        } else {
            random as f32 / conn.miner_stat.share_time
        };

        // an ESP32 only counts as half a worker, so a miner that measures faster than a
        // microcontroller after the warmup is moved to a full worker slot.
        // the curve is loaded at each use and never kept across an await: a held guard
        // occupies one of arc-swap's few per-thread slots while the miner is idle
        if conn.miner_stat.is_req_diff_esp32
            && conn.miner_stat.accepted_shares >= WARMUP_SHARES
            && hashrate_calc as f64 > CURVE_VALUES.load().mcu_pc_step_h
        {
            if let Some(mut alloc) = USR_WORKERS_ALLOC.get_mut(&conn.miner_stat.username) {
                alloc.release(conn.miner_stat.worker_slot, true);
                conn.miner_stat.worker_slot = alloc.allocate(false);
            }
            conn.miner_stat.is_req_diff_esp32 = false;
            debug!("{}, Downgraded ESP32 to {}", conn.id, conn.miner_stat.worker_slot.id());
        }

        if wrong_reported_hashrate {
            reported_hashrate = hashrate_calc;
        }

        {
            let now_secs = UNIX_EPOCH.elapsed()?.as_secs();
            // abs_diff: a wall clock that stepped back must neither underflow nor stall the reset
            if now_secs.abs_diff(conn.miner_stat.last_share_reset) >= 60 {
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

        // TODO: determine if needed
        // if diff > DUE_DIFF_CACHE.load(Relaxed)
        //     && diff <= ESP8266NH_DIFF_CACHE.load(Relaxed)
        //     && hashrate_perc_diff_for_calc > 15f32
        //     && hashrate < hashrate_calc // only underreporting keeps a lower difficulty (overreporting the hashrate doesn't benefit the user)
        //     && conn.miner_stat.share_time >= 1.5
        // {
        //     conn.miner_stat.reject_shares = Some("BAD,Modified hashrate\n");
        // }

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

        share_accepted = false;
        reward = 0f32;
        if let Some(response) = conn.miner_stat.reject_shares.take() {
            conn.write(response).await?;

            conn.miner_stat.rejected_shares += 1;
        } else if nonce == random {
            conn.miner_stat.accepted_shares += 1;
            if conn.miner_stat.accepted_shares > WARMUP_SHARES {
                // why is this a thing?
                // the difficulty directly correlates with the hashrate, so we can check the device type this way
                if hashrate_calc as f64 <= CURVE_VALUES.load().mcu_pc_step_h {
                    #[allow(clippy::if_same_then_else)]
                    if conn.miner_stat.this_miner_chipid.is_empty() {
                        conn.miner_stat.rejected_shares += 1;
                    } else if data_len >= 5 && data[4] != conn.miner_stat.this_miner_chipid.as_str()
                    {
                        conn.miner_stat.rejected_shares += 1;
                    } else {
                        reward = kolka::kolka::calculate_reward(hashrate_calc.max(1f32), conn.miner_stat.worker_slot.id(), diff, &CURVE_VALUES.load());
                    }
                } else {
                    reward = kolka::kolka::calculate_reward(hashrate_calc.max(1f32), conn.miner_stat.worker_slot.id(), diff, &CURVE_VALUES.load());
                }
            }

            if rng().random_range(0u32..CURVE_VALUES.load().block_probability) == 0 {
                reward += config.block_reward;

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
                timestamp: answer_received.duration_since(UNIX_EPOCH)?.as_secs() as u32,
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

        // after the reward and stats, so both use the difficulty of the job that was just solved
        // a rejected hashrate must not steer the next job (inf would make it unsolvable)
        if !invalid_hashrate {
            diff = kolka::kolka::calculate_difficulty(hashrate as f64, &CURVE_VALUES.load());
        }

        is_first_share = false;
        // because feedback_sent is only used to calculate the ping, we want to set it as late as possible
        feedback_sent = Instant::now();
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
    timestamp: u32, // unix secs of the last accepted share, u32 lasts until 2106
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
        stat.timestamp = self.timestamp;
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
