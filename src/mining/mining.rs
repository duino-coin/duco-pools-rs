use crate::cfg::config::PoolConfig;
use crate::data::data::{ConnId, Connection, ConnectionClosed};
use crate::mining::block_hash::BlockHash;
use crate::mining::id_allocator::WorkerAllocator;
use crate::mining::stats::{MinerStat, MinerStatSlim};
use crate::{kolka, CONFIG, CURVE_VALUES};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use compact_str::CompactString;
use dashmap::DashMap;
use log::{debug, info};
use rand::{rng, RngExt};
use sha1::Digest;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;
use std::time::{Instant, UNIX_EPOCH};

pub static USR_WORKERS_ALLOC: LazyLock<DashMap<CompactString, WorkerAllocator>> =
    LazyLock::new(DashMap::new);

pub static BALANCES_TO_UPDATE: LazyLock<DashMap<CompactString, f32>> = LazyLock::new(DashMap::new);

pub static MINER_STATS: LazyLock<DashMap<ConnId, MinerStatSlim>> = LazyLock::new(DashMap::new);

static LAST_BLOCK_HASH: LazyLock<BlockHash> = LazyLock::new(|| {
    let config = CONFIG.get().expect("config not initialised");
    // validate_config made sure it's 40 hex chars
    BlockHash::new(config.initial_blockhash.as_bytes().try_into().unwrap())
});

pub static GLOBAL_SHARES_INCREASE: AtomicU64 = AtomicU64::new(0);

// longest password (or legacy mining key) accepted, its base64 form fits PASSWORD_B64_BYTES
const MAX_PASSWORD_BYTES: usize = 96;
const PASSWORD_B64_BYTES: usize = MAX_PASSWORD_BYTES.div_ceil(3) * 4;

// weight of the newest share in the hashrate average, ~ the last 9 shares
const HASHRATE_EMA_WEIGHT: f32 = 0.2;
// keeps the hashrate finite if a share is answered within the timer resolution
const MIN_SHARE_TIME: f32 = 0.001;
// accepted shares without a regular reward, the hashrate estimate settles meanwhile
const WARMUP_SHARES: u64 = 10;

// just the socket side of a session: reads, writes and their timing; everything in between
// lives in `Session`, which never awaits, so none of it bloats the connection's future
pub async fn handle(
    conn: &mut Connection,
    mut read: String, // holds the first JOB request, then reused for every job/answer line
    config: &PoolConfig,
) -> Result<(), anyhow::Error> {
    let mut session = Session::new();
    let mut feedback_sent = Instant::now();

    loop {
        if !session.is_first_share {
            conn.read_to_string(&mut read, feedback_sent).await?;

            conn.miner_stat.ping = feedback_sent.elapsed().as_secs_f32();
        }
        // the handler already validated the first line; scan it the same way (trimmed)
        let job_request = if session.is_first_share {
            read.trim()
        } else {
            read.as_str()
        };
        if let Err(close) =
            session.on_job_request(conn.id, &mut conn.miner_stat, job_request, config)
        {
            return close.send(conn).await;
        }

        // the request fields aren't needed anymore, so `read` is free for the job line
        session.write_job(&mut read);
        conn.write(&read).await?;

        let job_sent = Instant::now();
        conn.read_to_string(&mut read, job_sent).await?;

        let answer_received = UNIX_EPOCH.elapsed()?.as_secs();
        conn.miner_stat.share_time = job_sent.elapsed().as_secs_f32().max(MIN_SHARE_TIME);

        let judged = match session.judge(
            conn.id,
            &mut conn.miner_stat,
            &read,
            answer_received,
            config,
        ) {
            Ok(judged) => judged,
            Err(close) => return close.send(conn).await,
        };
        conn.write(judged.feedback).await?;

        // only once the miner got its feedback, a failed write leaves the share uncredited
        if let Err(close) = session.commit(conn.id, &mut conn.miner_stat, &read, &judged, config) {
            return close.send(conn).await;
        }
        // because feedback_sent is only used to calculate the ping, we want to set it as late as possible
        feedback_sent = Instant::now();
    }
}

// the mining state of one connection that lives across jobs
struct Session {
    random: u64,
    hex_buf: [u8; 40], // hex of the current job's hash
    // averages of the nonce and the job to answer time of accepted shares; their ratio is the
    // hashrate we measure ourselves, the hashrate the miner reports doesn't go into it;
    // the nonce is the hash count of a miner scanning up from 0, miners splitting the range over
    // threads get there with fewer hashes per share, but that evens out on average;
    // the time includes a round trip, so the result comes out a bit low
    avg_hashes: f32,
    avg_share_time: f32,
    diff: u32,
    is_first_share: bool,
}

// what an answer earned, `commit` applies it once the feedback was sent
struct Judged {
    feedback: &'static str,
    reward: f32,
    share_accepted: bool,
    block_found: bool,
    hashrate: f32, // floored at the calculated one, steers the next difficulty
    published_hashrate: f32,
    invalid_hashrate: bool,
    answer_received: u64, // unix secs
}

// a last reply, the connection is closed once it's sent
struct Close(&'static str);

impl Close {
    async fn send(self, conn: &mut Connection) -> Result<(), anyhow::Error> {
        conn.write(self.0).await?;
        // used so we can handle closes centrally
        Err(ConnectionClosed.into())
    }
}

// explicit rejection instead of silently dropping the connection
const PASSWORD_TOO_LONG: Close = Close("BAD,Password too long\n");

impl Session {
    fn new() -> Self {
        Session {
            random: 0,
            hex_buf: [0u8; 40],
            avg_hashes: 0f32,
            avg_share_time: 0f32,
            diff: 1,
            is_first_share: true,
        }
    }

    fn on_job_request(
        &mut self,
        id: ConnId,
        stat: &mut MinerStat,
        job_request: &str,
        config: &PoolConfig,
    ) -> Result<(), Close> {
        let mut data: [&str; 8] = [""; 8];
        let data_len = scan_fields(job_request, &mut data);

        if self.is_first_share {
            if data_len >= 3 && data[2] == "ESP32" {
                stat.is_req_diff_esp32 = true;
            }

            let mut alloc = USR_WORKERS_ALLOC.entry(stat.username.clone()).or_default();
            let kolka_id = alloc.allocate(stat.is_req_diff_esp32);

            stat.worker_slot = kolka_id;

            debug!("{}, Assigned {}", id, stat.worker_slot.id());
        }

        if data_len >= 4 {
            if !encode_password(data[3], &mut stat.password) {
                return Err(PASSWORD_TOO_LONG);
            }
        } else {
            stat.password.clear();
        }

        // IoT reading stuff
        if data_len >= 5 {
            if data[4].len() > 48 {
                stat.iot_reading = CompactString::const_new("Error:data too long");
            } else {
                clean_iot_into(data[4], &mut stat.iot_reading);
            }
        }

        let (user_workers, new_id) = USR_WORKERS_ALLOC
            .get_mut(&stat.username)
            .map(|mut alloc| {
                let workers = alloc.active_workers();

                let new_id = alloc.try_recycle(stat.worker_slot);
                (workers, new_id)
            })
            .unwrap_or((0.0, None));

        if check_workers(user_workers, config) {
            stat.reject_shares = Some("BAD,Too many workers\n");
        }
        if let Some(id) = new_id {
            stat.worker_slot = id;
        }
        Ok(())
    }

    // writes "<last hash>,<new hash>,<diff>\n" into `out`
    fn write_job(&mut self, out: &mut String) {
        if self.is_first_share {
            self.diff = 100;
        }

        self.random = rand::rng().random_range(0u64..self.diff as u64 * 100u64) + 1;

        #[cfg(feature = "bench")]
        {
            std::hint::black_box(self.random);
            self.random = 1;
        }

        let last_block_hash = LAST_BLOCK_HASH.load();
        let mut hasher = sha1::Sha1::new_with_prefix(last_block_hash);

        hasher.update(itoa::Buffer::new().format(self.random).as_bytes());

        // can't fail, the buffer is exactly twice the digest length
        hex::encode_to_slice(hasher.finalize(), &mut self.hex_buf).unwrap();

        out.clear();
        // only ever holds hex, which is valid utf-8
        out.push_str(std::str::from_utf8(&last_block_hash).unwrap());
        out.push(',');
        out.push_str(self.new_hash_str());
        out.push(',');
        out.push_str(itoa::Buffer::new().format(self.diff));
        out.push('\n');
    }

    fn new_hash_str(&self) -> &str {
        // hex output is always valid utf-8
        std::str::from_utf8(&self.hex_buf).unwrap()
    }

    // expects the miner's share_time to be set already
    fn judge(
        &mut self,
        id: ConnId,
        stat: &mut MinerStat,
        answer: &str,
        answer_received: u64,
        config: &PoolConfig,
    ) -> Result<Judged, Close> {
        let mut data: [&str; 8] = [""; 8];
        let data_len = scan_fields(answer, &mut data);

        // a missing field is "", which doesn't parse either
        let reported = data[1].parse::<f32>().ok();
        let invalid_hashrate = reported.is_some_and(|rep| rep <= 0f32 || !rep.is_finite());
        if invalid_hashrate {
            stat.reject_shares = Some("BAD,Invalid data\n");
        }

        // saturating: a client flooding answers within one minute must not wrap past the limit
        stat.last_min_shares = stat.last_min_shares.saturating_add(1);

        let nonce = match u64::from_str(data[0]) {
            Ok(ok) => ok,
            Err(err) => {
                debug!("Nonce error: {err:?} ({:?})", data);
                return Err(Close("BAD,No response to job\n"));
                // (some request a job and after receiving the job they immediately request another one)
            }
        };

        // only correct answers are real work, fast wrong ones would inflate the average
        if nonce == self.random {
            // the first one seeds the averages
            let weight = if self.avg_share_time > 0f32 {
                HASHRATE_EMA_WEIGHT
            } else {
                1f32
            };
            self.avg_hashes += (self.random as f32 - self.avg_hashes) * weight;
            self.avg_share_time += (stat.share_time - self.avg_share_time) * weight;
        }
        let hashrate_calc = if self.avg_share_time > 0f32 {
            self.avg_hashes / self.avg_share_time
        } else {
            self.random as f32 / stat.share_time
        };

        // an ESP32 only counts as half a worker, so a miner that measures faster than a
        // microcontroller after the warmup is moved to a full worker slot.
        // the curve is only loaded where it's needed and never kept across an await: a held
        // guard occupies one of arc-swap's few per-thread slots while the miner is idle
        if stat.is_req_diff_esp32
            && stat.accepted_shares >= WARMUP_SHARES
            && hashrate_calc as f64 > CURVE_VALUES.load().mcu_pc_step_h
        {
            if let Some(mut alloc) = USR_WORKERS_ALLOC.get_mut(&stat.username) {
                alloc.release(stat.worker_slot, true);
                stat.worker_slot = alloc.allocate(false);
            }
            stat.is_req_diff_esp32 = false;
            debug!("{}, Downgraded ESP32 to {}", id, stat.worker_slot.id());
        }

        // an invalid report (<= 0, inf, NaN) isn't published, inf and NaN would end up as null in the json
        let published_hashrate = match reported {
            Some(reported) if !invalid_hashrate => reported,
            _ => hashrate_calc,
        };
        // a fast miner underreporting its hashrate solves every job far below the expected share time,
        // so it would keep its low difficulty without this floor. overreporting doesn't benefit it
        let hashrate = published_hashrate.max(hashrate_calc);

        // abs_diff: a wall clock that stepped back must neither underflow nor stall the reset
        if answer_received.abs_diff(stat.last_share_reset) >= 60 {
            stat.last_share_reset = answer_received;
            stat.last_min_shares = 0;
        }
        #[cfg(not(feature = "bench"))]
        // if the pool was built with the bench feature enabled, we discard this codeblock
        {
            if stat.last_min_shares > config.max_shares_per_minute {
                stat.reject_shares = Some("BAD,Too low difficulty\n");
            }
        }

        if self.is_first_share {
            stat.this_miner_chipid.clear();
            if data_len >= 5 {
                stat.this_miner_chipid.push_str(data[4]);
            }
        }

        let mut share_accepted = false;
        let mut block_found = false;
        let mut reward = 0f32;
        let feedback = if let Some(response) = stat.reject_shares.take() {
            stat.rejected_shares += 1;
            response
        } else if nonce == self.random {
            stat.accepted_shares += 1;
            // one load for the whole share, not held across an await
            let curve = CURVE_VALUES.load();
            if stat.accepted_shares > WARMUP_SHARES {
                // why is this a thing?
                // the difficulty directly correlates with the hashrate, so we can check the device type this way
                let chipid_mismatch = hashrate_calc as f64 <= curve.mcu_pc_step_h
                    && (stat.this_miner_chipid.is_empty()
                        || (data_len >= 5 && data[4] != stat.this_miner_chipid.as_str()));
                if chipid_mismatch {
                    stat.rejected_shares += 1;
                } else {
                    reward = kolka::kolka::calculate_reward(
                        hashrate_calc.max(1f32),
                        stat.worker_slot.id(),
                        self.diff,
                        &curve,
                    );
                }
            }

            share_accepted = true;
            if rng().random_range(0u32..curve.block_probability) == 0 {
                reward += curve.block_reward;
                block_found = true;
                "BLOCK\n"
            } else {
                "GOOD\n"
            }
        } else {
            stat.rejected_shares += 1;
            "BAD,Incorrect result\n"
        };

        Ok(Judged {
            feedback,
            reward,
            share_accepted,
            block_found,
            hashrate,
            published_hashrate,
            invalid_hashrate,
            answer_received,
        })
    }

    fn commit(
        &mut self,
        id: ConnId,
        stat: &mut MinerStat,
        answer: &str,
        judged: &Judged,
        config: &PoolConfig,
    ) -> Result<(), Close> {
        if judged.block_found {
            info!("Block found by {}", stat.username);
        }

        if judged.share_accepted {
            // only clone the username key when the entry doesn't exist yet
            match BALANCES_TO_UPDATE.get_mut(&stat.username) {
                Some(mut balance) => *balance += judged.reward,
                None => {
                    *BALANCES_TO_UPDATE
                        .entry(stat.username.clone())
                        .or_insert(0f32) += judged.reward
                }
            }

            GLOBAL_SHARES_INCREASE.fetch_add(1u64, std::sync::atomic::Ordering::Relaxed);
        }
        if stat.accepted_shares > 0
            && stat
                .accepted_shares
                .is_multiple_of(config.update_miners_stats_every as u64)
        {
            // judge scanned it too, but its fields aren't kept while the feedback is written
            let mut data: [&str; 8] = [""; 8];
            let data_len = scan_fields(answer, &mut data);

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
                if !encode_password(key, &mut stat.password) {
                    return Err(PASSWORD_TOO_LONG);
                }
            }

            let wallet_id = if data_len >= 6 { data[5] } else { "None" };
            {
                // one guard for all fields; nothing below fails
                let mut e = MINER_STATS.entry(id).or_default();
                e.update_from(stat);

                if data_len >= 3 {
                    clean_name_into(data[2], &mut e.software);
                } else {
                    e.software.clear();
                }
                if e.software.is_empty() {
                    e.software.push_str("Unknown miner");
                }

                e.wallet_id.clear();
                e.wallet_id.push_str(wallet_id.trim_end_matches("\n"));

                clean_name_into(rig_part, &mut e.id);
                if e.id.is_empty() {
                    e.id.push_str("None");
                }

                e.hashrate = judged.published_hashrate;
                e.difficulty = self.diff;
                e.reward = judged.reward;
                e.timestamp = judged.answer_received as u32; // u32 lasts until 2106
            }

            LAST_BLOCK_HASH.store(&self.hex_buf);
        }

        // after the reward and stats, so both use the difficulty of the job that was just solved
        // a rejected hashrate must not steer the next job (inf would make it unsolvable)
        if !judged.invalid_hashrate {
            self.diff =
                kolka::kolka::calculate_difficulty(judged.hashrate as f64, &CURVE_VALUES.load());
        }

        self.is_first_share = false;
        Ok(())
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

fn check_workers(max: f32, config: &PoolConfig) -> bool {
    // 0 disables the limit
    config.max_workers != 0 && max > config.max_workers as f32
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
