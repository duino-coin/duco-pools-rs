use crate::cfg::config::PoolConfig;
use crate::data::data::Connection;
use crate::mining::id_allocator::WorkerAllocator;
use crate::mining::stats::{BlockInfos, MinerStatSlim};
use crate::sync::sync::Difficulty;
use crate::{cfg, kolka, KOLKA, POOL_REWARDS};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use compact_str::CompactString;
use dashmap::DashMap;
use log::{debug, info};
use rand::{rng, Rng};
use sha1_smol::Sha1;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};
use arc_swap::ArcSwap;
use tokio::sync::Mutex;
// I hate that these are pub static, but I honestly can't be arsed to pass them as params..

pub static USR_WORKERS_ALLOC: LazyLock<DashMap<CompactString, WorkerAllocator>> = LazyLock::new(|| DashMap::new());

pub static BALANCES_TO_UPDATE: LazyLock<DashMap<CompactString, f32>> = LazyLock::new(|| DashMap::new());

pub static MINER_STATS: LazyLock<DashMap<Box<str>, MinerStatSlim>> = LazyLock::new(|| DashMap::new());

static LAST_BLOCK_HASH: LazyLock<ArcSwap<String>> = LazyLock::new(|| {
    let config = cfg::config::load_config().expect("failed to load config (static)");
    ArcSwap::from_pointee(config.initial_blockhash)
});

pub static GLOBAL_BLOCKS: LazyLock<Mutex<Vec<BlockInfos>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

pub static GLOBAL_SHARES_INCREASE: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

pub static GLOBAL_SHARES_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

pub static ESP8266NH_DIFF_CACHE: LazyLock<AtomicU32> = LazyLock::new(|| AtomicU32::new(0));

pub static DUE_DIFF_CACHE: LazyLock<AtomicU32> = LazyLock::new(|| AtomicU32::new(0));

const PC_DIFF_TIERS: [&str; 4] = ["EXTREME", "NET", "MEDIUM", "LOW"];

pub async fn handle(
    conn: &mut Connection,
    initial_data: Box<[&str]>,
    config: &PoolConfig,
    using_avr: bool,
) -> Result<(), anyhow::Error> {
    let mut random;
    let mut hasher = Sha1::new();
    let mut hex_buf = [0u8; 40]; // stack-allocated, reused every iteration
    let mut password_buf = [0u8; 128];
    let mut req_difficulty = CompactString::with_capacity(16);
    let mut this_miner_chipid = CompactString::default();
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
    let mut buffer = itoa::Buffer::new(); // used to convert nums to str a little faster
    let mut data: [&str; 8] = [""; 8];
    let mut data_len: usize;
    let mut read = String::with_capacity(32); // needed for first share..

    data_len = initial_data.len().min(8);
    data[..data_len].copy_from_slice(&initial_data[..data_len]);
    drop(initial_data);

    let mut job_str = String::with_capacity(32);
    let mut msg = String::with_capacity(23);

    loop {
        if is_first_share {
            req_difficulty.clear();
            if data_len < 3 || data[2].is_empty() {
                req_difficulty.push_str("NET");
            } else {
                if !set_req_difficulty(&mut req_difficulty, data[2]) {
                    req_difficulty.push_str("NET");
                }
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
            let mut alloc = USR_WORKERS_ALLOC.entry(conn.miner_stat.username.clone()).or_default();
            let kolka_id = alloc.allocate(conn.miner_stat.is_req_diff_esp32);

            conn.miner_stat.amount_of_workers = kolka_id;

            debug!("{}, Assigned {}", conn.id, conn.miner_stat.amount_of_workers);
        } else {
            data = [""; 8];
            conn.read_to_string(&mut read).await?;

            conn.miner_stat.ping = SystemTime::now()
                .duration_since(feedback_sent)?
                .as_secs_f32();

            data_len = scan_fields(&read, &mut data);
            req_difficulty.clear();

            if !conn.miner_stat.override_diff.is_empty() {
                if !set_req_difficulty(&mut req_difficulty, conn.miner_stat.override_diff.as_str()) {
                    req_difficulty.push_str("NET");
                }
            } else if using_avr {
                req_difficulty.push_str("AVR");
            } else {
                if data_len < 3 || data[2].is_empty() {
                    req_difficulty.push_str("NET");
                } else {
                    if !set_req_difficulty(&mut req_difficulty, data[2]) {
                        req_difficulty.push_str("NET");
                    }
                }

                if pool_rewards.get(req_difficulty.as_str()).is_none() {
                    req_difficulty.clear();
                    req_difficulty.push_str("NET");
                }
            }
        }

        if data_len >= 4 {
            let encoded_len = BASE64_STANDARD.encode_slice(data[3].as_bytes(), &mut password_buf)?;
            let encoded = std::str::from_utf8(&password_buf[..encoded_len])?;
            conn.miner_stat.password.clear();
            conn.miner_stat.password.push_str(encoded);
        } else {
            conn.miner_stat.password.clear();
        }

        // IoT reading stuff
        if data_len >= 5 {
            if data[4].len() > 48
            {
                conn.miner_stat.iot_reading = CompactString::const_new("Error:data too long");
            }
            else
            {
                clean_iot_into(&data[4], &mut conn.miner_stat.iot_reading);
            }
            // let cleaned_iot = clean_iot(&data[4]);

            // if cleaned_iot.is_empty() {
            //     conn.miner_stat.iot_reading = CompactString::default();
            // } else if data[4].len() > 48 {
            //     conn.miner_stat.iot_reading = CompactString::const_new("Error:data too long");
            // } else {
            //     conn.miner_stat.iot_reading = CompactString::from(data[4]);
            // }
        }

        // let user_workers = get_user_workers(&conn.miner_stat.username);
        let (user_workers, new_id) = USR_WORKERS_ALLOC
            .get_mut(&conn.miner_stat.username)
            .map(|mut alloc| {
                let workers = alloc.active_workers();
                let new_id = alloc.try_recycle(conn.miner_stat.amount_of_workers, conn.miner_stat.is_req_diff_esp32);
                (workers, new_id)
            })
            .unwrap_or((0.0, None));

        if check_workers(user_workers, &config) {
            conn.miner_stat.reject_shares = CompactString::const_new("Too many workers");
        }
        if let Some(id) = new_id {
            conn.miner_stat.amount_of_workers = id;
        }

        let diff_info =
            resolve_reward_info(pool_rewards, req_difficulty.as_str(), conn.miner_stat.override_diff.as_str())
                .ok_or_else(|| anyhow::anyhow!("failed to resolve difficulty info"))?;
        if is_first_share {
            diff = diff_info.difficulty as u32;
        }

        // id adjustment
        // {
        //     if let Some(mut alloc) = USR_WORKERS_ALLOC.get_mut(&conn.miner_stat.username) {
        //         if let Some(new_id) = alloc.try_recycle(conn.miner_stat.amount_of_workers, conn.miner_stat.is_req_diff_esp32) {
        //             conn.miner_stat.amount_of_workers = new_id;
        //         }
        //     }
        // }

        random = rand::rng().random_range(0u64..diff as u64 * 100u64) + 1;

        #[cfg(feature = "bench")]
        {
            random = 1;
        }

        let last_block_hash = LAST_BLOCK_HASH.load(); // Arc<String>
        hasher.reset();
        hasher.update(last_block_hash.as_bytes());
        hasher.update(buffer.format(random).as_bytes());

        let raw = hasher.digest().bytes();
        hex::encode_to_slice(raw, &mut hex_buf).unwrap();
        let new_hash_str = std::str::from_utf8(&hex_buf)?;

        job_str.clear();
        job_str.push_str(last_block_hash.as_str());
        job_str.push(',');
        job_str.push_str(new_hash_str);
        job_str.push(',');
        job_str.push_str(buffer.format(diff));
        job_str.push('\n');
        conn.write(&job_str).await?;

        let job_sent = SystemTime::now();
        data = [""; 8];
        conn.read_to_string(&mut read).await?;

        data_len = scan_fields(&read, &mut data);
        let answer_received = SystemTime::now();

        conn.miner_stat.share_time = answer_received.duration_since(job_sent)?.as_secs_f32();
        if conn.miner_stat.share_time > conn.miner_stat.ping {
            conn.miner_stat.share_time -= conn.miner_stat.ping;
        }

        // 1.5x to account that the average will be half the target (I don't know what this means, I copied the original comment)
        let val = conn.miner_stat.share_time * 1.5;
        conn.miner_stat
            .share_times_list
            .push_back(val);
        conn.miner_stat.total_share_times += val;
        if conn.miner_stat.share_times_list.len() > 50 {
            conn.miner_stat.total_share_times -= conn.miner_stat.share_times_list.pop_front().unwrap_or_else(|| 0f32);
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

        if reported_hashrate <= 0f32 && !wrong_reported_hashrate {
            conn.miner_stat.reject_shares = CompactString::const_new("Invalid data");
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

        let nonce;
        if using_avr {
            nonce = u64::from_str_radix(&data[0], 2)?;
        } else {
            nonce = match u64::from_str(&data[0]) {
                Ok(ok) => ok,
                Err(err) => {
                    debug!("Nonce error: {err:?} ({:?})", data);
                    conn.write("BAD,No response to job\n").await?;
                    return Err(anyhow::Error::from_boxed(Box::from("Shutting down.")));
                    // used so we can handle closes centrally
                    // (some request a job and after receiving the job they immediately request another one)
                }
            };
        }
        #[cfg(not(feature = "bench"))]
        // if the pool was built with the bench feature enabled, we discard this codeblock
        {
            if conn.miner_stat.last_min_shares > config.max_shares_per_minute {
                conn.miner_stat.reject_shares = CompactString::const_new("Too low difficulty");
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
            this_miner_chipid.clear();
            if data_len >= 5 {
                this_miner_chipid.push_str(data[4]);
            }
        }

        let hashrate_perc_diff_for_calc = perc_diff(hashrate, hashrate_calc);

        if diff > DUE_DIFF_CACHE.load(Relaxed)
            && diff <= ESP8266NH_DIFF_CACHE.load(Relaxed)
            && hashrate_perc_diff_for_calc > 15f32
            && conn.miner_stat.share_time >= 1.5
        {
            conn.miner_stat.reject_shares = CompactString::const_new("Modified hashrate");
        }

        // we don't care whether they overreport their hashrate
        if conn.miner_stat.share_time >= 1.5 && hashrate_perc_diff_for_calc > 15f32 && hashrate < hashrate_calc {
            // the difference is sometimes (currently) inexplicably high, therefore we rarely want to reject shares for it..
            if hashrate_perc_diff_for_calc >= 50f32 && false {
                // disabled for now..
                conn.miner_stat.reject_shares = CompactString::const_new("Modified hashrate");
            }
            hashrate = hashrate_calc;
        }

        let avg_sharetime = conn.miner_stat.total_share_times / conn.miner_stat.share_times_list.len() as f32;
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
            && PC_DIFF_TIERS.contains(&req_difficulty.as_str())
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

        reward = 0f32;
        if !conn.miner_stat.reject_shares.is_empty() {
            msg.clear();
            msg.push_str("BAD,");
            msg.push_str(conn.miner_stat.reject_shares.as_str());
            msg.push('\n');
            conn.write(&msg).await?;

            conn.miner_stat.reject_shares.clear();
            conn.miner_stat.rejected_shares += 1;
        } else if hashrate < min_hashrate as f32 {
            conn.miner_stat.override_diff = CompactString::from(kolka::kolka::v2_reverse(req_difficulty.as_str()));
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too high starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
        } else if hashrate >= max_hashrate as f32 {
            conn.miner_stat.override_diff = CompactString::from(kolka::kolka::v2(req_difficulty.as_str()));
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too low starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
        } else if nonce == random {
            conn.miner_stat.accepted_shares += 1;
            if conn.miner_stat.accepted_shares > 10 {
                // why is this a thing?
                if diff <= ESP8266NH_DIFF_CACHE.load(Relaxed) {
                    if this_miner_chipid.is_empty() {
                        conn.miner_stat.rejected_shares += 1;
                    } else if data_len >= 5 && data[4] != this_miner_chipid.as_str() {
                        conn.miner_stat.rejected_shares += 1;
                    } else {
                        reward = kolka::kolka::v1(
                            &kolka,
                            hashrate_calc,
                            diff,
                            conn.miner_stat.amount_of_workers,
                            reward_div,
                        );
                    }
                } else {
                    reward = kolka::kolka::v1(
                        &kolka,
                        hashrate_calc,
                        diff,
                        conn.miner_stat.amount_of_workers,
                        reward_div,
                    );
                }
            }

            // if (rand::rng().random_range(0f32..1f32) * block_probability as f32).floor() == 1f32 {
            if rng().random_range(0u32..block_probability) == 0 {
                reward += config.block_reward;

                let block_infos = BlockInfos::new(
                    answer_received.duration_since(UNIX_EPOCH)?.as_secs(),
                    conn.miner_stat.username.to_string(),
                    reward,
                    "DUCO-S1".to_string(),
                    new_hash_str.to_string(),
                );
                GLOBAL_BLOCKS.lock().await.push(block_infos);
                conn.write("BLOCK\n").await?;
                info!("Block found by {}", conn.miner_stat.username);
            } else {
                conn.write("GOOD\n").await?;
            }
        } else {
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Incorrect result\n").await?;
        }

        if conn.miner_stat.accepted_shares > 0
            && conn.miner_stat.accepted_shares % config.update_miners_stats_every as u64 == 0
        {
            *BALANCES_TO_UPDATE
                .entry(conn.miner_stat.username.clone())
                .or_insert(0f32) += reward;

            // conn.miner_stat.software = CompactString::from(
            //     if data_len >= 3 {
            //         let cleaned_name = clean_name(&data[2]);
            //         match cleaned_name.as_str() {
            //             "" => "Unknown miner".to_string(),
            //             _ => cleaned_name,
            //         }
            //     } else {
            //         "Unknown miner".to_string()
            //     }
            // );

            conn.miner_stat.software.clear();

            if data_len >= 3 {
                clean_name_into(data[2], &mut conn.miner_stat.software);

                if conn.miner_stat.software.is_empty() {
                    conn.miner_stat.software.push_str("Unknown miner");
                }
            } else {
                conn.miner_stat.software.push_str("Unknown miner");
            }

            let wallet_id = if data_len >= 6 { &data[5] } else { "None" };
            conn.miner_stat.wallet_id.clear();
            conn.miner_stat.wallet_id.push_str(wallet_id.trim_end_matches("\n"));

            let (rig_part, key_part) = if data_len >= 4 {
                match data[3].split_once(':') {
                    Some((r, k)) => (r, Some(k)),
                    None => (data[3], None),
                }
            } else {
                ("", None)
            };
            // let rig_identifier = {
            //     let cleaned = clean_name(rig_part);
            //     match cleaned.as_str() {
            //         "" => "None".to_string(),
            //         _ => cleaned,
            //     }
            // };

            conn.miner_stat.id.clear();
            clean_name_into(rig_part, &mut conn.miner_stat.id);
            if conn.miner_stat.id.is_empty() {
                conn.miner_stat.id.push_str("None");
            }

            // conn.miner_stat.id = CompactString::from(&rig_identifier);
            conn.miner_stat.hashrate = if hashrate_is_estimated {
                hashrate
            } else {
                reported_hashrate
            };
            conn.miner_stat.difficulty = diff;

            if let Some(key) = key_part {
                // mining key backwards compatibility for old miners
                let encoded_len = BASE64_STANDARD.encode_slice(key.as_bytes(), &mut password_buf)?;
                let encoded = std::str::from_utf8(&password_buf[..encoded_len])?;
                conn.miner_stat.password.clear();
                conn.miner_stat.password.push_str(encoded);
            }


            {
                let now_secs = UNIX_EPOCH.elapsed()?.as_secs();
                if now_secs - conn.miner_stat.last_share_reset >= 60 {
                    conn.miner_stat.last_share_reset = now_secs;
                    conn.miner_stat.last_min_shares = 0;
                }
            }

            conn.miner_stat.this_miner_chipid.clear();
            conn.miner_stat.this_miner_chipid.push_str(this_miner_chipid.as_str());
            conn.miner_stat.reward = reward;

            let miner_stat_slim = conn.miner_stat.convert_slim();

            match MINER_STATS.get_mut(&*conn.id) {
                Some(mut e) => *e = miner_stat_slim,
                None => { MINER_STATS.insert(conn.id.clone(), miner_stat_slim); }
            }

            LAST_BLOCK_HASH.store(Arc::new(new_hash_str.to_string()));
            GLOBAL_SHARES_INCREASE.fetch_add(
                config.update_miners_stats_every as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            GLOBAL_SHARES_TOTAL.fetch_add(
                config.update_miners_stats_every as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        is_first_share = false;
        // because feedback_sent is only used to calculate the ping, we want to set it as late as possible
        feedback_sent = SystemTime::now();
    }
}

fn avg_from_array(share_times_list: &VecDeque<f32>) -> f32 {
    share_times_list.iter().sum::<f32>() / share_times_list.len() as f32
}
fn perc_diff(hashrate: f32, hashrate_calc: f32) -> f32 {
    (100f32 * ((hashrate - hashrate_calc) / ((hashrate + hashrate_calc) / 2f32)).abs()).floor()
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

fn get_user_workers(username: &str) -> f32 {
    USR_WORKERS_ALLOC
        .get(username)
        .map(|a| a.active_workers())
        .unwrap_or(0.0)
}

fn check_workers(max: f32, config: &PoolConfig) -> bool {
    if config.max_workers <= 0 {
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

// const ALLOWED_SPECIAL_IOT: [char; 7] = ['.', '(', ')', ':', '@', '-', ' '];
// fn clean_iot(input: &str) -> String {
//     input.replace(
//         |x: char| !x.is_ascii_alphanumeric() && !ALLOWED_SPECIAL_IOT.contains(&x),
//         "",
//     )
// }

// const ALLOWED_SPECIAL: [char; 5] = ['.', '(', ')', '-', ' '];

// const fn build_allowed() -> [bool; 128] {
//     let mut t = [false; 128];
//     // alphanumeric handled separately; add special chars:
//     t[b'.' as usize] = true;
//     t[b'(' as usize] = true;
//     t[b')' as usize] = true;
//     t[b'-' as usize] = true;
//     t[b' ' as usize] = true;
//     t
// }
//
// const fn build_iot_allowed() -> [bool; 128] {
//     let mut t = [false; 128];
//     // alphanumeric handled separately; add special chars:
//     t[b'.' as usize] = true;
//     t[b'(' as usize] = true;
//     t[b')' as usize] = true;
//     t[b':' as usize] = true;
//     t[b'@' as usize] = true;
//     t[b'-' as usize] = true;
//     t[b' ' as usize] = true;
//     t
// }
// static ALLOWED_TABLE: [bool; 128] = build_allowed();
// static ALLOWED_IOT_TABLE: [bool; 128] = build_iot_allowed();
// fn clean_name(input: &str) -> String {
//     input.replace(
//         |x: char| !x.is_ascii_alphanumeric() && !ALLOWED_SPECIAL.contains(&x),
//         "",
//     )
// }

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
// fn scan_fields<'a>(s: &'a str, dst: &mut [&'a str; 8]) -> usize {
//     let mut n = 0;
//     let mut rest = s;
//     while n < dst.len() {
//         match rest.find(',') {
//             Some(i) => {
//                 dst[n] = &rest[..i];
//                 n += 1;
//                 rest = &rest[i + 1..];
//             }
//             None => {
//                 dst[n] = rest;
//                 n += 1;
//                 return n;
//             }
//         }
//     }
//     n
// }
fn scan_fields<'a>(s: &'a str, dst: &mut [&'a str; 8]) -> usize {
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
