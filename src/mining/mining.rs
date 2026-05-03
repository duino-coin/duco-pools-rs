use crate::cfg::config::PoolConfig;
use crate::data::data::Connection;
use crate::mining::id_allocator::WorkerAllocator;
use crate::mining::stats::{BlockInfos, MinerStatSlim};
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
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
// I hate that these are pub static, but I honestly can't be arsed to pass them as params..

pub static USR_WORKERS_ALLOC: LazyLock<DashMap<CompactString, WorkerAllocator>> = LazyLock::new(|| DashMap::new());

pub static BALANCES_TO_UPDATE: LazyLock<DashMap<CompactString, f32>> = LazyLock::new(|| DashMap::new());

pub static MINER_STATS: LazyLock<DashMap<String, MinerStatSlim>> = LazyLock::new(|| DashMap::new());

static LAST_BLOCK_HASH: LazyLock<Mutex<String>> = LazyLock::new(|| {
    let config = cfg::config::load_config().expect("failed to load config (static)");
    Mutex::new(config.initial_blockhash)
});

pub static GLOBAL_BLOCKS: LazyLock<Mutex<Vec<BlockInfos>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

pub static GLOBAL_SHARES_INCREASE: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

pub static GLOBAL_SHARES_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

const PC_DIFF_TIERS: [&str; 4] = ["EXTREME", "NET", "MEDIUM", "LOW"];

pub async fn handle(
    conn: &mut Connection,
    initial_data: Box<[&str]>,
    config: &PoolConfig,
    using_avr: bool,
) -> Result<(), anyhow::Error> {
    let mut random;
    let mut new_hash;
    // Reusable buffer — req_difficulty cycles through short fixed strings every iteration
    let mut req_difficulty = String::with_capacity(16);
    let mut this_miner_chipid = CompactString::default();
    // let mut miner_name;
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
    let mut read; // needed for first share..

    let mut data = initial_data.iter().map(|&x| x).collect::<Box<[&str]>>();
    drop(initial_data);
    loop {
        if is_first_share {
            req_difficulty.clear();
            if data.len() < 3 || data[2].is_empty() {
                req_difficulty.push_str("NET");
            } else {
                req_difficulty.push_str(data[2]);
            }
            if !pool_rewards.get(&req_difficulty).is_some() {
                req_difficulty.clear();
                req_difficulty.push_str("NET");
            }
            diff = kolka::kolka::get_diff(&req_difficulty);

            // why??
            if req_difficulty == "ESP32" {
                conn.miner_stat.is_req_diff_esp32 = true;
            }
            let mut alloc = USR_WORKERS_ALLOC.entry(conn.miner_stat.username.clone()).or_default();
            let kolka_id = alloc.allocate(conn.miner_stat.is_req_diff_esp32);

            conn.miner_stat.amount_of_workers = kolka_id;
            // drop(val);
            debug!("{}, Assigned {}", conn.id, conn.miner_stat.amount_of_workers);
        } else {
            read = conn.read_to_string().await?;

            conn.miner_stat.ping = SystemTime::now()
                .duration_since(feedback_sent)?
                .as_secs_f32();

            data = read.split(",").collect::<Box<[&str]>>(); // receive new data and split by "args"

            req_difficulty.clear();

            if !conn.miner_stat.override_diff.is_empty() {
                req_difficulty.clear();
                req_difficulty.push_str(&conn.miner_stat.override_diff);
            } else if using_avr {
                req_difficulty.clear();
                req_difficulty.push_str("AVR");
            } else {
                if data.len() < 3 || data[2].is_empty() {
                    req_difficulty.push_str("NET");
                } else {
                    req_difficulty.push_str(data[2]);
                }

                if !pool_rewards.get(&req_difficulty).is_some() {
                    req_difficulty.clear();
                    req_difficulty.push_str("NET");
                }
            }
        }

        conn.miner_stat.password = if data.len() >= 4 {
            CompactString::from(BASE64_STANDARD.encode(data[3]))
        } else {
            CompactString::default()
        };

        // IoT reading stuff
        if data.len() >= 5 {
            let cleaned_iot = clean_iot(&data[4]);
            if cleaned_iot.is_empty() {
                conn.miner_stat.iot_reading = CompactString::default();
            } else if data[4].len() > 48 {
                conn.miner_stat.iot_reading = CompactString::const_new("Error:data too long");
            } else {
                conn.miner_stat.iot_reading = CompactString::from(data[4]);
            }
        }

        let user_workers = get_user_workers(&conn.miner_stat.username);

        if check_workers(user_workers, &config) {
            conn.miner_stat.reject_shares = CompactString::const_new("Too many workers");
        }

        // id adjustment
        {
            if let Some(mut alloc) = USR_WORKERS_ALLOC.get_mut(&conn.miner_stat.username) {
                if let Some(new_id) = alloc.try_recycle(conn.miner_stat.amount_of_workers, conn.miner_stat.is_req_diff_esp32) {
                    conn.miner_stat.amount_of_workers = new_id;
                }
            }
        }

        random = rng().random_range(0u64..diff as u64 * 100u64) + 1;

        #[cfg(feature = "bench")]
        {
            random = 1;
        }

        // used so we don't keep the lock on LAST_BLOCK_HASH over a conn.write (+ to drop job_str implicitly)
        {
            // used to lose the lock on LAST_BLOCK_HASH
            let job_str = {
                let last_block_hash = LAST_BLOCK_HASH.lock().await;
                let mut hasher = Sha1::from(last_block_hash.as_str());
                hasher.update(buffer.format(random).as_bytes());
                new_hash = hasher.hexdigest();

                let diff_str = buffer.format(diff);
                let mut job_str =
                    String::with_capacity(last_block_hash.len() + 1 + new_hash.len() + 1 + diff_str.len() + 1);
                job_str.push_str(&last_block_hash);
                job_str.push_str(",");
                job_str.push_str(new_hash.as_str());
                job_str.push_str(",");
                job_str.push_str(diff_str);
                job_str.push_str("\n");

                job_str
            };
            conn.write(&job_str).await?;
        }

        let job_sent = SystemTime::now();
        read = conn.read_to_string().await?;

        data = read.split(",").collect::<Box<[&str]>>();
        let answer_received = SystemTime::now();

        conn.miner_stat.share_time = answer_received.duration_since(job_sent)?.as_secs_f32();
        if conn.miner_stat.share_time > conn.miner_stat.ping {
            conn.miner_stat.share_time -= conn.miner_stat.ping;
        }

        // 1.5x to account that the average will be half the target (I don't know what this means, I copied the original comment)
        conn.miner_stat
            .share_times_list
            .push_back(conn.miner_stat.share_time * 1.5);

        if conn.miner_stat.share_times_list.len() > 50 {
            conn.miner_stat.share_times_list.pop_front();
        }

        let mut wrong_reported_hashrate = false;
        if data.len() >= 2 {
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
            this_miner_chipid = if data.len() >= 5 {
                CompactString::from(data[4])
            } else {
                CompactString::default()
            };
        }

        let hashrate_perc_diff_for_calc = perc_diff(hashrate, hashrate_calc);

        if diff > kolka::kolka::get_diff("DUE")
            && diff <= kolka::kolka::get_diff("ESP8266NH")
            && hashrate_perc_diff_for_calc > 15f32
            && conn.miner_stat.share_time >= 1.5
        {
            conn.miner_stat.reject_shares = CompactString::const_new("Modified hashrate");
        }

        // we don't care whether they overreport their hashrate
        if hashrate_perc_diff_for_calc > 15f32 && hashrate < hashrate_calc {
            // the difference is sometimes (currently) inexplicably high, therefore we rarely want to reject shares for it..
            if hashrate_perc_diff_for_calc >= 50f32 && false {
                // disabled for now..
                conn.miner_stat.reject_shares = CompactString::const_new("Modified hashrate");
            }
            hashrate = hashrate_calc;
        }

        let avg_sharetime = avg_from_array(&conn.miner_stat.share_times_list);
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
                avg_from_array(&conn.miner_stat.share_times_list),
                expected_share_time,
                hashrate,
            );
        }
        let diff_obj = match pool_rewards
            .get(&req_difficulty) {
            None => {
                let ret = if !conn.miner_stat.override_diff.is_empty() {
                    pool_rewards.get(conn.miner_stat.override_diff.as_str()).expect(&format!("failed to get diff {}", conn.miner_stat.override_diff))
                } else {
                    pool_rewards.get("NET").expect("failed to get diff NET")
                };
                ret
            }
            Some(ok) => { ok }
        };

        let reward_div = diff_obj.reward;
        let max_hashrate = diff_obj.max_hashrate;
        let min_hashrate = diff_obj.min_hashrate;
        expected_share_time = diff_obj.expected_share_time;
        let block_probability = diff_obj.block_chance;

        reward = 0f32;
        if !conn.miner_stat.reject_shares.is_empty() {
            let mut msg = String::with_capacity(4 + conn.miner_stat.reject_shares.len() + 1);
            msg.push_str("BAD,");
            msg.push_str(&conn.miner_stat.reject_shares);
            msg.push('\n');
            conn.write(&msg).await?;

            conn.miner_stat.reject_shares.clear();
            conn.miner_stat.rejected_shares += 1;
        } else if hashrate < min_hashrate as f32 {
            conn.miner_stat.override_diff = CompactString::from(kolka::kolka::v2_reverse(&req_difficulty));
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too high starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
        } else if hashrate >= max_hashrate as f32 {
            conn.miner_stat.override_diff = CompactString::from(kolka::kolka::v2(&req_difficulty));
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too low starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
        } else if nonce == random {
            conn.miner_stat.accepted_shares += 1;
            if conn.miner_stat.accepted_shares > 10 {
                // why is this a thing?
                if diff <= kolka::kolka::get_diff("ESP8266NH") {
                    if this_miner_chipid.is_empty() {
                        conn.miner_stat.rejected_shares += 1;
                    } else if data.len() >= 5 && data[4] != this_miner_chipid.as_str() {
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

            if (rng().random_range(0f32..1f32) * block_probability as f32).floor() == 1f32 {
                reward += config.block_reward;

                let block_infos = BlockInfos::new(
                    // UNIX_EPOCH.duration_since(SystemTime::now())?.as_secs(),
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                    conn.miner_stat.username.to_string(),
                    reward,
                    "DUCO-S1".to_string(),
                    new_hash.clone(),
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

            // miner_name = if data.len() >= 3 {
            //     let cleaned_name = clean_name(&data[2]);
            //     match cleaned_name.as_str() {
            //         "" => "Unknown miner".to_string(),
            //         _ => cleaned_name,
            //     }
            // } else {
            //     "Unknown miner".to_string()
            // };
            // conn.miner_stat.software = CompactString::from(&miner_name);
            conn.miner_stat.software = CompactString::from(
                if data.len() >= 3 {
                    let cleaned_name = clean_name(&data[2]);
                    match cleaned_name.as_str() {
                        "" => "Unknown miner".to_string(),
                        _ => cleaned_name,
                    }
                } else {
                    "Unknown miner".to_string()
                }
            );

            let wallet_id = if data.len() >= 6 { &data[5] } else { "None" };
            conn.miner_stat.wallet_id = CompactString::from(wallet_id.replace("\n", ""));

            let cut_rigid = if data.len() >= 4 {
                data[3].split(":").collect::<Vec<&str>>()
            } else {
                Vec::new()
            };
            let rig_identifier = if cut_rigid.len() > 0 {
                let cleaned = clean_name(cut_rigid[0]);

                match cleaned.as_str() {
                    "" => "None".to_string(),
                    _ => cleaned,
                }
            } else {
                "None".to_string()
            };
            conn.miner_stat.id = CompactString::from(&rig_identifier);
            // conn.miner_stat.algorithm = CompactString::const_new("DUCO-S1");
            conn.miner_stat.hashrate = if hashrate_is_estimated {
                hashrate
            } else {
                reported_hashrate
            };
            conn.miner_stat.difficulty = diff;

            if cut_rigid.len() >= 2 {
                // mining key backwards compatibility for old miners
                conn.miner_stat.password = CompactString::from(BASE64_STANDARD.encode(cut_rigid[1]));
            }

            if UNIX_EPOCH.elapsed()?.as_secs() - conn.miner_stat.last_share_reset >= 60 {
                conn.miner_stat.last_share_reset = UNIX_EPOCH.elapsed()?.as_secs();
                conn.miner_stat.last_min_shares = 0;
            }

            // conn.miner_stat.kolka_drop = conn.miner_stat.amount_of_workers;
            conn.miner_stat.this_miner_chipid = this_miner_chipid.clone();
            conn.miner_stat.reward = reward;
            // conn.miner_stat.ip = CompactString::from(&conn.ip);
            // conn.miner_stat.username = CompactString::from(&conn.username);
            // conn.miner_stat.pool_name = CompactString::from(&config.pool_name);

            let miner_stat_slim = conn.miner_stat.convert_slim();
            MINER_STATS.insert((*conn.id).to_string(), miner_stat_slim);

            let mut last_block_hash = LAST_BLOCK_HASH.lock().await;
            last_block_hash.clear();
            last_block_hash.push_str(&new_hash);
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

const ALLOWED_SPECIAL_IOT: [char; 7] = ['.', '(', ')', ':', '@', '-', ' '];
fn clean_iot(input: &str) -> String {
    input.replace(
        |x: char| !x.is_ascii_alphanumeric() && !ALLOWED_SPECIAL_IOT.contains(&x),
        "",
    )
}

const ALLOWED_SPECIAL: [char; 5] = ['.', '(', ')', '-', ' '];
fn clean_name(input: &str) -> String {
    input.replace(
        |x: char| !x.is_ascii_alphanumeric() && !ALLOWED_SPECIAL.contains(&x),
        "",
    )
}
