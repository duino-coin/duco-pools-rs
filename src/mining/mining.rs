use crate::cfg::config::PoolConfig;
use crate::data::data::Connection;
use crate::mining::stats::{BlockInfos, MinerStat};
use crate::{cfg, kolka, KOLKA, POOL_REWARDS};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use dashmap::{DashMap, Entry};
use log::{debug, info};
use rand::{rng, Rng};
use sha1_smol::Sha1;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use dashmap::mapref::one::Ref;
use tokio::sync::Mutex;

// I hate that these are pub static, but I honestly can't be arsed to pass them as params..
pub static WORKERS: LazyLock<DashMap<String, f32>> = LazyLock::new(|| DashMap::new());

pub static USR_WORKERS: LazyLock<DashMap<String, f32>> = LazyLock::new(|| DashMap::new());
pub static RELEGATION_MAP: LazyLock<DashMap<String, Vec<u16>>> = LazyLock::new(|| DashMap::new());
pub static BALANCES_TO_UPDATE: LazyLock<DashMap<String, f32>> = LazyLock::new(|| DashMap::new());

pub static MINER_STATS: LazyLock<DashMap<String, MinerStat>> = LazyLock::new(|| DashMap::new());

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
    let mut req_difficulty;
    let mut mining_key;
    let mut this_miner_chipid = "".to_string();
    let mut miner_name;
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
    // let pool_rewards = POOL_REWARDS.get().unwrap();
    let pool_rewards = &POOL_REWARDS;
    let mut buffer = itoa::Buffer::new(); // used to convert nums to str a little faster
    let mut read; // needed for first share..

    let mut data = initial_data.iter().map(|&x| x).collect::<Box<[&str]>>();
    drop(initial_data);
    loop {
        if is_first_share {
            req_difficulty = if data.len() < 3 || data[2].is_empty() {
                "NET".to_string()
            } else {
                data[2].to_string()
            };
            if !pool_rewards.get(&req_difficulty).is_some() {
                req_difficulty = "NET".to_string();
            }
            diff = kolka::kolka::get_diff(&req_difficulty);

            let mut worker_add = 1f32;
            // why??
            if req_difficulty == "ESP32" {
                worker_add = 0.5;
            }
            *WORKERS.entry(conn.ip.clone()).or_insert(0f32) += worker_add;
            *USR_WORKERS.entry(conn.username.clone()).or_insert(0f32) += worker_add;

            if conn.ip != "127.0.0.1" {
                conn.miner_stat.amount_of_workers = f32::max(
                    f32::max(get_user_workers(&conn.username), get_ip_workers(&conn.ip)),
                    conn.miner_stat.server_miners as f32,
                ); // this is horrible
            } else {
                conn.miner_stat.amount_of_workers = f32::max(
                    get_user_workers(&conn.username),
                    conn.miner_stat.server_miners as f32,
                );
            }
        } else {
            read = conn.read_to_string().await?;

            conn.miner_stat.ping = SystemTime::now()
                .duration_since(feedback_sent)?
                .as_secs_f32();


            data = read.split(",").collect::<Box<[&str]>>(); // receive new data and split by "args"

            req_difficulty = if data.len() < 3 || data[2].is_empty() {
                "NET".to_string()
            } else {
                data[2].to_string()
            };

            if !pool_rewards.get(&req_difficulty).is_some() {
                req_difficulty = "NET".to_string();
            }

            if !conn.miner_stat.override_diff.is_empty() {
                let override_diff = conn.miner_stat.override_diff.clone();
                req_difficulty = override_diff;
            } else if using_avr {
                req_difficulty = "AVR".to_string();
            }
        }

        if data.len() >= 4 {
            mining_key = BASE64_STANDARD.encode(&data[3]);
        } else {
            mining_key = String::new();
        }
        conn.miner_stat.password = mining_key;

        // IoT reading stuff
        if data.len() >= 5 {
            let cleaned_iot = clean_iot(&data[4]);
            if cleaned_iot.is_empty() {
                conn.miner_stat.iot_reading = "".to_string();
            } else if data[4].len() > 48 {
                conn.miner_stat.iot_reading = "Error:data too long".to_string();
            } else {
                conn.miner_stat.iot_reading = data[4].to_string();
            }
        }

        // TODO: determine if needed after new fix
        let ip_workers = if conn.ip != "127.0.0.1" {
            get_ip_workers(&conn.ip)
        } else {
            0f32
        };
        let user_workers = get_user_workers(&conn.username);
        let max = max_f32(
            ip_workers,
            user_workers,
            conn.miner_stat.server_miners as f32,
        );
        if check_workers(max, &config) {
            conn.miner_stat.reject_shares = "Too many workers".to_string();
        }

        if conn.miner_stat.amount_of_workers > max {
            if RELEGATION_MAP.contains_key(&conn.miner_stat.username) {
                let mut map_val = RELEGATION_MAP.entry(conn.miner_stat.username.clone()).or_default();
                let val = map_val.value_mut();
                let id = val.pop();
                if id.is_some()
                {
                    conn.miner_stat.amount_of_workers = id.unwrap() as f32;
                }
                else
                {
                    conn.miner_stat.amount_of_workers = max;
                }
            }
            else
            {
                conn.miner_stat.amount_of_workers = max;
            }

        }

        random = rng().random_range(0u64..diff as u64 * 100u64) + 1;

        #[cfg(feature = "bench")]
        {
            random = 1;
        }

        // used to lose the lock on LAST_BLOCK_HASH
        {
            let last_block_hash = LAST_BLOCK_HASH.lock().await.clone();
            let mut hasher = Sha1::from(&last_block_hash);
            hasher.update(buffer.format(random).as_bytes());
            new_hash = hasher.hexdigest();

            let job = (last_block_hash, new_hash.clone());
            let diff_str = buffer.format(diff);
            let mut job_str =
                String::with_capacity(",,,\n".len() + job.0.len() + job.1.len() + diff_str.len()); // faster than format macro
            job_str.push_str(job.0.as_str());
            job_str.push_str(",");
            job_str.push_str(job.1.as_str());
            job_str.push_str(",");
            job_str.push_str(diff_str);
            job_str.push_str("\n");

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
            .push(conn.miner_stat.share_time * 1.5);

        if conn.miner_stat.share_times_list.len() > 50 {
            let _ = conn.miner_stat.share_times_list.pop();
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
        }
        else {
            wrong_reported_hashrate = true;
            reported_hashrate = 0f32;
        }

        if reported_hashrate <= 0f32 && !wrong_reported_hashrate {
            conn.miner_stat.reject_shares = "Invalid data".to_string();
        }
        // if conn.miner_stat.share_time >= 0.5 || wrong_reported_hashrate {

        {
            let temp_share_time = conn.miner_stat.share_time + conn.miner_stat.ping; // it benefits the user
            if temp_share_time > 0f32 {
                hashrate_calc = random as f32 / temp_share_time;
            }
            else {
                hashrate_calc = if wrong_reported_hashrate { start_hashrate } else { reported_hashrate }; // I'd be damned if the sharetime's actually ever EXACTLY 0...
            }
        }

        if wrong_reported_hashrate {
            reported_hashrate = hashrate_calc;
        }
        // } else {
        //     hashrate_calc = reported_hashrate;
        // }

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
                conn.miner_stat.reject_shares = "Too low difficulty".to_string();
            }
        }

        // let hashrate_perc_diff = perc_diff(reported_hashrate, hashrate_calc);

        hashrate_is_estimated = false;
        if reported_hashrate <= 0f32 {
            hashrate_is_estimated = true;
            hashrate = hashrate_calc;
        } else {
            hashrate = reported_hashrate;
        }

        if is_first_share {
            this_miner_chipid = if data.len() >= 5 {
                data[4].to_string()
            } else {
                "".to_string()
            };
        }

        let hashrate_perc_diff_for_calc = perc_diff(hashrate, hashrate_calc);

        if diff > kolka::kolka::get_diff("DUE")
            && diff <= kolka::kolka::get_diff("ESP8266NH")
            && hashrate_perc_diff_for_calc > 15f32
            && conn.miner_stat.share_time >= 1.5
        {
            conn.miner_stat.reject_shares = "Modified hashrate".to_string();
        }


        // we don't care whether they overreport their hashrate
        if hashrate_perc_diff_for_calc > 15f32 && hashrate < hashrate_calc {
            // the difference is sometimes (currently) inexplicably high, therefore we rarely want to reject shares for it..
            if hashrate_perc_diff_for_calc >= 50f32 && false { // disabled for now..
                conn.miner_stat.reject_shares = "Modified hashrate".to_string();
                // println!("diff {hashrate_perc_diff_for_calc} {hashrate} {hashrate_calc} {random} {} {}", conn.miner_stat.share_time, conn.miner_stat.ping);
            }
            hashrate = hashrate_calc;
        }

        let avg_sharetime = avg_from_array(&conn.miner_stat.share_times_list);
        let actual_expected_share_time = random / (diff as u64 * 100 + 1) * expected_share_time as u64;
        if (conn.miner_stat.accepted_shares == 4
            || (
                conn.miner_stat.accepted_shares > 4
                && (perc_diff(hashrate, start_hashrate) > 15f32 || (1f32 - (avg_sharetime / actual_expected_share_time as f32)) * 100f32 > 25f32) // if the hashrate has changed substantially or if the sharetime is substantially lower than targeted, adjust
            )
        )
            && PC_DIFF_TIERS.contains(&req_difficulty.as_str())
        {
            start_hashrate = hashrate;

            diff = kolka::kolka::v3(
                avg_from_array(&conn.miner_stat.share_times_list),
                expected_share_time,
                hashrate,
            );
        }
        let diff_obj = pool_rewards
            .get(&req_difficulty)
            .expect("Missing pool reward");

        let reward_div = diff_obj.reward;
        let max_hashrate = diff_obj.max_hashrate;
        let min_hashrate = diff_obj.min_hashrate;
        expected_share_time = diff_obj.expected_share_time;
        let block_probability = diff_obj.block_chance;

        reward = 0f32;
        if !conn.miner_stat.reject_shares.is_empty() {
            let mut res =
                String::with_capacity("BAD,\n".len() + conn.miner_stat.reject_shares.len()); // this is faster than the format macro (ufmt is possibly a faster alternative?)
            res.push_str("BAD,");
            res.push_str(conn.miner_stat.reject_shares.as_str());
            res.push_str("\n");

            conn.write(&res).await?;
            // feedback_sent = SystemTime::now();

            conn.miner_stat.rejected_shares += 1;
        } else if hashrate < min_hashrate as f32 {
            conn.miner_stat.override_diff = kolka::kolka::v2_reverse(&req_difficulty);
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too high starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
            // feedback_sent = SystemTime::now();
        } else if hashrate >= max_hashrate as f32 {
            conn.miner_stat.override_diff = kolka::kolka::v2(&req_difficulty);
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too low starting difficulty\n").await?;
            start_hashrate = -1f32;
            diff = kolka::kolka::get_diff(&conn.miner_stat.override_diff);
            // feedback_sent = SystemTime::now();
        } else if nonce == random {
            conn.miner_stat.accepted_shares += 1;
            if conn.miner_stat.accepted_shares > 10 {
                // why is this a thing?
                if diff <= kolka::kolka::get_diff("ESP8266NH") {
                    if this_miner_chipid.is_empty() {
                        conn.miner_stat.rejected_shares += 1;
                    } else if data.len() >= 5 && data[4] != this_miner_chipid {
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
                    UNIX_EPOCH.duration_since(SystemTime::now())?.as_secs(),
                    conn.username.clone(),
                    reward,
                    "DUCO-S1".to_string(),
                    new_hash.clone(),
                );
                GLOBAL_BLOCKS.lock().await.push(block_infos);
                conn.write("BLOCK\n").await?;
                info!("Block found by {}", conn.username);
                // feedback_sent = SystemTime::now();
            } else {
                conn.write("GOOD\n").await?;
                // feedback_sent = SystemTime::now();
            }
        } else {
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Incorrect result\n").await?;
            // feedback_sent = SystemTime::now();
        }

        if conn.miner_stat.accepted_shares > 0
            && conn.miner_stat.accepted_shares % config.update_miners_stats_every as u64 == 0
        {
            *BALANCES_TO_UPDATE
                .entry(conn.username.clone())
                .or_insert(0f32) += reward;

            let cleaned_name = clean_name(&data[2]);

            miner_name = if data.len() >= 3 {
                match cleaned_name.as_str() {
                    "" => "Unknown miner".to_string(),
                    _ => cleaned_name,
                }
            } else {
                "Unknown miner".to_string()
            };
            conn.miner_stat.software = miner_name.to_string();

            let wallet_id = if data.len() >= 6 { &data[5] } else { "None" };
            conn.miner_stat.wallet_id = wallet_id.replace("\n", "");

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
            conn.miner_stat.id = rig_identifier;
            conn.miner_stat.algorithm = "DUCO-S1".to_string();
            conn.miner_stat.hashrate = if hashrate_is_estimated {
                hashrate
            } else {
                reported_hashrate
            };
            conn.miner_stat.difficulty = diff;

            if cut_rigid.len() >= 2 {
                // mining key backwards compatibility for old miners
                conn.miner_stat.password = BASE64_STANDARD.encode(cut_rigid[1]);
            }

            if UNIX_EPOCH.elapsed()?.as_secs() - conn.miner_stat.last_share_reset >= 60 {
                conn.miner_stat.last_share_reset = UNIX_EPOCH.elapsed()?.as_secs();
                conn.miner_stat.last_min_shares = 0;
            }

            conn.miner_stat.kolka_drop = conn.miner_stat.amount_of_workers;
            conn.miner_stat.this_miner_chipid = this_miner_chipid.clone();
            conn.miner_stat.reward = reward;
            conn.miner_stat.ip = conn.ip.clone();
            conn.miner_stat.username = conn.username.clone();
            conn.miner_stat.pool_name = config.pool_name.clone();
            MINER_STATS.insert(conn.id.clone(), conn.miner_stat.clone());
            // println!("{} {}", conn.miner_stat.id, conn.miner_stat.kolka_drop);
            let mut last_block_hash = LAST_BLOCK_HASH.lock().await;
            *last_block_hash = new_hash;
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

fn avg_from_array(share_times_list: &Vec<f32>) -> f32 {
    share_times_list.iter().sum::<f32>() / share_times_list.len() as f32
}
fn perc_diff(hashrate: f32, hashrate_calc: f32) -> f32 {
    (100f32 * ((hashrate - hashrate_calc) / ((hashrate + hashrate_calc) / 2f32)).abs()).floor()
}
fn get_user_workers(username: &String) -> f32 {
    match USR_WORKERS.get(username) {
        None => {
            USR_WORKERS.insert(username.to_owned(), 1f32);
            1f32
        }
        Some(some) => some.value().clone(),
    }
}

fn get_ip_workers(ip: &String) -> f32 {
    match WORKERS.get(ip) {
        None => {
            WORKERS.insert(ip.to_string(), 1.0);
            1f32
        }
        Some(some) => some.value().clone(),
    }
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

fn max_f32(x1: f32, x2: f32, x3: f32) -> f32 {
    f32::max(f32::max(x1, x2), x3)
}
