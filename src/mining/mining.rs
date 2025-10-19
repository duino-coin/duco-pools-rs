use crate::cfg;
use crate::cfg::config::PoolConfig;
use crate::data::data::Connection;
use crate::kolka::kolka::Kolka;
use crate::mining::stats::{BlockInfos, MinerStat};
use arcstr::ArcStr;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use dashmap::DashMap;
use log::{debug, info};
// use rand::{rng, Rng};
// use regex::{Regex};
use serde_json::{Value};
use sha1_smol::Sha1;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};
use rand::{rng, Rng};
use tokio::sync::Mutex;

// I hate that these are pub static, but I honestly can't be arsed to pass them as params..
pub static WORKERS: LazyLock<DashMap<String, f32>> = LazyLock::new(|| DashMap::new());

pub static USR_WORKERS: LazyLock<DashMap<String, f32>> = LazyLock::new(|| {
    // TODO: maybe move from String to ArcStr?
    DashMap::new()
});

pub static BALANCES_TO_UPDATE: LazyLock<DashMap<String, f32>> = LazyLock::new(|| {
    // TODO: maybe move from String to ArcStr?
    DashMap::new()
});

pub static MINER_STATS: LazyLock<DashMap<String, MinerStat>> = LazyLock::new(|| {
    // TODO: maybe move from String to ArcStr?
    DashMap::new()
});

// I hate this, but passing this as param kinda seems annoying..
static LAST_BLOCK_HASH: LazyLock<Mutex<String>> = LazyLock::new(|| {
    let config = cfg::config::load_config().expect("failed to load config (static)");
    Mutex::new(config.initial_blockhash)
});

pub static GLOBAL_BLOCKS: LazyLock<Mutex<Vec<BlockInfos>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

pub static GLOBAL_SHARES_INCREASE: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

pub static GLOBAL_SHARES_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

// static IOT_REGEX: LazyLock<Regex> = LazyLock::new(|| {Regex::new("[A-Za-z0-9 .():@-]+").expect("Failed to compile IoT regex")});
// static NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| { Regex::new("[A-Za-z0-9 .()-]+").expect("Failed to compile name regex")});

const PC_DIFF_TIERS: [&str; 4] = ["EXTREME", "NET", "MEDIUM", "LOW"];

pub async fn handle(
    conn: &mut Connection,
    initial_data: Vec<&str>,
    pool_rewards: Arc<Value>,
    config: Arc<PoolConfig>,
    kolka: Arc<Kolka>,
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
    let mut start_hashrate = -1i64; // i32 limits it to a max of ~ 4 GH/s
    let mut diff = 1u32;
    let mut reported_hashrate;
    let mut hashrate_calc;
    let mut reward: f32;
    let mut hashrate_is_estimated;
    let mut hashrate;
    let mut is_first_share = true;

    // let ip = conn.addr.ip().to_string();
    // let iot_regex: Regex = Regex::new("[A-Za-z0-9 .():@-]+").expect("Failed to compile IoT regex");
    // let name_regex = Regex::new("[A-Za-z0-9 .()-]+").expect("Failed to compile name regex");
    // let mut rng = Pcg64Mcg::from_rng(&mut rand::rng()); // not cryptographically secure, but I don't think we need it for this?
    // let mut rng = Biski64Rng::from_rng(&mut rand::rng());
    // let mut rng = rand::rng();
    let mut buffer = itoa::Buffer::new(); // used to convert nums to str a little faster

    loop {
        let read; // needed for first share..
        let mut data;
        if is_first_share {
            req_difficulty = if initial_data.len() < 3 || initial_data[2].is_empty() {
                "NET".to_string()
            } else {
                initial_data[2].to_string()
            };
            if !pool_rewards.get(&req_difficulty).is_some() {
                req_difficulty = "NET".to_string();
            }
            diff = get_diff(&pool_rewards, &req_difficulty);

            let mut worker_add = 1f32;
            if req_difficulty == "ESP32"
            // why??
            {
                worker_add = 0.5;
            }

            *WORKERS.entry(conn.ip.clone()).or_insert(0f32) += worker_add;
            *USR_WORKERS.entry(conn.username.to_string()).or_insert(0f32) += worker_add;
            // println!("{}", WORKERS.get(&ip).unwrap().value());
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
            data = initial_data.iter().map(|x| x.to_string()).collect::<Vec<String>>().clone();
        } else {
            read = conn.read_to_string().await?;
            data = read.split(",").map(|x| x.replace("\n", "")).collect::<Vec<String>>(); // receive new data and split by "args"
            // let test = read.split(",");
            // TODO: check if this works
            req_difficulty = if data.len() < 3 || data[2].is_empty() {
                "NET".to_string()
            } else {
                data[2].to_string()
            };

            if !pool_rewards.get(&req_difficulty).is_some() {
                req_difficulty = "NET".to_string();
            }

            conn.miner_stat.ping = SystemTime::now()
                .duration_since(feedback_sent)?
                .as_secs_f32();

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
        conn.miner_stat.password = mining_key.clone();

        if data.len() >= 5
        // IoT reading stuff
        {
            let cleaned_iot = clean_iot(&data[4]);
            // originally: IOT_REGEX.find(data[4]).is_none()
            if cleaned_iot.is_empty() {
                conn.miner_stat.iot_reading = "Error:incorrect data".to_string();
            } else if data[4].len() > 48 {
                conn.miner_stat.iot_reading = "Error:data too long".to_string();
            } else {
                conn.miner_stat.iot_reading = data[4].to_string();
            }
        }

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
            conn.miner_stat.amount_of_workers = max;
        }

        random = rng().random_range(0..diff * 100) + 1;
        // random = rng.random_range(0..diff * 100) + 1;
        #[cfg(run_env = "bench")] {
            random = 1;
        }
        let job;
        {
            // used to lose the lock on LAST_BLOCK_HASH
            let last_block_hash = LAST_BLOCK_HASH.lock().await.clone();
            let mut hasher = Sha1::from(&last_block_hash);
            hasher.update(buffer.format(random).as_bytes());
            new_hash = hasher.hexdigest();
            job = (last_block_hash, new_hash.clone());
        }

        { // using a block to drop diff_str and job_str faster
            let diff_str = buffer.format(diff);
            let mut job_str = String::with_capacity(",,,\n".len() + job.0.len() + job.1.len() + diff_str.len()); // faster than format macro
            job_str.push_str(job.0.as_str());
            job_str.push_str(",");
            job_str.push_str(job.1.as_str());
            job_str.push_str(",");
            job_str.push_str(diff_str);
            job_str.push_str("\n");

            conn.write(&job_str).await?;
            // conn.write(&format!("{},{},{}\n", job.0, job.1, job.2))
            //     .await?;
        }


        let job_sent = SystemTime::now();
        let read = conn.read_to_string().await?;
        // data = read.split(",").collect::<Vec<&str>>();
        data = read.split(",").map(|x| x.replace("\n", "")).collect::<Vec<String>>();
        let answer_received = SystemTime::now();

        conn.miner_stat.share_time = answer_received
            .duration_since(job_sent)?
            .as_secs_f32();
        if conn.miner_stat.share_time > conn.miner_stat.ping {
            conn.miner_stat.share_time -= conn.miner_stat.ping;
        }

        conn.miner_stat
            .share_times_list
            .push(conn.miner_stat.share_time * 1.5);

        if conn.miner_stat.share_times_list.len() > 50 {
            let _ = conn.miner_stat.share_times_list.pop();
        }

        let mut wrong_reported_hashrate = false;
        reported_hashrate = match data[1].parse::<f32>() {
            Ok(rep) => { rep }
            Err(_) => {
                wrong_reported_hashrate = true;
                0f32
            }
        };

        if reported_hashrate <= 0f32 && !wrong_reported_hashrate {
            conn.miner_stat.reject_shares = "Invalid data".to_string();
        }

        if conn.miner_stat.share_time >= 0.5 || wrong_reported_hashrate {
            hashrate_calc = (random as f32 / conn.miner_stat.share_time).round() as u32;

            if wrong_reported_hashrate {
                reported_hashrate = hashrate_calc as f32;
            }
        } else {
            hashrate_calc = reported_hashrate.round() as u32;
        }

        conn.miner_stat.last_min_shares += 1;

        let nonce;
        if using_avr {
            nonce = u32::from_str_radix(&data[0], 2)?;
        } else {
            nonce = match u32::from_str(&data[0]) {
                Ok(ok) => { ok }
                Err(err) => {
                    debug!("Shutting down due to nonce: {err:?}");
                    conn.shutdown().await?; // close connection, seems like a faulty miner
                    // (some request a job and after receiving the job they immediately request another one)
                    break;
                }
            };
        }
        #[cfg(
            not(run_env = "bench")
        )] // if the pool was built with "run_env" set as bench, we discard this codeblock
        {
            if conn.miner_stat.last_min_shares > config.max_shares_per_minute {
                conn.miner_stat.reject_shares = "Too low difficulty".to_string();
            }
        }


        hashrate_is_estimated = false;
        hashrate = hashrate_calc;
        if reported_hashrate <= 0f32 {
            hashrate_is_estimated = false;
        }

        if is_first_share {
            this_miner_chipid = if data.len() >= 5 { data[4].to_string() } else { "".to_string() };
        }

        if diff > get_diff(&pool_rewards, "DUE")
            && diff <= get_diff(&pool_rewards, "ESP8266NH")
            && perc_diff(hashrate as f32, hashrate_calc as f32) > 15f32
            && conn.miner_stat.share_time >= 1.5
        {
            conn.miner_stat.reject_shares = "Modified hashrate".to_string();
        }

        if (conn.miner_stat.accepted_shares == 4
            || (conn.miner_stat.accepted_shares > 4
            && perc_diff(hashrate as f32, start_hashrate as f32) > 15f32))
            && PC_DIFF_TIERS.contains(&req_difficulty.clone().as_str())
        {
            start_hashrate = hashrate as i64;

            diff = (*kolka).clone().v3(
                avg_from_array(&conn.miner_stat.share_times_list),
                expected_share_time,
                hashrate,
            );
        }

        let reward_div = pool_rewards
            .get(&req_difficulty)
            .unwrap()
            .as_object()
            .expect("failed to get diff as obj")
            .get("reward")
            .expect("failed to get reward")
            .as_u64()
            .expect("failed to get reward as u64") as u32;
        let max_hashrate = pool_rewards
            .get(&req_difficulty)
            .unwrap()
            .as_object()
            .expect("failed to get diff as obj")
            .get("max_hashrate")
            .expect("failed to get reward")
            .as_u64()
            .expect("failed to get reward as u64");
        let min_hashrate = pool_rewards
            .get(&req_difficulty)
            .unwrap()
            .as_object()
            .expect("failed to get diff as obj")
            .get("min_hashrate")
            .expect("failed to get reward")
            .as_u64()
            .expect("failed to get reward as u64") as u32;
        expected_share_time = pool_rewards
            .get(&req_difficulty)
            .unwrap()
            .as_object()
            .expect("failed to get diff as obj")
            .get("expected_sharetime")
            .expect("failed to get reward")
            .as_u64()
            .expect("failed to get reward as u64") as u16;
        let block_probability = pool_rewards
            .get(&req_difficulty)
            .unwrap()
            .as_object()
            .expect("failed to get diff as obj")
            .get("block_chance")
            .expect("failed to get reward")
            .as_u64()
            .expect("failed to get reward as u64") as u32;
        reward = 0f32;
        if !conn.miner_stat.reject_shares.is_empty() {
            let mut res = String::with_capacity("BAD,\n".len() + conn.miner_stat.reject_shares.len()); // this is faster than the format macro (ufmt is possibly a faster alternative?)
            res.push_str("BAD,");
            res.push_str(conn.miner_stat.reject_shares.as_str());
            res.push_str("\n");

            conn.write(&res)
                .await?;
            feedback_sent = SystemTime::now();
        } else if hashrate < min_hashrate {
            conn.miner_stat.override_diff = (*kolka).clone().v2_reverse(&req_difficulty);
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too high starting difficulty\n").await?;
            start_hashrate = -1i64;
            diff = get_diff(&pool_rewards, &conn.miner_stat.override_diff);
            feedback_sent = SystemTime::now();
        } else if hashrate as u64 >= max_hashrate {
            conn.miner_stat.override_diff = (*kolka).clone().v2(&req_difficulty);
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too low starting difficulty\n").await?;
            start_hashrate = -1;
            diff = get_diff(&pool_rewards, &conn.miner_stat.override_diff);
            feedback_sent = SystemTime::now();
        } else if nonce == random {
            conn.miner_stat.accepted_shares += 1;
            if conn.miner_stat.accepted_shares > 10 {
                if diff <= get_diff(&pool_rewards, "ESP8266NH") {
                    if this_miner_chipid.is_empty() {
                        conn.miner_stat.rejected_shares += 1;
                    } else if data.len() >= 5 && data[4] != this_miner_chipid {
                        conn.miner_stat.rejected_shares += 1;
                    } else {
                        reward = (*kolka).clone().v1(
                            hashrate_calc as f32,
                            diff,
                            conn.miner_stat.amount_of_workers,
                            reward_div,
                        );
                    }
                } else {
                    reward = (*kolka).clone().v1(
                        hashrate_calc as f32,
                        diff,
                        conn.miner_stat.amount_of_workers,
                        reward_div,
                    );
                }
            }
            // was previously: rng().random_range(0f32..1f32)
            if (rng().random_range(0f32..1f32) * block_probability as f32).floor() == 1f32 {
                reward += config.block_reward;

                let block_infos = BlockInfos::new(
                    UNIX_EPOCH
                        .duration_since(SystemTime::now())?
                        .as_secs(),
                    conn.username.to_string(),
                    reward,
                    "DUCO-S1".to_string(),
                    new_hash.clone(),
                );
                GLOBAL_BLOCKS.lock().await.push(block_infos);
                conn.write("BLOCK\n").await?;
                info!("Block found by {}", conn.username);
                feedback_sent = SystemTime::now();
            } else {
                conn.write("GOOD\n").await?;
                feedback_sent = SystemTime::now();
            }
        } else {
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Incorrect result\n").await?;
            feedback_sent = SystemTime::now();
        }

        if conn.miner_stat.accepted_shares > 0
            && conn.miner_stat.accepted_shares % config.update_miners_stats_every as u64 == 0
        {
            *BALANCES_TO_UPDATE
                .entry(conn.username.to_string())
                .or_insert(0f32) += reward;

            let cleaned_name = clean_name(&data[2]);

            miner_name = if data.len() >= 3 {
                match cleaned_name.as_str() {
                    "" => {"Unknown miner".to_string()}
                    _ => { cleaned_name }
                }
                // match NAME_REGEX.find(data[2]) {
                //     None => "Unknown miner".to_string(),
                //     Some(some) => some.as_str().to_string(),
                // }
            } else { "Unknown miner".to_string() };
            conn.miner_stat.software = miner_name.to_string();

            let wallet_id = if data.len() >= 6 { &data[5] } else { "None" };
            conn.miner_stat.wallet_id = wallet_id.to_string().replace("\n", "");

            let cut_rigid = if data.len() >= 4 { data[3].split(":").collect::<Vec<&str>>() } else { Vec::new() };
            let rig_identifier = if cut_rigid.len() > 0 {
                let cleaned = clean_name(cut_rigid[0]);

                match cleaned.as_str() {
                    "" => {"None".to_string()}
                    _ => {cleaned}
                }
                // match NAME_REGEX.find(cut_rigid[0]) {
                //     None => "None".to_string(),
                //     Some(some) => some.as_str().to_string(),
                // }
            } else { "None".to_string() };
            conn.miner_stat.id = rig_identifier;
            conn.miner_stat.algorithm = "DUCO-S1".to_string();
            conn.miner_stat.hashrate = if hashrate_is_estimated {
                hashrate as u64
            } else {
                reported_hashrate as u64
            };
            conn.miner_stat.difficulty = diff;
            if cut_rigid.len() >= 2 {
                conn.miner_stat.password = BASE64_STANDARD.encode(cut_rigid[1].as_bytes());
            }

            if UNIX_EPOCH.elapsed()?.as_secs() - conn.miner_stat.last_share_reset >= 60 {
                conn.miner_stat.last_share_reset = UNIX_EPOCH.elapsed()?.as_secs();
                conn.miner_stat.last_min_shares = 0;
            }

            conn.miner_stat.kolka_drop = conn.miner_stat.amount_of_workers;
            conn.miner_stat.this_miner_chipid = this_miner_chipid.clone();
            conn.miner_stat.reward = reward;
            conn.miner_stat.ip = conn.ip.clone();
            conn.miner_stat.username = conn.username.to_string();
            MINER_STATS.insert(conn.id.to_string(), conn.miner_stat.clone());

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
    }

    Ok(())
}

fn avg_from_array(share_times_list: &Vec<f32>) -> f32 {
    share_times_list.clone().iter().sum::<f32>() / share_times_list.len() as f32
}
fn perc_diff(hashrate: f32, hashrate_calc: f32) -> f32 {
    (100f32 * ((hashrate - hashrate_calc) / ((hashrate + hashrate_calc) / 2f32)).abs()).floor()
}
fn get_user_workers(username: &ArcStr) -> f32 {
    let usr = username.to_string();
    match USR_WORKERS
        .get(&usr) {
        None => {
            USR_WORKERS.insert(usr, 1f32);
            1f32
        },
        Some(some) => {some
            .value()
            .clone()}
    }
}

fn get_ip_workers(ip: &String) -> f32 {
    match WORKERS
        .get(ip) {
        None => {
            WORKERS.insert(ip.to_string(), 1.0);
            1f32
        }
        Some(some) => {some.value()
            .clone()}
    }

}

fn get_diff(pool_rewards: &Arc<Value>, req_diff: &str) -> u32 {
    let diff = pool_rewards
        .get(req_diff)
        .unwrap()
        .get("difficulty")
        .unwrap()
        .as_u64()
        .unwrap();
    diff as u32
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

//static IOT_REGEX: LazyLock<Regex> = LazyLock::new(|| {Regex::new("[A-Za-z0-9 .():@-]+").expect("Failed to compile IoT regex")});
// static NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| { Regex::new("[A-Za-z0-9 .()-]+").expect("Failed to compile name regex")});

fn clean_iot(input: &str) -> String
{
    const ALLOWED_SPECIAL: [&str; 7] = [".", "(", ")", ":", "@", "-", " "];
    input.chars().into_iter().filter(|x| ALLOWED_SPECIAL.contains(&x.to_string().as_str()) || x.is_alphanumeric()).collect::<String>()
}

fn clean_name(input: &str) -> String
{
    const ALLOWED_SPECIAL: [&str; 5] = [".", "(", ")", "-", " "];
    input.chars().into_iter().filter(|x| ALLOWED_SPECIAL.contains(&x.to_string().as_str()) || x.is_alphanumeric()).collect::<String>()
}
// fn check_workers(ip_workers: f32, user_workers: f32, server_miners: u64, config: &PoolConfig) -> bool
// {
//     if config.max_workers <= 0
//     {
//         return false;
//     }
//
//     if max_f32(ip_workers, user_workers, server_miners as f32) > config.max_workers as f32
//     {
//         return true;
//     }
//     false
// }

fn max_f32(x1: f32, x2: f32, x3: f32) -> f32 {
    f32::max(f32::max(x1, x2), x3)
}

//const checkWorkers = (ipWorkers, usrWorkers, serverMiners, username) => {
//     if (maxWorkers <= 0) return false;
//
//     if (Math.max(ipWorkers, usrWorkers, serverMiners) > maxWorkers) {
//         return true;
//
//     }
//     return false;
// };
