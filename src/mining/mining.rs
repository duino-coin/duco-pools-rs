use std::cmp::max;
use std::ops::Add;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use arcstr::ArcStr;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use dashmap::DashMap;
use log::info;
use rand::{rng, Rng};
use regex::{Match, Regex};
use reqwest::get;
use serde_json::Value;
use sha1_smol::Sha1;
use crate::cfg;
use crate::cfg::config::PoolConfig;
use crate::data::data::Connection;
use crate::mining::stats::{BlockInfos, MinerStat};
use crate::kolka::kolka::Kolka;

// I hate that these are pub static, but I honestly can't be arsed to pass them as params..
pub static WORKERS: LazyLock<DashMap<String, f32>> = LazyLock::new(|| {
    DashMap::new()
});

pub static USR_WORKERS: LazyLock<DashMap<String, f32>> = LazyLock::new(|| { // TODO: maybe move from String to ArcStr?
    DashMap::new()
});

pub static BALANCES_TO_UPDATE: LazyLock<DashMap<String, f32>> = LazyLock::new(|| { // TODO: maybe move from String to ArcStr?
    DashMap::new()
});

pub static MINER_STATS: LazyLock<DashMap<String, MinerStat>> = LazyLock::new(|| { // TODO: maybe move from String to ArcStr?
    DashMap::new()
});


// I hate this, but passing this as param kinda seems annoying..
static LAST_BLOCK_HASH: LazyLock<Mutex<String>> = LazyLock::new(|| {
    let config = cfg::config::load_config().expect("failed to load config (static)");
    Mutex::new(config.initial_blockhash)
});

pub static GLOBAL_BLOCKS: LazyLock<Mutex<Vec<BlockInfos>>> = LazyLock::new(|| {
    Mutex::new(Vec::new())
});

pub static GLOBAL_SHARES_INCREASE: LazyLock<AtomicU64> = LazyLock::new(|| {
   AtomicU64::new(0)
});

pub static GLOBAL_SHARES_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| {
    AtomicU64::new(0)
});

const PC_DIFF_TIERS: [&str; 4] = ["EXTREME", "NET", "MEDIUM", "LOW"];

pub async fn handle(conn: &mut Connection, initial_data: Vec<&str>, pool_rewards: Arc<Value>, config: Arc<PoolConfig>, kolka: Arc<Kolka>, using_avr: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut random;
    let mut new_hash;
    let mut req_difficulty= "".to_string();
    let mut mining_key;
    let mut this_miner_chipid= "".to_string();
    let mut miner_name;
    let mut expected_share_time= 0;
    let mut feedback_sent = SystemTime::now();
    let mut start_hashrate = -1i64; // i32 limits it to a max of ~ 4 GH/s
    let mut diff = 1u32;
    let mut reported_hashrate;
    let mut hashrate_calc;
    let mut reward= 0f32;
    let mut hashrate_is_estimated;
    let mut hashrate;
    let mut is_first_share = true;
    // let mut miner_stat = &mut conn.miner_stat;

    let ip = conn.addr.ip().to_string();
    let iot_regex: Regex = Regex::new("/[A-Za-z0-9 .():@-]+/")?;
    let name_regex = Regex::new("/[A-Za-z0-9 .()-]+/g")?;
    let mut buffer = itoa::Buffer::new(); // used to convert nums to str a little faster
    // miner_stat.accepted_shares = 0;
    // miner_stat.rejected_shares = 0;
    //
    // miner_stat.server_miners = 0;
    // miner_stat.this_miner_id = 0;
    // miner_stat.last_min_shares = 0;
    // miner_stat.last_share_reset = 0;
    // miner_stat.sharetimes_list = Vec::new();
    // miner_stat.ping = 0;

    while true
    {
        let read; // needed for first share..
        let mut data;
        if is_first_share
        {
            req_difficulty = if initial_data[2].is_empty() { "NET".to_string() } else { initial_data[2].to_string() };
            if !pool_rewards.get(&req_difficulty).is_some()
            {
                req_difficulty = "NET".to_string();
            }
            diff = get_diff(&pool_rewards, &req_difficulty);

            let mut worker_add = 1f32;
            if req_difficulty == "ESP32" // why??
            {
                worker_add = 0.5;
            }

            *WORKERS.entry(ip.clone()).or_insert(0f32) += worker_add;
            *USR_WORKERS.entry(conn.username.to_string()).or_insert(0f32) += worker_add;
            println!("{}", WORKERS.get(&ip).unwrap().value());
            if ip != "127.0.0.1"
            {
                conn.miner_stat.amount_of_workers = f32::max(
                    f32::max(
                        get_user_workers(&conn.username),
                        get_ip_workers(&ip)),
                    conn.miner_stat.server_miners as f32); // this is horrible
            }
            else
            {
                conn.miner_stat.amount_of_workers = f32::max(get_user_workers(&conn.username),
                                                             conn.miner_stat.server_miners as f32);

            }
        }
        else
        {
            read = conn.read_to_string().await?;
            data = read.split(",").collect::<Vec<&str>>(); // receive new data and split by "args"

            conn.miner_stat.ping = SystemTime::now().duration_since(feedback_sent).expect("failed to get time").as_secs_f32(); // TODO: check if this is really seconds

            if !conn.miner_stat.override_diff.is_empty()
            {
                let override_diff = conn.miner_stat.override_diff.clone();
                req_difficulty = override_diff;
            }
            else if using_avr
            {
                req_difficulty = "AVR".to_string();
            }
        }

        if initial_data.len() >= 4
        {
            mining_key = BASE64_STANDARD.encode(&initial_data[3]);
        }
        else {
            mining_key = String::new();
        }
        conn.miner_stat.password = mining_key.clone();

        if initial_data.len() >= 5 // IoT reading stuff
        {
            if iot_regex.find(initial_data[4]).is_some()
            {
                conn.miner_stat.iot_reading = "Error:incorrect data".to_string();
            }
            else if initial_data[4].len() > 48
            {
                conn.miner_stat.iot_reading = "Error:data too long".to_string();
            }
            else
            {
                conn.miner_stat.iot_reading = initial_data[4].to_string();
            }
        }

        let ip_workers = if ip != "127.0.0.1" { get_ip_workers(&ip) } else { 0f32 };
        let user_workers = get_user_workers(&conn.username);
        let max = max_f32(ip_workers, user_workers, conn.miner_stat.server_miners as f32);
        if check_workers(max, &config)
        {
            conn.miner_stat.reject_shares = "Too many workers".to_string();
        }

        if conn.miner_stat.amount_of_workers > max
        {
            conn.miner_stat.amount_of_workers = max;
        }

        random = rng().random_range(0..diff*100) + 1;
        let job;
        { // used to lose the lock on LAST_BLOCK_HASH
            let last_block_hash = LAST_BLOCK_HASH.lock().unwrap().clone();
            let mut hasher = Sha1::from(&last_block_hash);
            hasher.update(buffer.format(random).as_bytes());
            new_hash = hasher.hexdigest();
            job = (last_block_hash, new_hash.clone(), diff);
            // job = vec![last_block_hash, new_hash.clone(), diff];
        }

        conn.write(&format!("{},{},{}\n", job.0, job.1, job.2)).await?;

        let job_sent = SystemTime::now();
        let read = conn.read_to_string().await?;
        data = read.split(",").collect::<Vec<&str>>();
        let answer_received = SystemTime::now();

        conn.miner_stat.share_time = answer_received.duration_since(job_sent).expect("failed to get share time").as_secs_f32();
        if conn.miner_stat.share_time > conn.miner_stat.ping
        {
            conn.miner_stat.share_time -= conn.miner_stat.ping;
        }

        conn.miner_stat.share_times_list.push(conn.miner_stat.share_time *1.5);

        if conn.miner_stat.share_times_list.len() > 50
        {
            let _ = conn.miner_stat.share_times_list.pop();
        }

        reported_hashrate = data[1].parse::<f32>().expect("failed to parse hashrate");

        if reported_hashrate <= 0f32
        {
            conn.miner_stat.reject_shares = "Invalid data".to_string();
        }

        if conn.miner_stat.share_time >= 0.5
        {
            hashrate_calc = (random as f32 / conn.miner_stat.share_time).round() as u32;
        }
        else
        {
            hashrate_calc = reported_hashrate.round() as u32;
        }

        conn.miner_stat.last_min_shares += 1;

        let mut nonce;
        if using_avr
        {
            nonce = u32::from_str_radix(data[0], 2)?;
        }
        else
        {
            nonce = u32::from_str(data[0])?;
        }

        if conn.miner_stat.last_min_shares > config.max_shares_per_minute
        {
            conn.miner_stat.reject_shares = "Too low difficulty".to_string();
        }

        hashrate_is_estimated = false;
        hashrate = hashrate_calc;
        if reported_hashrate <= 0f32
        {
            hashrate_is_estimated = false;
        }

        if is_first_share
        {
            this_miner_chipid = data[4].to_string();
        }

        if diff > get_diff(&pool_rewards, "DUE") &&
            diff <= get_diff(&pool_rewards, "ESP8266NH") &&
            perc_diff(hashrate as f32, hashrate_calc as f32) > 15f32 && conn.miner_stat.share_time >= 1.5
        {
            conn.miner_stat.reject_shares = "Modified hashrate".to_string();
        }

        if (conn.miner_stat.accepted_shares == 4 || (conn.miner_stat.accepted_shares > 4 && perc_diff(hashrate as f32, hashrate_calc as f32) > 15f32))
            &&  PC_DIFF_TIERS.contains(&req_difficulty.clone().as_str())
        {
            start_hashrate = hashrate as i64;

            diff = (*kolka).clone().v3(avg_from_array(&conn.miner_stat.share_times_list), expected_share_time, hashrate);
        }

        let reward_div = pool_rewards
            .get(&req_difficulty).unwrap().as_object().expect("failed to get diff as obj")
            .get("reward").expect("failed to get reward").as_u64().expect("failed to get reward as u64") as u32;
        let max_hashrate = pool_rewards
            .get(&req_difficulty).unwrap().as_object().expect("failed to get diff as obj")
            .get("max_hashrate").expect("failed to get reward").as_u64().expect("failed to get reward as u64");
        let min_hashrate = pool_rewards
            .get(&req_difficulty).unwrap().as_object().expect("failed to get diff as obj")
            .get("min_hashrate").expect("failed to get reward").as_u64().expect("failed to get reward as u64") as u32;
        expected_share_time = pool_rewards
            .get(&req_difficulty).unwrap().as_object().expect("failed to get diff as obj")
            .get("expected_sharetime").expect("failed to get reward").as_u64().expect("failed to get reward as u64") as u16;
        let block_probability = pool_rewards
            .get(&req_difficulty).unwrap().as_object().expect("failed to get diff as obj")
            .get("block_chance").expect("failed to get reward").as_u64().expect("failed to get reward as u64") as u32;
        reward = 0f32;
        if !conn.miner_stat.reject_shares.is_empty()
        {
            conn.write(&format!("BAD,{}\n", conn.miner_stat.reject_shares)).await?;
            feedback_sent = SystemTime::now();
        }
        else if hashrate < min_hashrate
        {
            conn.miner_stat.override_diff = (*kolka).clone().v2_reverse(&req_difficulty);
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too high starting difficulty\n").await?;
            start_hashrate = -1i64;
            diff = get_diff(&pool_rewards, &conn.miner_stat.override_diff);
            feedback_sent = SystemTime::now();
        }
        else if hashrate as u64 >= max_hashrate
        {
            conn.miner_stat.override_diff = (*kolka).clone().v2(&req_difficulty);
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Too low starting difficulty\n").await?;
            start_hashrate = -1;
            diff = get_diff(&pool_rewards, &conn.miner_stat.override_diff);
            feedback_sent = SystemTime::now();
        }
        else if nonce == random
        {
            conn.miner_stat.accepted_shares += 1;
            if conn.miner_stat.accepted_shares > 10
            {
                if diff <= get_diff(&pool_rewards, "ESP8266NH")
                {
                    if this_miner_chipid.is_empty()
                    {
                        conn.miner_stat.rejected_shares += 1;
                    }
                    else if data[4] != this_miner_chipid
                    {
                        conn.miner_stat.rejected_shares += 1;
                    }
                    else
                    {
                        reward = (*kolka).clone().v1(hashrate_calc as f32, diff, conn.miner_stat.amount_of_workers, reward_div);
                    }
                }
                else
                {
                    reward = (*kolka).clone().v1(hashrate_calc as f32, diff, conn.miner_stat.amount_of_workers, reward_div);
                }
            }

            if (rng().random::<f32>() * block_probability as f32) == 1f32
            {
                reward += config.block_reward;

                let block_infos = BlockInfos::new(
                    UNIX_EPOCH.duration_since(SystemTime::now()).unwrap().as_secs(),
                    conn.username.to_string(), reward, "DUCO-S1".to_string(), new_hash.clone());
                GLOBAL_BLOCKS.lock()?.push(block_infos);
                conn.write("BLOCK\n").await?;
                info!("Block found by {}", conn.username);
                feedback_sent = SystemTime::now();
            }
            else
            {
                conn.write("GOOD\n").await?;
                feedback_sent = SystemTime::now();
            }
        }
        else
        {
            conn.miner_stat.rejected_shares += 1;
            conn.write("BAD,Incorrect result\n").await?;
            feedback_sent = SystemTime::now();
        }

        if conn.miner_stat.accepted_shares > 0 && conn.miner_stat.accepted_shares % config.update_miners_stats_every as u64 == 0
        {
            let _ = BALANCES_TO_UPDATE.entry(conn.username.to_string()).or_insert(0f32).add(reward);

            miner_name = match name_regex.find(data[2]) {
                None => {"Unknown miner".to_string()}
                Some(some) => {some.as_str().to_string()}
            };
            conn.miner_stat.software = miner_name.to_string();

            let wallet_id = if data.len() >= 5 { data[5] } else { "None" };
            conn.miner_stat.wallet_id = wallet_id.to_string().replace("\n", "");

            let cut_rigid = data[3].split(":").collect::<Vec<&str>>();
            let rig_identifier = match name_regex.find(cut_rigid[0]) {
                None => {"None".to_string()}
                Some(some) => {some.as_str().to_string()}
            };
            conn.miner_stat.id = rig_identifier;
            conn.miner_stat.algorithm = "DUCO-S1".to_string();
            conn.miner_stat.hashrate = hashrate as u64;
            conn.miner_stat.difficulty = diff;
            if cut_rigid.len() >= 2
            {
                conn.miner_stat.password = BASE64_STANDARD.encode(cut_rigid[1].as_bytes());
            }

            if UNIX_EPOCH.elapsed().unwrap().as_secs() - conn.miner_stat.last_share_reset >= 60
            {
                conn.miner_stat.last_share_reset = UNIX_EPOCH.elapsed().unwrap().as_secs();
                conn.miner_stat.last_min_shares = 0;
            }

            conn.miner_stat.kolka_drop = conn.miner_stat.amount_of_workers;
            conn.miner_stat.this_miner_chipid = this_miner_chipid.clone();
            conn.miner_stat.reward = reward;
            conn.miner_stat.ip = conn.addr.ip().to_string();
            conn.miner_stat.username = conn.username.to_string();
            MINER_STATS.insert(conn.id.to_string(), conn.miner_stat.clone());

            let mut last_block_hash = LAST_BLOCK_HASH.lock().unwrap();
            *last_block_hash = new_hash;
            GLOBAL_SHARES_INCREASE.fetch_add(config.update_miners_stats_every as u64, std::sync::atomic::Ordering::Relaxed);
            GLOBAL_SHARES_TOTAL.fetch_add(config.update_miners_stats_every as u64, std::sync::atomic::Ordering::Relaxed);

        }

        is_first_share = false;



    }

    Ok(())
}


fn avg_from_array(share_times_list: &Vec<f32>) -> f32
{
    share_times_list.clone().iter().sum::<f32>() / share_times_list.len() as f32
}
fn perc_diff(hashrate: f32, hashrate_calc: f32) -> f32
{
    (100f32* ((hashrate - hashrate_calc) / ((hashrate + hashrate_calc) / 2f32)).abs()).floor()
}
fn get_user_workers(username: &ArcStr) -> f32
{
    USR_WORKERS.get(&username.to_string()).expect("failed to get workers by username").value().clone()
}

fn get_ip_workers(ip: &String) -> f32
{
    WORKERS.get(ip).expect("failed to get workers by ip").value().clone()
}

fn get_diff(pool_rewards: &Arc<Value>, req_diff: &str) -> u32
{
    let diff = pool_rewards.get(req_diff).unwrap().get("difficulty").unwrap().as_u64().unwrap();
    diff as u32
}

fn check_workers(max: f32, config: &PoolConfig) -> bool
{
    if config.max_workers <= 0
    {
        return false;
    }

    if max > config.max_workers as f32
    {
        return true;
    }
    false
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

fn max_f32(x1: f32, x2: f32, x3: f32) -> f32
{
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