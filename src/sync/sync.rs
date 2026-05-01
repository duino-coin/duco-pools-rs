use crate::cfg::config::PoolConfig;
use crate::data::data::Bans;
use crate::mining::mining;
use crate::{data, CONFIG, CONNECTIONS, POOL_REWARDS};
use data::data::LoginInfos;
use log::{debug, error, info, warn};
use reqwest::multipart;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::process::exit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

static SYNC_COUNT: AtomicUsize = AtomicUsize::new(0);

//#[serde(skip_serializing_if = "Option::is_none")]
#[derive(Serialize, Deserialize, Debug)]
pub struct Difficulty {
    pub difficulty: u64,
    pub reward: u32,
    pub max_hashrate: u64,
    pub min_hashrate: u64,
    pub kolka_decrease_perc: u8,
    #[serde(rename = "expected_sharetime")]
    pub expected_share_time: u16,
    pub block_chance: u32,
}

pub async fn get_pool_ip(config: &&PoolConfig) -> Result<String, Box<dyn std::error::Error>> {
    debug!("Getting ip..");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout as u64))
        .build()?;
    let response = client.get("https://api.ipify.org/").send().await?;

    let text = response.text().await?;
    Ok(text)
}

pub async fn login(config: &PoolConfig) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Gathering login data..");
    let ip;
    if config.use_ngrok {
        // ip = "".to_string();
        return Err(Box::from("Ngrok not implemented"));
    } else {
        ip = get_pool_ip(&config).await?;
    }
    let infos = LoginInfos::new(
        &ip,
        config.port,
        &config.pool_version,
        &config.pool_id,
        &config.pool_name,
    );
    info!("Logging in..");
    let mut socket = TcpStream::connect(format!("{}:{}", config.server_ip, config.server_port))
        .await
        .unwrap();
    debug!("Connected to server");
    let mut reader = BufReader::new(&mut socket);
    let received: Vec<u8> = reader.fill_buf().await?.to_vec();
    debug!("Received: {}", received.len());
    reader.consume(received.len());

    let data = String::from_utf8(received).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Couldn't parse received string as utf8",
        )
    })?;
    debug!("Received: {}", data);
    if data.starts_with(&config.server_version) {
        let info_str = serde_json::to_string(&infos)?;
        debug!("Sending {info_str}");
        socket
            .write_all(format!("PoolLogin,{info_str}").as_bytes())
            .await?;
        socket.flush().await?;
    }
    debug!("Waiting for response..");
    reader = BufReader::new(&mut socket); // no clue why I need to do it like this, but the borrow checker is happy now
    let received: Vec<u8> = reader.fill_buf().await?.to_vec();

    reader.consume(received.len());

    let data = String::from_utf8(received).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Couldn't parse received string as utf8",
        )
    })?;
    debug!("Received: {}", data);
    if data == "LoginOK" {
        info!("Successfully logged in");
    } else {
        error!("Unknown error, server returned {data} in login");
        exit(-1);
    }
    Ok(())
}

pub async fn logout(config: &PoolConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut socket =
        TcpStream::connect(format!("{}:{}", config.server_ip, config.server_port)).await?;
    let mut reader = BufReader::new(&mut socket);
    let received: Vec<u8> = reader.fill_buf().await?.to_vec();
    reader.consume(received.len());

    let data = String::from_utf8(received).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Couldn't parse received string as utf8",
        )
    })?;
    if data.starts_with(&config.server_version) {
        socket
            .write_all(format!("PoolLogout,{}", config.pool_id).as_bytes())
            .await?;
        socket.flush().await?;
    } else if data == "LogoutOK" {
        info!("Successfully logged out");
    } else {
        error!("Unknown error, server returned {data} in logout");
        return Err(Box::from("Unknown error"));
    }
    Ok(())
}

pub async fn resync_pool_rewards(config: &PoolConfig) -> Result<(), Box<dyn std::error::Error>> {
    let reward_content = update_pool_rewards(config).await?;
    let pool_rewards: HashMap<String, Difficulty> = serde_json::from_str(&reward_content)?;

    for (key, val) in pool_rewards {
        POOL_REWARDS.insert(key, val);
    }

    Ok(())
}

async fn update_pool_rewards(config: &PoolConfig) -> Result<String, Box<dyn std::error::Error>> {
    let response_data = reqwest::get(format!("{}/poolrewards.json", config.server_sync_url))
        .await?
        .text()
        .await?;
    std::fs::write("./config/poolRewards.json", &response_data)?;
    info!("Successfully updated pool rewards");

    let mut bans = Bans::load_bans().expect("failed to load config bans.json");

    let response_data_bans = reqwest::get(format!("{}/bans.json", config.server_sync_url))
        .await?
        .json::<HashSet<String>>()
        .await?;
    bans.banned_usernames = response_data_bans;

    bans.save_bans().expect("failed to save config bans.json");
    info!("Synced bans.");

    Ok(response_data)
}

pub async fn sync(
    sys: &mut Arc<Mutex<System>>,
    last_ip: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    info!("Starting sync..");
    let config = CONFIG.get().unwrap();
    if !std::fs::exists(&config.base_sync_folder)? {
        std::fs::create_dir_all(&config.base_sync_folder)?;
    }

    // with a standard sync delay of 15s, this is 300 seconds (or every 5 minutes)
    let sync_count = SYNC_COUNT.load(Ordering::Relaxed);
    // this is a dirty fix for the kolka_id "randomly" starting at a higher point..
    if sync_count != 0 && sync_count % 10 == 0 {
        // this entire thing roughly takes 150ms for 10k miners..
        // let mut mismatched = Vec::new();
        //
        //
        // let mut user_miner_count = HashMap::new();
        // let mut user_conn_ids = HashMap::new();
        let total_connections = mining::MINER_STATS.len();
        // let miner_stats_clone = mining::MINER_STATS.clone();

        // mining::MINER_STATS.iter().map(|x| (x.key().clone(), x.value().clone()))
        //     .for_each(|(id, stats)|{
        //         *user_miner_count.entry(stats.username.clone()).or_insert(0f32) += if stats.is_req_diff_esp32 { 0.5 } else { 1.0 };
        //         user_conn_ids.entry(stats.username).and_modify(|id_vec: &mut Vec<String>| id_vec.push(id.clone())).or_insert(vec![id]);
        //     });
        //
        //
        // user_miner_count.into_iter().for_each(|(username, count)|{
        //     if !mining::USR_WORKERS.contains_key(&username) || *mining::USR_WORKERS.get(&username).unwrap().value() != count {
        //         mismatched.push(username.clone());
        //         *mining::USR_WORKERS.entry(username).or_insert(count) = count;
        //     }
        // });

        // info!("Readjusted {} miners' assignment", mismatched.len());
        // for username in mismatched {
        //     // TODO: test
        //     let mut current = 0.0;
        //     if user_conn_ids.contains_key(&username) {
        //         // iterate through every connection associated with this username
        //         user_conn_ids.get(&username).unwrap().iter().for_each(|id|{
        //             // get the corresponding MinerStat struct
        //             mining::MINER_STATS.entry(id.to_string()).and_modify(|stat| {
        //                 let worker_add;
        //                 // determine the new kolka_id
        //                 if stat.is_req_diff_esp32 {
        //                     worker_add = 0.5;
        //                 } else {
        //                     worker_add = 1.0;
        //                 }
        //                 current += worker_add;
        //                 mining::SYNC_RELEGATION_MAP.insert(id.to_string(), current);
        //             });
        //         })
        //     }
        // }
        let mut current_id_map: HashMap<String, f32> = HashMap::new();

        mining::MINER_STATS.iter().map(|x| (x.key().clone(), x.value().clone()))
            .for_each(|(id, stat)|{

                let worker_add;
                // determine the new kolka_id
                if stat.is_req_diff_esp32 {
                    worker_add = 0.5;
                } else {
                    worker_add = 1.0;
                }
                let current = current_id_map.entry(stat.username.clone()).or_insert(0.0);
                *current += worker_add;
                mining::SYNC_RELEGATION_MAP.insert(id.to_string(), current.clone());
            });

    }

    let mut sys = sys.lock().await;
    let ip = match get_pool_ip(&config).await {
        Ok(ok) => ok,
        Err(err) => {
            if last_ip.is_empty() {
                return Err(Box::from(format!("Failed to get pool ip {err}")));
            }
            error!("Failed to get pool ip: {err}. Falling back to last ip collected.");
            last_ip.to_string()
        }
    };
    let block_increase = mining::GLOBAL_SHARES_INCREASE.load(Ordering::Relaxed);
    let big_blocks;
    {
        // used to drop lock faster
        let lock = mining::GLOBAL_BLOCKS.lock().await;
        big_blocks = lock.len();
    }

    let connections = CONNECTIONS.load(Ordering::Relaxed);
    // TODO: impl ngrok

    //    if (use_ngrok && sync_count > 0 && sync_count % 50 == 0)
    //         await get_ngrok_ip();

    // only refresh certain parts of the system info
    let mut refresh_kind = RefreshKind::nothing();

    let mut cpu_refresh = CpuRefreshKind::nothing();
    cpu_refresh = cpu_refresh.with_cpu_usage();

    let mut memory_refresh = MemoryRefreshKind::nothing();
    memory_refresh = memory_refresh.with_ram();

    refresh_kind = refresh_kind.with_cpu(cpu_refresh);
    refresh_kind = refresh_kind.with_memory(memory_refresh);

    sys.refresh_specifics(refresh_kind);
    let cpu_usage = sys.global_cpu_usage();

    let mem_usage =
        (sys.free_like_used_memory() as f32 / sys.total_memory() as f32 * 1e4).round() / 100.0; // rounding to two decimals
    mining::GLOBAL_SHARES_INCREASE.store(0, Ordering::Relaxed);

    let worker_path = format!(
        "{}/workers_{}.json",
        config.base_sync_folder, config.pool_name
    );

    let stats = mining::MINER_STATS.clone();
    // keep everything we won't sync
    mining::MINER_STATS.retain(|key, _| !stats.contains_key(key));
    mining::MINER_STATS.shrink_to_fit();
    let worker_content = serde_json::to_string(&stats)?;
    drop(stats);

    std::fs::write(&worker_path, &worker_content)?;

    let reward_path = format!(
        "{}/rewards_{}.json",
        config.base_sync_folder, config.pool_name
    );

    let request_url = format!(
        "https://{}/pool_sync/?host={}&port={}&version={}&identifier={}&name={}\
    &blockIncrease={}&bigBlocks={}&cpu={}&ram={}&connections={}",
        "server.duinocoin.com", //TODO: temp fix
        ip,
        config.port,
        config.pool_version,
        config.pool_id,
        config.pool_name,
        block_increase,
        big_blocks,
        cpu_usage,
        mem_usage,
        connections
    );
    let form = reqwest::multipart::Form::new();

    debug!("Workers: {worker_content}");
    let workers = multipart::Part::text(worker_content)
        .file_name(format!("workers_{}.json", config.pool_name));

    let balances = mining::BALANCES_TO_UPDATE.clone();
    let rewards_content = serde_json::to_string(&balances)?;

    std::fs::write(&reward_path, &rewards_content)?;

    debug!("Rewards: {rewards_content}");
    let rewards = multipart::Part::text(rewards_content)
        .file_name(format!("rewards_{}.json", config.pool_name));

    // Add the custom part to our form...
    let form = form.part("rewards", rewards).part("workers", workers);

    SYNC_COUNT.fetch_add(1, Ordering::Relaxed);
    debug!("Syncing to {request_url}");
    #[cfg(not(feature = "dummy_sync"))]
    {
        let client = reqwest::Client::new();
        match client
            .post(request_url)
            .multipart(form)
            .timeout(Duration::from_secs(config.timeout as u64))
            .send()
            .await
        {
            Ok(res) => {
                let status = res.status();
                if status.is_success() {
                    // remove all balances we've already synced
                    mining::BALANCES_TO_UPDATE.retain(|key, _| !balances.contains_key(key));
                    {
                        let mut block_lock = mining::GLOBAL_BLOCKS.lock().await;
                        block_lock.clear();
                        block_lock.shrink_to_fit();
                    }
                    drop(balances);
                    mining::BALANCES_TO_UPDATE.shrink_to_fit();
                    info!("Successfully synced {}", SYNC_COUNT.load(Ordering::Relaxed));
                    std::fs::remove_file(&reward_path)?; // so we don't accidentally sync the same thing twice
                    std::fs::remove_file(&worker_path)?;
                } else {
                    warn!(
                    "Failed to sync {}: {}",
                    SYNC_COUNT.load(Ordering::Relaxed),
                    res.text().await?
                );
                }
            }
            Err(err) => {
                error!("Error while syncing: {}", err);
                return Err(Box::new(err));
            }
        };
    }
    mining::USR_WORKERS.shrink_to_fit();
    // mining::WORKERS.shrink_to_fit();

    Ok(ip)
}
