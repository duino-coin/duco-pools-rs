use crate::cfg::config::PoolConfig;
use crate::data::data::Bans;
use crate::mining::mining;
use crate::{data, CONFIG, CONNECTIONS};
use data::data::LoginInfos;
use log::{debug, error, info, warn};
use reqwest::multipart;
use std::process::exit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

static SYNC_COUNT: AtomicUsize = AtomicUsize::new(0);

pub async fn get_pool_ip(config: &&PoolConfig) -> Result<String, Box<dyn std::error::Error>> {
    debug!("Getting ip..");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout as u64))
        .build()?;
    let response = client.get("https://api.ipify.org/").send().await?;

    Ok(response.text().await?)
}

pub async fn login(config: &PoolConfig) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Gathering login data..");
    let ip;
    if config.use_ngrok {
        ip = "".to_string();
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
        // socket.write(`PoolLogin,${JSON.stringify(loginInfos)}`);
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

pub async fn update_pool_rewards(config: &PoolConfig) -> Result<(), Box<dyn std::error::Error>> {
    let response_data = reqwest::get(format!("{}/poolrewards.json", config.server_sync_url))
        .await?
        .text()
        .await?;
    std::fs::write("./config/poolRewards.json", response_data)?;
    info!("Successfully updated pool rewards");
    
    let mut bans = Bans::load_bans().expect("failed to load config bans.json");

    let response_data = reqwest::get(format!("{}/bans.json", config.server_sync_url))
        .await?
        .json::<Vec<String>>()
        .await?;
    bans.banned_usernames = response_data;

    bans.save_bans().expect("failed to save config bans.json");
    info!("Synced bans.");

    Ok(())
}

//pub async fn sync(
//     config: &PoolConfig,
//     sys: &mut Arc<Mutex<System>>,

pub async fn sync(
    sys: &mut Arc<Mutex<System>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting sync..");
    let config = CONFIG.get().unwrap();
    if !std::fs::exists(&config.base_sync_folder)? {
        std::fs::create_dir_all(&config.base_sync_folder)?;
    }

    let mut sys = sys.lock().await;
    let ip = get_pool_ip(&config).await.expect("failed to get pool ip");
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

    let mem_usage = (sys.used_memory() as f32 / sys.total_memory() as f32 * 1e4).round() / 100.0; // rounding to two decimals
    mining::GLOBAL_SHARES_INCREASE.store(0, Ordering::Relaxed);

    let worker_path = format!(
        "{}/workers_{}.json",
        config.base_sync_folder, config.pool_name
    );

    let stats = mining::MINER_STATS.clone();
    mining::MINER_STATS.clear();
    mining::MINER_STATS.shrink_to_fit();
    stats.iter_mut().for_each(|mut s| {
        s.value_mut().pool_name = config.pool_name.clone();
    }); // inject the pool name here, so we don't stupidly waste 24 + pool_name.len() bytes
    let worker_content = serde_json::to_string(&stats)?;

    std::fs::write(&worker_path, &worker_content)?;
    drop(stats);

    let reward_path = format!(
        "{}/rewards_{}.json",
        config.base_sync_folder, config.pool_name
    );

    let balances = mining::BALANCES_TO_UPDATE.clone();
    let rewards_content = serde_json::to_string(&balances)?;

    std::fs::write(&reward_path, &rewards_content)?;

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

    // Customize all the details of a Part if needed...

    debug!("Rewards: {rewards_content}");
    let rewards = multipart::Part::text(rewards_content)
        .file_name(format!("rewards_{}.json", config.pool_name));

    debug!("Workers: {worker_content}");
    let workers = multipart::Part::text(worker_content)
        .file_name(format!("workers_{}.json", config.pool_name));

    // Add the custom part to our form...
    let form = form.part("rewards", rewards).part("workers", workers);

    SYNC_COUNT.fetch_add(1, Ordering::Relaxed);
    debug!("Syncing to {request_url}");
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
                mining::BALANCES_TO_UPDATE.clear();
                {
                    let mut block_lock = mining::GLOBAL_BLOCKS.lock().await;
                    block_lock.clear();
                    block_lock.shrink_to_fit();
                }
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

    mining::USR_WORKERS.shrink_to_fit();
    mining::WORKERS.shrink_to_fit();
    Ok(())
}