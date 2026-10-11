use crate::cfg::config::PoolConfig;
use crate::mining::{mining, stats};
use crate::{data, kolka, BANS, CONFIG, CONNECTIONS, POOL_REWARDS};
use compact_str::CompactString;
use dashmap::DashMap;
use data::data::LoginInfos;
#[cfg(not(feature = "dummy_sync"))]
use log::warn;
use log::{debug, error, info};
use reqwest::multipart;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::process::exit;
#[cfg(any(all(not(feature = "dummy_sync"), not(debug_assertions)), test))]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

static SYNC_COUNT: AtomicUsize = AtomicUsize::new(0);

// shrinking write-locks every shard of the maps, so only do it every n syncs (~5 min by default)
const SHRINK_EVERY_N_SYNCS: usize = 20;

// one client for all requests, so the TLS setup is only built once. Idle pooling is disabled
// so every request still opens its own connection, like the per-request clients did before
static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .expect("failed to build http client")
});

// writes the file on the blocking pool and hands the buffer back, so it doesn't have to be copied
async fn write_file(path: String, content: Vec<u8>) -> std::io::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || std::fs::write(&path, &content).map(|_| content))
        .await
        .map_err(std::io::Error::other)?
}

// Keep the uploaded values alongside the keys already needed for acknowledgement.
struct RewardSnapshot(Vec<(CompactString, f32)>);

impl RewardSnapshot {
    fn capture(balances: &DashMap<CompactString, f32>) -> Self {
        Self(
            balances
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect(),
        )
    }

    // never more than the serialized size: `"<name>":0.0` per entry, plus commas and braces
    fn min_json_len(&self) -> usize {
        2 + self.0.iter().map(|(key, _)| key.len() + 6).sum::<usize>()
    }
}

impl Serialize for RewardSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

#[cfg(any(all(not(feature = "dummy_sync"), not(debug_assertions)), test))]
fn acknowledge_uploaded_work(
    rewards: &RewardSnapshot,
    balances: &DashMap<CompactString, f32>,
    blocks: &mut Vec<stats::BlockInfos>,
    big_blocks: usize,
    pending_shares: &AtomicU64,
    block_increase: u64,
) {
    for (key, uploaded) in &rewards.0 {
        balances.remove_if_mut(key, |_, pending| {
            *pending -= *uploaded;
            *pending == 0.0
        });
    }
    blocks.drain(..big_blocks);
    pending_shares.fetch_sub(block_increase, Relaxed);
}

//#[serde(skip_serializing_if = "Option::is_none")]
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
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

#[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
pub async fn get_pool_ip(config: &&PoolConfig) -> Result<String, Box<dyn std::error::Error>> {
    debug!("Getting ip..");
    let client = &*HTTP_CLIENT;
    let request = client.get("https://api.ipify.org/");
    let response = request
        .timeout(Duration::from_secs(config.timeout as u64))
        .send()
        .await?;

    let text = response.text().await?;
    Ok(text)
}
#[cfg(not(debug_assertions))]
pub async fn login(config: &PoolConfig) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Gathering login data..");
    let ip = if config.use_ngrok {
        // ip = "".to_string();
        return Err(Box::from("Ngrok not implemented"));
    } else {
        get_pool_ip(&config).await?
    };
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
#[cfg(not(debug_assertions))]
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

    mining::DUE_DIFF_CACHE.store(kolka::kolka::get_diff("DUE"), Relaxed);
    mining::ESP8266NH_DIFF_CACHE.store(kolka::kolka::get_diff("ESP8266NH"), Relaxed);

    Ok(())
}

async fn update_pool_rewards(config: &PoolConfig) -> Result<String, Box<dyn std::error::Error>> {
    let response_data = HTTP_CLIENT
        .get(format!("{}/poolrewards.json", config.server_sync_url))
        .send()
        .await?
        .text()
        .await?;
    std::fs::write("./config/poolRewards.json", &response_data)?;
    info!("Successfully updated pool rewards");

    // let mut bans = Bans::load_bans().expect("failed to load config bans.json");

    let response_data_bans = HTTP_CLIENT
        .get(format!("{}/bans.json", config.server_sync_url))
        .send()
        .await?
        .json::<HashSet<CompactString>>()
        .await?;
    // bans.banned_usernames = response_data_bans;

    // bans.save_bans().expect("failed to save config bans.json");
    let mut ban_lock = BANS.lock().await;
    ban_lock.banned_usernames = response_data_bans;

    ban_lock.save_bans()?;
    info!("Synced bans.");

    Ok(response_data)
}

#[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
pub async fn sync(
    sys: &Arc<Mutex<System>>,
    last_ip: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    info!("Starting sync..");
    let config = CONFIG.get().unwrap();
    if !std::fs::exists(&config.base_sync_folder)? {
        std::fs::create_dir_all(&config.base_sync_folder)?;
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

    let worker_path = format!(
        "{}/workers_{}.json",
        config.base_sync_folder, config.pool_name
    );

    // we inject the pool_name AND the algorithm, because they're the same for every miner
    let workers_ctx = stats::StatsWithContext {
        stats: &mining::MINER_STATS,
        pool_name: &config.pool_name,
    };
    // pre-sized with a lower bound: fewer reallocations, never more capacity than needed
    let mut worker_content = Vec::with_capacity(workers_ctx.min_json_len());
    serde_json::to_writer(&mut worker_content, &workers_ctx)?;
    // drop(stats);

    let worker_content = write_file(worker_path.clone(), worker_content).await?;

    let reward_path = format!(
        "{}/rewards_{}.json",
        config.base_sync_folder, config.pool_name
    );

    let request_url = format!(
        "https://{}/pool_sync/?host={}&port={}&version={}&identifier={}&name={}\
    &blockIncrease={}&bigBlocks={}&cpu={}&ram={}&connections={}",
        // "server.duinocoin.com", //TODO: temp fix
        config.server_ip,
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


    let rewards_snapshot = RewardSnapshot::capture(&mining::BALANCES_TO_UPDATE);
    let mut rewards_content = Vec::with_capacity(rewards_snapshot.min_json_len());
    serde_json::to_writer(&mut rewards_content, &rewards_snapshot)?;

    let rewards_content = write_file(reward_path.clone(), rewards_content).await?;


    let sync_count = SYNC_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    let shrink_maps = sync_count.is_multiple_of(SHRINK_EVERY_N_SYNCS);
    debug!("Syncing to {request_url}");
    #[cfg(not(feature = "dummy_sync"))]
    {
        debug!("Workers: {}", String::from_utf8_lossy(&worker_content));
        let workers = multipart::Part::bytes(worker_content)
            .file_name(format!("workers_{}.json", config.pool_name));

        debug!("Rewards: {}", String::from_utf8_lossy(&rewards_content));
        let rewards = multipart::Part::bytes(rewards_content)
            .file_name(format!("rewards_{}.json", config.pool_name));

        let form = reqwest::multipart::Form::new()
            .part("rewards", rewards)
            .part("workers", workers);
        let client = &*HTTP_CLIENT;
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
                    {
                        let mut block_lock = mining::GLOBAL_BLOCKS.lock().await;
                        acknowledge_uploaded_work(
                            &rewards_snapshot,
                            &mining::BALANCES_TO_UPDATE,
                            &mut block_lock,
                            big_blocks,
                            &mining::GLOBAL_SHARES_INCREASE,
                            block_increase,
                        );
                        block_lock.shrink_to_fit();
                    }
                    if shrink_maps {
                        mining::BALANCES_TO_UPDATE.shrink_to_fit();
                    }
                    info!("Successfully synced {}", SYNC_COUNT.load(Ordering::Relaxed));
                    tokio::fs::remove_file(&reward_path).await?; // so we don't accidentally sync the same thing twice
                    tokio::fs::remove_file(&worker_path).await?;
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
    if shrink_maps {
        mining::USR_WORKERS_ALLOC.shrink_to_fit();
        mining::MINER_STATS.shrink_to_fit();
    }

    Ok(ip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn block(id: u64) -> stats::BlockInfos {
        stats::BlockInfos::new(
            id,
            format!("miner{id}").into(),
            id as f32,
            "DUCO-S1".into(),
            format!("hash{id}"),
        )
    }

    #[test]
    fn snapshot_serializes_captured_values_even_after_live_updates() {
        let balances = DashMap::new();
        let username = CompactString::from("alice\"\\username-long-enough-to-use-the-heap");
        balances.insert(username.clone(), 1.25);
        let snapshot = RewardSnapshot::capture(&balances);

        *balances.get_mut(&username).unwrap() += 0.5;
        balances.insert(CompactString::from("bob"), 2.0);

        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert!(snapshot.min_json_len() <= encoded.len());
        let decoded: HashMap<String, f32> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[username.as_str()], 1.25);
        assert_eq!(
            serde_json::to_string(&RewardSnapshot::capture(&DashMap::new())).unwrap(),
            "{}"
        );
    }

    #[test]
    fn acknowledgement_preserves_new_work_and_block_order() {
        let balances = DashMap::new();
        balances.insert(CompactString::from("alice"), 1.25);
        balances.insert(CompactString::from("fully-sent"), 2.0);
        balances.insert(CompactString::from("zero"), 0.0);
        balances.insert(CompactString::from("tiny"), 0.0);
        let snapshot = RewardSnapshot::capture(&balances);
        let mut blocks = vec![block(1), block(2)];
        let pending_shares = AtomicU64::new(2);
        let uploaded_blocks = blocks.len();
        let uploaded_shares = pending_shares.load(Relaxed);

        *balances.get_mut("alice").unwrap() += 0.5;
        *balances.get_mut("alice").unwrap() += 0.125;
        *balances.get_mut("tiny").unwrap() += f32::EPSILON / 2.0;
        balances.insert(CompactString::from("bob"), 3.0);
        blocks.extend([block(3), block(4)]);
        pending_shares.fetch_add(3, Relaxed);
        let expected_blocks = serde_json::to_value(&blocks[uploaded_blocks..]).unwrap();

        acknowledge_uploaded_work(
            &snapshot,
            &balances,
            &mut blocks,
            uploaded_blocks,
            &pending_shares,
            uploaded_shares,
        );

        assert_eq!(
            serde_json::to_value(&balances).unwrap(),
            json!({"alice": 0.625, "bob": 3.0, "tiny": f32::EPSILON / 2.0})
        );
        assert_eq!(serde_json::to_value(&blocks).unwrap(), expected_blocks);
        assert_eq!(pending_shares.load(Relaxed), 3);

        // The retained work is acknowledged exactly once by the next successful sync.
        let next = RewardSnapshot::capture(&balances);
        let remaining_blocks = blocks.len();
        acknowledge_uploaded_work(
            &next,
            &balances,
            &mut blocks,
            remaining_blocks,
            &pending_shares,
            3,
        );
        assert!(balances.is_empty());
        assert!(blocks.is_empty());
        assert_eq!(pending_shares.load(Relaxed), 0);
    }
}
