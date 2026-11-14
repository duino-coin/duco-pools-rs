use crate::cfg::config::PoolConfig;
use crate::mining::{mining, stats};
use crate::{BANS, CONFIG, CONNECTIONS, CURVE_VALUES};
use anyhow::bail;
use compact_str::CompactString;
use dashmap::DashMap;
#[cfg(not(feature = "dummy_sync"))]
use log::warn;
use log::{debug, error, info};
use reqwest::multipart;
use serde::{Deserialize, Serialize, Serializer};
#[cfg(all(not(feature = "dummy_sync"), not(debug_assertions)))]
use std::collections::HashMap;
use std::collections::HashSet;
#[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
use std::path::Path;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

static SYNC_COUNT: AtomicUsize = AtomicUsize::new(0);
// unix secs of the last accepted sync, published on /data; 0 until the first one
pub static LAST_SYNC_TIME: AtomicU64 = AtomicU64::new(0);

const CURVE_PATH: &str = "./config/curve.json";

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

// writes the file on the blocking pool and hands the buffer back, so it doesn't have to be copied;
// staged next to it and renamed over it, so a crash never leaves a cut-off file behind
pub(crate) async fn write_file(path: PathBuf, content: Vec<u8>) -> std::io::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let mut staging = path.clone().into_os_string();
        staging.push(".tmp");
        std::fs::write(&staging, &content)?;
        std::fs::rename(&staging, &path).map(|_| content)
    })
    .await
    .map_err(std::io::Error::other)?
}

// a final sync that failed leaves its rewards file behind, they go out with the next sync
#[cfg(all(not(feature = "dummy_sync"), not(debug_assertions)))]
pub fn load_pending_rewards(config: &PoolConfig) -> Result<(), anyhow::Error> {
    // joined, so an empty base_sync_folder is the working directory and not the filesystem root
    let path =
        Path::new(&config.base_sync_folder).join(format!("rewards_{}.json", config.pool_name));
    if !std::fs::exists(&path)? {
        return Ok(());
    }
    let rewards: HashMap<CompactString, f32> = serde_json::from_slice(&std::fs::read(&path)?)?;
    info!(
        "Loaded pending rewards of {} users from {}",
        rewards.len(),
        path.display()
    );
    for (username, reward) in rewards {
        *mining::BALANCES_TO_UPDATE.entry(username).or_insert(0f32) += reward;
    }
    Ok(())
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
        serializer.collect_map(self.0.iter().map(|(key, value)| (key, value)))
    }
}

#[cfg(any(all(not(feature = "dummy_sync"), not(debug_assertions)), test))]
fn acknowledge_uploaded_work(
    rewards: &RewardSnapshot,
    balances: &DashMap<CompactString, f32>,
    pending_shares: &AtomicU64,
    block_increase: u64,
) {
    for (key, uploaded) in &rewards.0 {
        balances.remove_if_mut(key, |_, pending| {
            *pending -= *uploaded;
            *pending == 0.0
        });
    }
    pending_shares.fetch_sub(block_increase, Relaxed);
}

// query of the sync request, serialized so every value is url-encoded
#[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SyncQuery<'a> {
    host: &'a str,
    port: u32,
    version: &'a str,
    identifier: &'a str,
    name: &'a str,
    block_increase: u64,
    cpu: f32,
    ram: f32,
    connections: usize,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct Curve {
    pub pc_base: f64,               // Base pay for PCs
    pub mcu_base: f64,              // Base pay for microcontrollers
    pub mcu_pc_step_h: f64, // Hashrate where pay steps from the microcontroller level to the PC level
    pub mcu_pc_step_sharpness: f64, // How abrupt that step is
    pub slow_boost_h: f64,  // Extra pay for slow devices

    pub arduino_h: f64, // Arduino hashrate: the centre of the bonus and the low-end cutoff point
    pub cutoff_sharpness: f64, // How fast pay falls below the Arduino hashrate
    pub cutoff_slope: f64, // How sharp the cutoff’s corner is
    pub arduino_bonus: f64, // Height of the Arduino bonus
    pub arduino_bonus_width: f64, // Width of that bonus

    pub pc_gpu_crossover_h: f64, // Hashrate where the PC branch hands over to the GPU branch
    pub pc_gpu_crossover_sharpness: f64, // How abrupt that handover is

    pub gpu_base: f64,            // Starting pay of the GPU branch
    pub gpu_growth: f64,          // Pay added per doubling of hashrate on the GPU branch
    pub gpu_growth_start: f64,    // Where that growth begins, as a multiple of pc_gpu_crossover_h
    pub gpu_growth_softness: f64, // How gradually that growth starts

    pub h_ref: f64,     // Hashrate when the switch from drop_low to drop_high happens
    pub drop_low: f64,  // Kolka efficiency drop for devices slower than h_ref
    pub drop_high: f64, // Kolka efficiency drop for devices faster than h_ref

    pub expected_sharetime: u8, // the sharetime in seconds want to normalize for (u8 allows up to 255)
    pub block_probability: u32,
    pub block_reward: f32,
}
impl Default for Curve {
    fn default() -> Self {
        Curve {
            pc_base: 0.0,
            mcu_base: 0.0,
            mcu_pc_step_h: 0.0,
            mcu_pc_step_sharpness: 0.0,
            slow_boost_h: 0.0,
            arduino_h: 0.0,
            cutoff_sharpness: 0.0,
            cutoff_slope: 0.0,
            arduino_bonus: 0.0,
            arduino_bonus_width: 0.0,
            pc_gpu_crossover_h: 0.0,
            pc_gpu_crossover_sharpness: 0.0,
            gpu_base: 0.0,
            gpu_growth: 0.0,
            gpu_growth_start: 0.0,
            gpu_growth_softness: 0.0,
            h_ref: 0.0,
            drop_low: 0.0,
            drop_high: 0.0,
            expected_sharetime: 100,
            block_probability: 28110000,
            block_reward: 28.11,
        }
    }
}

impl Curve {
    // serde_json only yields finite numbers, so this checks what the reward and
    // difficulty calculations divide by, and what would panic or flood the pool
    fn parse(content: &[u8]) -> Result<Self, anyhow::Error> {
        let curve: Curve = serde_json::from_slice(content)?;
        let divisors = [
            ("mcu_pc_step_h", curve.mcu_pc_step_h),
            ("arduino_h", curve.arduino_h),
            ("cutoff_sharpness", curve.cutoff_sharpness),
            ("arduino_bonus_width", curve.arduino_bonus_width),
            ("pc_gpu_crossover_h", curve.pc_gpu_crossover_h),
            ("gpu_growth_start", curve.gpu_growth_start),
            ("gpu_growth_softness", curve.gpu_growth_softness),
        ];
        if let Some((name, _)) = divisors.iter().find(|(_, value)| *value <= 0.0) {
            bail!("curve: {name} must be positive");
        }
        // block_probability is the exclusive bound of a random range
        if curve.expected_sharetime == 0 || curve.block_probability == 0 {
            bail!("curve: expected_sharetime and block_probability must be positive");
        }
        Ok(curve)
    }
}
// an ip address is at most 45 bytes, anything longer is an error page
#[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
const MAX_IP_RESPONSE_BYTES: usize = 64;

#[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
pub async fn get_pool_ip(config: &PoolConfig) -> Result<String, anyhow::Error> {
    debug!("Getting ip..");
    let client = &*HTTP_CLIENT;
    let request = client.get("https://api.ipify.org/");
    let mut response = request
        .timeout(Duration::from_secs(config.timeout as u64))
        .send()
        .await?
        .error_for_status()?;

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > MAX_IP_RESPONSE_BYTES - body.len() {
            bail!("ip lookup reply is too long");
        }
        body.extend_from_slice(&chunk);
    }
    // the reply is sent to the master as the pool's host, so it has to be an address
    match std::str::from_utf8(&body).map(|text| text.trim().parse::<std::net::IpAddr>()) {
        Ok(Ok(ip)) => Ok(ip.to_string()),
        _ => bail!("ip lookup reply is not an ip address"),
    }
}
// payload of the PoolLogin message
#[cfg(not(debug_assertions))]
#[derive(Serialize)]
struct LoginInfos<'a> {
    host: &'a str,
    port: u32,
    version: &'a str,
    identifier: &'a str,
    name: &'a str,
}

#[cfg(not(debug_assertions))]
pub async fn login(config: &PoolConfig) -> Result<(), anyhow::Error> {
    debug!("Gathering login data..");
    let ip = if config.use_ngrok {
        // ip = "".to_string();
        bail!("Ngrok not implemented");
    } else {
        get_pool_ip(config).await?
    };
    let infos = LoginInfos {
        host: &ip,
        port: config.port,
        version: &config.pool_version,
        identifier: &config.pool_id,
        name: &config.pool_name,
    };
    info!("Logging in..");
    let timeout = Duration::from_secs(config.timeout as u64);
    let address = format!("{}:{}", config.server_ip, config.server_port);
    let mut socket = tokio::time::timeout(timeout, TcpStream::connect(address)).await??;
    debug!("Connected to server");
    let data = read_master_reply(&mut socket, config).await?;
    if data.starts_with(&config.server_version) {
        let info_str = serde_json::to_string(&infos)?;
        debug!("Sending {info_str}");
        socket
            .write_all(format!("PoolLogin,{info_str}").as_bytes())
            .await?;
        socket.flush().await?;
    } else {
        error!("Server version mismatch, server returned {data} in login");
        exit(-1);
    }
    debug!("Waiting for response..");
    let data = read_master_reply(&mut socket, config).await?;
    if data == "LoginOK" {
        info!("Successfully logged in");
    } else {
        error!("Unknown error, server returned {data} in login");
        exit(-1);
    }
    Ok(())
}
#[cfg(not(debug_assertions))]
pub async fn logout(config: &PoolConfig) -> Result<(), anyhow::Error> {
    // bounded like login, this runs after ctrl-c, so a silent master must not hang the shutdown
    let timeout = Duration::from_secs(config.timeout as u64);
    let address = format!("{}:{}", config.server_ip, config.server_port);
    let mut socket = tokio::time::timeout(timeout, TcpStream::connect(address)).await??;
    let data = read_master_reply(&mut socket, config).await?;
    // the greeting comes first, the logout reply only after our request
    if data.starts_with(&config.server_version) {
        socket
            .write_all(format!("PoolLogout,{}", config.pool_id).as_bytes())
            .await?;
        socket.flush().await?;
        let data = read_master_reply(&mut socket, config).await?;
        if data == "LogoutOK" {
            info!("Successfully logged out");
            return Ok(());
        }
        error!("Unknown error, server returned {data} in logout");
    } else {
        error!("Unknown error, server greeted with {data} in logout");
    }
    bail!("Unknown error")
}

// the master server answers each step with one unterminated message
#[cfg(not(debug_assertions))]
const MASTER_REPLY_MAX_BYTES: usize = 8 * 1024;

#[cfg(not(debug_assertions))]
async fn read_master_reply(
    socket: &mut TcpStream,
    config: &PoolConfig,
) -> Result<String, anyhow::Error> {
    // one read, at most as much as the BufReader it replaces could take in
    let mut received = vec![0u8; MASTER_REPLY_MAX_BYTES];
    let timeout = Duration::from_secs(config.timeout as u64);
    let len = tokio::time::timeout(timeout, socket.read(&mut received)).await??;
    received.truncate(len);

    let data = String::from_utf8(received).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Couldn't parse received string as utf8",
        )
    })?;
    debug!("Received: {}", data);
    Ok(data)
}

// the last curve resync_curve fetched, so the pool can start while the master is unreachable
pub fn load_saved_curve() -> Result<(), anyhow::Error> {
    let pool_rewards = Curve::parse(&std::fs::read(CURVE_PATH)?)?;
    CURVE_VALUES.store(Arc::new(pool_rewards));
    Ok(())
}

pub async fn resync_curve(config: &PoolConfig) -> Result<(), anyhow::Error> {
    // cloudflare ignores no-cache from clients, a unique query string misses the edge cache
    let cache_bust = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let curve_content = fetch_curve(config, cache_bust).await?;
    // keeps the previous curve if the new one is unusable
    let pool_rewards = Curve::parse(curve_content.as_bytes())?;

    CURVE_VALUES.store(Arc::new(pool_rewards));
    // only a validated curve replaces the one on disk;
    // the curve is in use already, a failed write only keeps the previous file
    if let Err(err) = write_file(CURVE_PATH.into(), curve_content.into_bytes()).await {
        error!("Failed to save curve.json: {err}");
    }
    info!("Successfully updated curve settings");

    // the curve is in use already, a failed bans sync only keeps the previous bans
    if let Err(err) = sync_bans(config, cache_bust).await {
        error!("Failed to sync bans: {err}");
    }
    Ok(())
}

async fn fetch_curve(config: &PoolConfig, cache_bust: u64) -> Result<String, anyhow::Error> {
    Ok(HTTP_CLIENT
        .get(format!(
            "{}/curve.json?t={cache_bust}",
            config.server_sync_url
        ))
        .header("Cache-Control", "no-cache")
        // bounded like the other requests, shutdown waits for a running sync
        .timeout(Duration::from_secs(config.timeout as u64))
        .send()
        .await?
        .text()
        .await?)
}

async fn sync_bans(config: &PoolConfig, cache_bust: u64) -> Result<(), anyhow::Error> {
    let response_data_bans = HTTP_CLIENT
        .get(format!(
            "{}/bans.json?t={cache_bust}",
            config.server_sync_url
        ))
        .timeout(Duration::from_secs(config.timeout as u64))
        .send()
        .await?
        .json::<HashSet<CompactString>>()
        .await?;

    let mut ban_lock = BANS.lock().await;
    ban_lock.banned_usernames = response_data_bans;

    ban_lock.save_bans().await?;
    info!("Synced bans.");

    Ok(())
}

#[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
pub async fn sync(sys: &mut System, last_ip: &str) -> Result<String, anyhow::Error> {
    info!("Starting sync..");
    let config = CONFIG.get().unwrap();
    // succeeds if the folder already exists
    std::fs::create_dir_all(&config.base_sync_folder)?;

    let ip = match get_pool_ip(config).await {
        Ok(ok) => ok,
        Err(err) => {
            if last_ip.is_empty() {
                bail!("Failed to get pool ip {err}");
            }
            error!("Failed to get pool ip: {err}. Falling back to last ip collected.");
            last_ip.to_string()
        }
    };
    let block_increase = mining::GLOBAL_SHARES_INCREASE.load(Ordering::Relaxed);

    let connections = CONNECTIONS.load(Ordering::Relaxed);
    // TODO: impl ngrok

    //    if (use_ngrok && sync_count > 0 && sync_count % 50 == 0)
    //         await get_ngrok_ip();

    // only refresh certain parts of the system info
    sys.refresh_specifics(
        RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
            .with_memory(MemoryRefreshKind::nothing().with_ram()),
    );
    let cpu_usage = sys.global_cpu_usage();

    let mem_usage =
        (sys.free_like_used_memory() as f32 / sys.total_memory() as f32 * 1e4).round() / 100.0; // rounding to two decimals

    // joined, so an empty base_sync_folder is the working directory and not the filesystem root
    let worker_path =
        Path::new(&config.base_sync_folder).join(format!("workers_{}.json", config.pool_name));

    // we inject the pool_name AND the algorithm, because they're the same for every miner
    let workers_ctx = stats::StatsWithContext {
        stats: &mining::MINER_STATS,
        pool_name: &config.pool_name,
    };
    // pre-sized from the previous sync plus some headroom, usually no reallocations
    static LAST_WORKERS_JSON_LEN: AtomicUsize = AtomicUsize::new(0);
    let last_len = LAST_WORKERS_JSON_LEN.load(Relaxed);
    let mut worker_content = Vec::with_capacity(last_len + last_len / 8);
    serde_json::to_writer(&mut worker_content, &workers_ctx)?;
    LAST_WORKERS_JSON_LEN.store(worker_content.len(), Relaxed);

    let worker_content = write_file(worker_path.clone(), worker_content).await?;

    let reward_path =
        Path::new(&config.base_sync_folder).join(format!("rewards_{}.json", config.pool_name));

    let request_url = format!("https://{}/pool_sync/", config.server_endpoint);
    let query = SyncQuery {
        host: &ip,
        port: config.port,
        version: &config.pool_version,
        identifier: &config.pool_id,
        name: &config.pool_name,
        block_increase,
        cpu: cpu_usage,
        ram: mem_usage,
        connections,
    };

    let rewards_snapshot = RewardSnapshot::capture(&mining::BALANCES_TO_UPDATE);
    let mut rewards_content = Vec::with_capacity(rewards_snapshot.min_json_len());
    serde_json::to_writer(&mut rewards_content, &rewards_snapshot)?;

    let rewards_content = write_file(reward_path.clone(), rewards_content).await?;

    let sync_count = SYNC_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    let shrink_maps = sync_count.is_multiple_of(SHRINK_EVERY_N_SYNCS);
    debug!("Syncing to {request_url} with {query:?}");
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
            .query(&query)
            .multipart(form)
            .timeout(Duration::from_secs(config.timeout as u64))
            .send()
            .await
        {
            Ok(res) => {
                let status = res.status();
                let final_url = res.url().clone();
                // the server answers 200 even when it rejects the sync, the verdict is in the body
                let body = res.text().await?;
                let accepted = serde_json::from_str::<serde_json::Value>(&body)
                    .is_ok_and(|reply| reply["success"] == true);
                if accepted {
                    acknowledge_uploaded_work(
                        &rewards_snapshot,
                        &mining::BALANCES_TO_UPDATE,
                        &mining::GLOBAL_SHARES_INCREASE,
                        block_increase,
                    );
                    if shrink_maps {
                        mining::BALANCES_TO_UPDATE.shrink_to_fit();
                    }
                    info!("Successfully synced {sync_count}");
                    tokio::fs::remove_file(&reward_path).await?; // so we don't accidentally sync the same thing twice
                    tokio::fs::remove_file(&worker_path).await?;
                    LAST_SYNC_TIME.store(UNIX_EPOCH.elapsed()?.as_secs(), Relaxed);
                } else {
                    warn!(
                        "Failed to sync {} ({status} from {final_url}): {}",
                        sync_count, body
                    );
                }
            }
            Err(err) => {
                error!("Error while syncing: {}", err);
                return Err(err.into());
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
    use std::collections::HashMap;

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
    fn acknowledgement_preserves_new_work() {
        let balances = DashMap::new();
        balances.insert(CompactString::from("alice"), 1.25);
        balances.insert(CompactString::from("fully-sent"), 2.0);
        balances.insert(CompactString::from("zero"), 0.0);
        balances.insert(CompactString::from("tiny"), 0.0);
        let snapshot = RewardSnapshot::capture(&balances);
        let pending_shares = AtomicU64::new(2);
        let uploaded_shares = pending_shares.load(Relaxed);

        *balances.get_mut("alice").unwrap() += 0.5;
        *balances.get_mut("alice").unwrap() += 0.125;
        *balances.get_mut("tiny").unwrap() += f32::EPSILON / 2.0;
        balances.insert(CompactString::from("bob"), 3.0);
        pending_shares.fetch_add(3, Relaxed);

        acknowledge_uploaded_work(&snapshot, &balances, &pending_shares, uploaded_shares);

        assert_eq!(
            serde_json::to_value(&balances).unwrap(),
            json!({"alice": 0.625, "bob": 3.0, "tiny": f32::EPSILON / 2.0})
        );
        assert_eq!(pending_shares.load(Relaxed), 3);

        // The retained work is acknowledged exactly once by the next successful sync.
        let next = RewardSnapshot::capture(&balances);
        acknowledge_uploaded_work(&next, &balances, &pending_shares, 3);
        assert!(balances.is_empty());
        assert_eq!(pending_shares.load(Relaxed), 0);
    }
}
