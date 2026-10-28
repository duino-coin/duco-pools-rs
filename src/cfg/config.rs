use anyhow::bail;
use config::Config;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Deserialize, Serialize)]
pub struct PoolConfig {
    #[serde(rename = "poolName")]
    pub pool_name: String,
    // pub pool_test: &'static str,
    #[serde(rename = "poolPassword")]
    pub pool_password: String,
    #[serde(rename = "poolID")]
    pub pool_id: String,
    #[serde(skip, default = "pool_version")]
    pub pool_version: String,
    pub host: String,
    pub port: u32,
    #[serde(rename = "serverIP")]
    pub server_ip: String,
    #[serde(rename = "serverPort")]
    pub server_port: u32,
    #[serde(skip, default = "server_version")]
    pub server_version: String,
    // #[serde(rename = "preGenJobCount")]
    // pub pre_gen_job_count: u32,
    // #[serde(rename = "jobGenerationDelay")]
    // pub job_generation_delay: u32,
    #[serde(rename = "maxWorkers")]
    pub max_workers: u64,
    pub motd: String,
    #[serde(rename = "initialBlockHash")]
    pub initial_blockhash: String,
    // #[serde(rename = "expectedSharetime")]
    // pub expected_sharetime: u16, // don't think this should exceed 65k
    #[serde(rename = "updateMinersStatsEvery")]
    pub update_miners_stats_every: u16,
    // #[serde(rename = "blockProbability")]
    // pub block_probability: u64,
    // #[serde(rename = "blockReward")]
    // pub block_reward: f32,
    // #[serde(rename  = "useNgrok")]
    pub use_ngrok: bool,
    #[serde(rename = "autoRestart")]
    pub auto_restart: u32,
    #[serde(rename = "guessPort")]
    pub guess_port: bool,
    pub dashboard_port: u32,
    #[serde(rename = "syncTime")]
    pub sync_time: u32,
    pub timeout: u16, // probably shouldn't exceed 65k either
    pub server_sync_url: String,
    #[serde(default = "default_server_endpoint")]
    pub server_endpoint: String,
    pub base_sync_folder: String,
    #[serde(skip, default = "default_shares")]
    pub max_shares_per_minute: u16, // probably shouldn't exceed 65k either
    pub restart_script: String,
    pub firewall_cmd: String,
    #[serde(default = "generate_key")]
    pub dashboard_key: String,
    pub update_restart_script: String,
    pub auto_update: bool,
    pub update_check_time_seconds: u64,
}
fn default_shares() -> u16 {
    80u16
}

fn pool_version() -> String {
    "0.3".to_string()
}
fn server_version() -> String {
    "3.0".to_string()
}

fn generate_key() -> String {
    // alphanumeric only, so the key can be used in the dashboard's query string as is
    // let mut rng = rand::rng();
    // let str: String = (0..32)
    //     .map(|_| char::from(Alphanumeric.sample(&mut rng)))
    //     .collect();
    // str
    // keep the dash disabled with no key provided
    String::new()
}

fn default_server_endpoint() -> String {
    "server.duinocoin.com".into()
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            pool_name: "".to_string(),
            pool_password: "".to_string(),
            pool_id: "".to_string(),
            pool_version: pool_version(),
            host: "0.0.0.0".to_string(),
            port: 6000,
            server_ip: "server.duinocoin.com".to_string(),
            server_port: 32811,
            server_version: server_version(),
            max_workers: 125,
            motd: "".to_string(),
            initial_blockhash: "05b7406b3909071b2d85b2d2958ad07a0d52bd12".to_string(),
            update_miners_stats_every: 1,
            use_ngrok: false,
            auto_restart: 0,
            guess_port: false,
            dashboard_port: 6030,
            sync_time: 15,
            timeout: 30,
            server_sync_url: "https://server.duinocoin.com/poolsyncdata".to_string(),
            server_endpoint: "server.duinocoin.com".to_string(),
            base_sync_folder: "".to_string(),
            max_shares_per_minute: 80,
            restart_script: "".to_string(),
            firewall_cmd: "".to_string(),
            dashboard_key: generate_key(),
            update_restart_script: "".to_string(),
            auto_update: false,
            update_check_time_seconds: 86400,
        }
    }
}

pub fn load_config() -> Result<PoolConfig, anyhow::Error> {
    let path = Path::new("./config/config.json");

    if path.exists() {
        let settings = Config::builder()
            .add_source(config::File::with_name(path.to_str().unwrap()))
            .build()?;

        let config = settings.try_deserialize::<PoolConfig>()?;
        validate_config(&config)?;
        Ok(config)
    } else {
        std::fs::create_dir_all(path.parent().unwrap())?;
        let config = PoolConfig::default();
        save_config(&config);
        validate_config(&config)?;
        Ok(config)
    }
}

pub fn save_config(config: &PoolConfig) {
    let string = serde_json::to_string_pretty(config).expect("Failed to convert config to str");
    let _ = std::fs::write("./config/config.json", string);
}

fn validate_config(config: &PoolConfig) -> anyhow::Result<()> {
    if config.auto_update && config.update_check_time_seconds == 0 {
        bail!("update_check_time_seconds must be greater than 0");
    }
    if config.update_miners_stats_every == 0 {
        bail!("update_miners_stats_every must be greater than 0");
    }
    // 0 would sync back to back against the master server
    if config.sync_time == 0 {
        bail!("syncTime must be greater than 0");
    }
    // 0 would fail every request to the master server
    if config.timeout == 0 {
        bail!("timeout must be greater than 0");
    }
    // the job hash chain starts from it, and the mining code keeps it as the 40 hex chars of a sha1
    if config.initial_blockhash.len() != 40
        || !config
            .initial_blockhash
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        bail!("initial_blockhash must be 40 hex characters");
    }

    Ok(())
}
