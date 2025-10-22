use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MinerStat
// TODo: update types
{
    #[serde(rename = "u")]
    pub username: String, // this is stupid..
    #[serde(rename = "h")]
    pub hashrate: f32,
    #[serde(rename = "s")]
    pub share_time: f32,
    #[serde(rename = "a")]
    pub accepted_shares: u64,
    #[serde(rename = "r")]
    pub rejected_shares: u64,
    #[serde(rename = "c")]
    pub kolka_drop: f32,
    #[serde(rename = "al")]
    pub algorithm: String,
    #[serde(rename = "d")]
    pub difficulty: u32,
    #[serde(rename = "p")]
    pub pool_name: String,
    #[serde(rename = "sft")]
    pub software: String,
    pub id: String,
    #[serde(rename = "wd")]
    pub wallet_id: String,
    #[serde(rename = "k")]
    pub this_miner_chipid: String, // this_miner_chipid (whatever this is)
    #[serde(rename = "pw")]
    pub password: String,
    #[serde(rename = "ls")]
    pub last_min_shares: u16, // if it exceeds that would be ~1.1k shares per SECOND..
    #[serde(rename = "it")]
    pub iot_reading: String,
    #[serde(rename = "pg")]
    pub ping: f32,
    #[serde(rename = "rw")]
    pub reward: f32,
    pub ip: String, // still annoying..
    #[serde(skip_serializing, skip_deserializing)]
    pub server_miners: u64,
    #[serde(skip_serializing, skip_deserializing)]
    pub amount_of_workers: f32,
    #[serde(skip_serializing, skip_deserializing)]
    pub last_share_reset: u64,
    #[serde(skip_serializing, skip_deserializing)]
    pub share_times_list: Vec<f32>,
    #[serde(skip_serializing, skip_deserializing)]
    pub reject_shares: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub override_diff: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockInfos {
    time_since_epoch_millis: u64,
    finder: String,
    amount: f32, // reward
    algorithm: String,
    hash: String,
}

impl BlockInfos {
    pub fn new(
        time_since_epoch_millis: u64,
        finder: String,
        amount: f32,
        algorithm: String,
        hash: String,
    ) -> Self {
        BlockInfos {
            time_since_epoch_millis,
            finder,
            amount,
            algorithm,
            hash,
        }
    }
}
