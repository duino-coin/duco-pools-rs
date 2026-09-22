use crate::mining::id_allocator::WorkerSlot;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[derive(Debug, Default, Clone)]
pub struct MinerStat {
    // u
    pub username: CompactString, // this is stupid..
    // h
    pub hashrate: f32,
    // s
    pub share_time: f32,
    // a
    pub accepted_shares: u64,
    // r
    pub rejected_shares: u64,
    // al
    // pub algorithm: CompactString,
    // d
    pub difficulty: u32,
    // p
    // pub pool_name: CompactString,
    // sft
    pub software: CompactString,
    // id
    pub id: CompactString,
    // wd
    pub wallet_id: CompactString,
    // k
    pub this_miner_chipid: CompactString, // this_miner_chipid (whatever this is)
    // pw
    pub password: CompactString,
    // ls
    pub last_min_shares: u16, // if it exceeds that would be ~1.1k shares per SECOND..
    // it
    pub iot_reading: CompactString,
    // pg
    pub ping: f32,
    // rw
    pub reward: f32,
    // ip
    pub ip: CompactString, // still annoying..
    // c
    pub amount_of_workers: f32, // it's actually the id for the current worker (read: nth worker)
    pub worker_slot: WorkerSlot,

    // NEVER SERIALIZED:
    pub last_share_reset: u64,
    pub total_share_times: f32, // adds all items from share_time_list
    pub share_times_list: VecDeque<f32>,
    pub reject_shares: CompactString,
    pub override_diff: CompactString,
    pub is_req_diff_esp32: bool, // because "ESP32" only adds 0.5..
}

impl MinerStat {
    pub fn new(ip: String, username: String) -> MinerStat {
        let mut default = MinerStat::default();
        default.ip = CompactString::from(ip);
        default.username = CompactString::from(username);
        default
    }

    pub fn convert_slim(&self) -> MinerStatSlim {
        MinerStatSlim::new(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MinerStatSlim {
    #[serde(rename = "u")]
    pub username: CompactString, // this is stupid..
    #[serde(rename = "h")]
    pub hashrate: f32,
    #[serde(rename = "s")]
    pub share_time: f32,
    #[serde(rename = "a")]
    pub accepted_shares: u64,
    #[serde(rename = "r")]
    pub rejected_shares: u64,
    // #[serde(rename = "al")]
    // pub algorithm: CompactString,
    #[serde(rename = "d")]
    pub difficulty: u32,
    // #[serde(rename = "p")]
    // pub pool_name: CompactString,
    #[serde(rename = "sft")]
    pub software: CompactString,
    pub id: CompactString,
    #[serde(rename = "wd")]
    pub wallet_id: CompactString,
    #[serde(rename = "k")]
    pub this_miner_chipid: CompactString, // this_miner_chipid (whatever this is)
    #[serde(rename = "pw")]
    pub password: CompactString,
    #[serde(rename = "ls")]
    pub last_min_shares: u16, // if it exceeds that would be ~1.1k shares per SECOND..
    #[serde(rename = "it")]
    pub iot_reading: CompactString,
    #[serde(rename = "pg")]
    pub ping: f32,
    #[serde(rename = "rw")]
    pub reward: f32,
    pub ip: CompactString, // still annoying..
    #[serde(rename = "c")]
    pub amount_of_workers: f32, // it's actually the id for the current worker (read: nth worker)
}

impl MinerStatSlim {
    pub fn new(miner_stat: &MinerStat) -> MinerStatSlim {
        MinerStatSlim {
            username: miner_stat.username.clone(),
            hashrate: miner_stat.hashrate,
            share_time: miner_stat.share_time,
            accepted_shares: miner_stat.accepted_shares,
            rejected_shares: miner_stat.rejected_shares,
            // algorithm: miner_stat.algorithm.clone(),
            difficulty: miner_stat.difficulty,
            // pool_name: miner_stat.pool_name.clone(),
            software: miner_stat.software.clone(),
            id: miner_stat.id.clone(),
            wallet_id: miner_stat.wallet_id.clone(),
            this_miner_chipid: miner_stat.this_miner_chipid.clone(),
            password: miner_stat.password.clone(),
            last_min_shares: miner_stat.last_min_shares,
            iot_reading: miner_stat.iot_reading.clone(),
            ping: miner_stat.ping,
            reward: miner_stat.reward,
            ip: miner_stat.ip.clone(),
            // amount_of_workers: miner_stat.amount_of_workers,
            amount_of_workers: miner_stat.worker_slot.id(),
        }
    }
}

#[derive(Serialize)]
struct MinerStatSlimContext<'a> {
    #[serde(flatten)]
    stat: &'a MinerStatSlim, // inlines all existing fields
    #[serde(rename = "al")]
    algorithm: &'a str,
    #[serde(rename = "p")]
    pool_name: &'a str,
}

pub struct StatsWithContext<'a> {
    pub stats: &'a dashmap::DashMap<Box<str>, MinerStatSlim>,
    pub pool_name: &'a str,
}
impl<'a> serde::Serialize for StatsWithContext<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.stats.len()))?;
        for entry in self.stats.iter() {
            let ctx = MinerStatSlimContext {
                stat: entry.value(),
                algorithm: "DUCO-S1",
                pool_name: self.pool_name,
            };
            map.serialize_entry(entry.key(), &ctx)?;
        }
        map.end()
    }
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
