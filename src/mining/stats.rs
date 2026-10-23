use crate::data::data::ConnId;
use crate::mining::id_allocator::WorkerSlot;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};

// Live per-connection state. The display-only fields (sft, id, wd, h, d, rw) aren't kept
// here, they are written straight into the published MinerStatSlim (see mining::Session::commit)
// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[derive(Debug, Default, Clone)]
pub struct MinerStat {
    // u
    pub username: CompactString, // this is stupid..
    // s
    pub share_time: f32,
    // a
    pub accepted_shares: u64,
    // r
    pub rejected_shares: u64,
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
    // ip
    pub ip: CompactString, // still annoying..
    // c
    pub worker_slot: WorkerSlot,

    // NEVER SERIALIZED:
    pub last_share_reset: u64,
    pub reject_shares: Option<&'static str>, // the full "BAD,<reason>\n" response
    pub is_req_diff_esp32: bool,             // because "ESP32" only adds 0.5..
}

impl MinerStat {
    pub fn new(ip: CompactString, username: CompactString) -> MinerStat {
        MinerStat {
            ip,
            username,
            ..Default::default()
        }
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
    #[serde(rename = "d")]
    pub difficulty: u32,
    #[serde(rename = "sft")]
    pub software: CompactString,
    pub id: CompactString,
    #[serde(rename = "t")]
    pub timestamp: u32, // unix secs of the last update, the server uses it to tell active miners; u32: fine until 2106 and fits within struct without extra bytes
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
    // Copies the fields taken from the live state, reusing the existing string buffers;
    // the display-only fields (software, id, timestamp, wallet_id, hashrate, difficulty, reward)
    // are left as they are, the caller writes them (mining::Session::commit)
    pub fn update_from(&mut self, miner_stat: &MinerStat) {
        fn set(dst: &mut CompactString, src: &CompactString) {
            if dst != src {
                dst.clear();
                dst.push_str(src);
            }
        }
        set(&mut self.username, &miner_stat.username);
        self.share_time = miner_stat.share_time;
        self.accepted_shares = miner_stat.accepted_shares;
        self.rejected_shares = miner_stat.rejected_shares;
        set(&mut self.this_miner_chipid, &miner_stat.this_miner_chipid);
        set(&mut self.password, &miner_stat.password);
        self.last_min_shares = miner_stat.last_min_shares;
        set(&mut self.iot_reading, &miner_stat.iot_reading);
        self.ping = miner_stat.ping;
        set(&mut self.ip, &miner_stat.ip);
        self.amount_of_workers = miner_stat.worker_slot.id();
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
    pub stats: &'a dashmap::DashMap<ConnId, MinerStatSlim>,
    pub pool_name: &'a str,
}

impl<'a> serde::Serialize for StatsWithContext<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        // found by Opus 5.5
        // no length: DashMap::len() read-locks every shard, and a stale Some(0) makes serde_json
        // close the object early, so an entry inserted meanwhile would produce invalid JSON.
        // not collect_map, the context borrows from the entry guard
        let mut map = serializer.serialize_map(None)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_id_keys_are_16_lowercase_hex_digits() {
        let stats = dashmap::DashMap::new();
        stats.insert(ConnId(0x00ab_0000_0000_00ff), MinerStatSlim::default());
        let json = serde_json::to_value(&stats).unwrap();
        assert!(json.get("00ab0000000000ff").is_some(), "{json}");
        assert_eq!(ConnId(0x0f).to_string(), "000000000000000f");
    }
}
