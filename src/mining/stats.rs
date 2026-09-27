use crate::data::data::ConnId;
use crate::mining::id_allocator::WorkerSlot;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};

// Same window as the previous VecDeque: after pushing the 51st sample the oldest is dropped.
const SHARE_TIME_WINDOW: usize = 50;

// Fixed-size ring buffer for the rolling share times, stored inline in MinerStat
// so it never allocates.
#[derive(Debug, Clone)]
pub struct ShareTimes {
    buf: [f32; SHARE_TIME_WINDOW],
    head: u8, // index of the oldest sample
    len: u8,
}

impl Default for ShareTimes {
    fn default() -> Self {
        ShareTimes {
            buf: [0f32; SHARE_TIME_WINDOW],
            head: 0,
            len: 0,
        }
    }
}

impl ShareTimes {
    // Appends a sample and returns the evicted one, if the window was full
    pub fn push(&mut self, val: f32) -> Option<f32> {
        if (self.len as usize) < SHARE_TIME_WINDOW {
            self.buf[(self.head as usize + self.len as usize) % SHARE_TIME_WINDOW] = val;
            self.len += 1;
            None
        } else {
            let oldest = std::mem::replace(&mut self.buf[self.head as usize], val);
            self.head = ((self.head as usize + 1) % SHARE_TIME_WINDOW) as u8;
            Some(oldest)
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    // oldest to newest
    pub fn iter(&self) -> impl Iterator<Item = f32> + '_ {
        (0..self.len as usize).map(|i| self.buf[(self.head as usize + i) % SHARE_TIME_WINDOW])
    }
}

impl Serialize for ShareTimes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

// Live per-connection state. The display-only fields (sft, id, wd, h, d, rw) aren't kept
// here, they are written straight into the published MinerStatSlim (see mining::Published)
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
    // al
    // pub algorithm: CompactString,
    // p
    // pub pool_name: CompactString,
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
    pub total_share_times: f32, // adds all items from share_time_list
    pub share_times_list: ShareTimes,
    pub reject_shares: Option<&'static str>, // the full "BAD,<reason>\n" response
    pub override_diff: CompactString,
    pub is_req_diff_esp32: bool, // because "ESP32" only adds 0.5..
}

impl MinerStat {
    pub fn new(ip: CompactString, username: CompactString) -> MinerStat {
        let mut default = MinerStat::default();
        default.ip = ip;
        default.username = username;
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
    // The display-only fields (software, id, wallet_id, hashrate, difficulty, reward) start
    // empty/zero, the caller writes them (mining::Published) before publishing
    pub fn new(miner_stat: &MinerStat) -> MinerStatSlim {
        MinerStatSlim {
            username: miner_stat.username.clone(),
            share_time: miner_stat.share_time,
            accepted_shares: miner_stat.accepted_shares,
            rejected_shares: miner_stat.rejected_shares,
            // algorithm: miner_stat.algorithm.clone(),
            // pool_name: miner_stat.pool_name.clone(),
            this_miner_chipid: miner_stat.this_miner_chipid.clone(),
            password: miner_stat.password.clone(),
            last_min_shares: miner_stat.last_min_shares,
            iot_reading: miner_stat.iot_reading.clone(),
            ping: miner_stat.ping,
            ip: miner_stat.ip.clone(),
            // amount_of_workers: miner_stat.amount_of_workers,
            amount_of_workers: miner_stat.worker_slot.id(),
            ..MinerStatSlim::default()
        }
    }

    // Same result as `new` for the fields taken from the live state, but reuses the existing
    // string buffers instead of reallocating. The display-only fields are left as they are
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

// JSON bytes of one entry with every string empty and every number as short as possible
// (`"<16 hex>":{"u":"","h":0.0,...,"al":"DUCO-S1","p":""}`), without the pool name
const MIN_ENTRY_JSON_BYTES: usize = 172;

impl StatsWithContext<'_> {
    // never more than the serialized size, so it can pre-size the buffer without over-allocating
    pub fn min_json_len(&self) -> usize {
        2 + self.stats.len() * (MIN_ENTRY_JSON_BYTES + self.pool_name.len())
    }
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
    finder: CompactString,
    amount: f32, // reward
    algorithm: CompactString,
    hash: String,
}

impl BlockInfos {
    pub fn new(
        time_since_epoch_millis: u64,
        finder: CompactString,
        amount: f32,
        algorithm: CompactString,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn share_times_match_previous_vecdeque_window() {
        let mut ring = ShareTimes::default();
        let mut ring_total = 0f32;
        let mut deque = VecDeque::new();
        let mut deque_total = 0f32;

        for i in 0..137 {
            let val = (i as f32 * 0.37).sin().abs() * 3.0;

            ring_total += val;
            if let Some(old) = ring.push(val) {
                ring_total -= old;
            }

            deque.push_back(val);
            deque_total += val;
            if deque.len() > 50 {
                deque_total -= deque.pop_front().unwrap_or_else(|| 0f32);
            }

            assert_eq!(ring.len(), deque.len());
            assert_eq!(ring_total.to_bits(), deque_total.to_bits());
            assert!(ring.iter().eq(deque.iter().copied()));
        }
        assert_eq!(
            serde_json::to_string(&ring).unwrap(),
            serde_json::to_string(&deque).unwrap()
        );
    }

    #[test]
    fn min_json_len_is_exact_for_the_smallest_entry() {
        let stats = dashmap::DashMap::new();
        let ctx = StatsWithContext {
            stats: &stats,
            pool_name: "pool",
        };
        assert_eq!(ctx.min_json_len(), serde_json::to_vec(&ctx).unwrap().len());

        stats.insert(ConnId(0), MinerStatSlim::default());
        assert_eq!(ctx.min_json_len(), serde_json::to_vec(&ctx).unwrap().len());

        stats.insert(ConnId(u64::MAX), MinerStat::new("::1".into(), "bob".into()).convert_slim());
        assert!(ctx.min_json_len() <= serde_json::to_vec(&ctx).unwrap().len());
    }

    #[test]
    fn conn_id_keys_are_16_lowercase_hex_digits() {
        let stats = dashmap::DashMap::new();
        stats.insert(ConnId(0x00ab_0000_0000_00ff), MinerStatSlim::default());
        let json = serde_json::to_value(&stats).unwrap();
        assert!(json.get("00ab0000000000ff").is_some(), "{json}");
        assert_eq!(ConnId(0x0f).to_string(), "000000000000000f");
    }

    #[test]
    fn update_from_matches_new() {
        let mut stat = MinerStat::new("127.0.0.1".into(), "alice".into());
        let mut slim = MinerStatSlim::new(&stat);

        stat.iot_reading = "an-iot-reading-that-is-longer-than-24-bytes".into();
        stat.password = "cGFzc3dvcmQ=".into();
        stat.accepted_shares = 12;
        stat.ping = 0.25;
        stat.this_miner_chipid = "chip".into();
        slim.update_from(&stat);

        assert_eq!(
            serde_json::to_value(&slim).unwrap(),
            serde_json::to_value(MinerStatSlim::new(&stat)).unwrap()
        );
    }
}
