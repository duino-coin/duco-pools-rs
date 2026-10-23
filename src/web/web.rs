use crate::data::data::ConnId;
use crate::mining::id_allocator::WorkerAllocator;
use crate::mining::stats::MinerStatSlim;
use crate::{mining, CONFIG, CONNECTIONS};
use compact_str::CompactString;
use dashmap::DashMap;
use log::info;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use tokio::task::JoinHandle;
use warp::Filter;

#[derive(Serialize)]
struct HttpRes {
    result: &'static str,
    success: bool,
}

// serialized straight from the maps, without building an intermediate serde_json::Value
#[derive(Serialize)]
struct DataRes<'a> {
    connections: usize,
    miner_stats: StatsWithoutKeys<'a>, // keys serialize as 16 hex chars
    workers: WorkersWithoutLen<'a>,
}

// DashMap's own Serialize passes len() as a hint, a stale Some(0) makes serde_json close the
// object early, so a worker joining meanwhile would produce invalid JSON
struct WorkersWithoutLen<'a>(&'a DashMap<CompactString, WorkerAllocator>);

impl Serialize for WorkersWithoutLen<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        // not collect_map, the entries borrow from their guard
        let mut map = serializer.serialize_map(None)?;
        for entry in self.0.iter() {
            map.serialize_entry(entry.key(), entry.value())?;
        }
        map.end()
    }
}

// MINER_STATS as-is, minus the mining keys (pw), blanked on a per-entry copy while serializing
struct StatsWithoutKeys<'a>(&'a DashMap<ConnId, MinerStatSlim>);

impl Serialize for StatsWithoutKeys<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().map(|entry| {
            let stat = MinerStatSlim {
                password: CompactString::default(),
                ..entry.value().clone()
            };
            (*entry.key(), stat)
        }))
    }
}

pub async fn start_webserver(addr: SocketAddr) -> JoinHandle<()> {
    let ping = warp::path("ping")
        .and(warp::get())
        .and(warp::path::end())
        .map(|| {
            let res = HttpRes {
                result: "Pong!",
                success: true,
            };
            warp::reply::json(&res)
        });

    let dash_key = CONFIG.get().unwrap().dashboard_key.as_str();
    let dash_enabled = dash_key.len() > 20;
    let data = warp::path("data")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(warp::path::end())
        .and_then(
            move |params: std::collections::HashMap<String, String>| async move {
                if dash_enabled {
                    if let Some(provided_key) = params.get("key") {
                        if ct_eq(provided_key, dash_key) {
                            let data_res = DataRes {
                                connections: CONNECTIONS.load(Relaxed),
                                miner_stats: StatsWithoutKeys(&mining::mining::MINER_STATS),
                                workers: WorkersWithoutLen(&mining::mining::USR_WORKERS_ALLOC),
                            };
                            return Ok(warp::reply::json(&data_res));
                        }
                    }
                }
                Err(warp::reject::not_found())
            },
        );

    info!("Binding webserver to to {addr:?}");
    let route = ping.or(data);
    let serve = warp::serve(route).bind(addr).await;

    tokio::spawn(serve.run())
}

// constant time equals check, prevents timing-based brute forcing
fn ct_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}
