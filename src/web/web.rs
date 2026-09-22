use crate::{mining, CONNECTIONS};
use log::info;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use tokio::task::JoinHandle;
use warp::Filter;

#[derive(Serialize)]
struct HttpRes {
    result: String,
    success: bool,
}

// #[derive(Serialize)]
// struct DataRes {
//     workers: DashMap<String, mining::id_allocator::WorkerAllocator>,
//     miner_stats: DashMap<String, MinerStat>
// }

pub async fn start_webserver(addr: SocketAddr, dash_key: String) -> JoinHandle<()> {
    let ping = warp::path("ping")
        .and(warp::get())
        .and(warp::path::end())
        .map(|| {
            let res = HttpRes {
                result: "Pong!".to_string(),
                success: true,
            };
            warp::reply::json(&res)
        });

    let dash_enabled = dash_key.len() > 20;
    let data = warp::path("data")
        // .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(warp::path::end())
        .and_then(move |params: std::collections::HashMap<String, String>| {
            let dash_key_clone = dash_key.to_string();
            async move {
                if dash_enabled {
                    if let Some(provided_key) = params.get("key") {
                        // if provided_key == &dash_key_clone {
                        if ct_eq(provided_key, &dash_key_clone) {
                            let workers_json =
                                serde_json::to_value(&*mining::mining::USR_WORKERS_ALLOC).unwrap();
                            let stats_json =
                                serde_json::to_value(&*mining::mining::MINER_STATS).unwrap();
                            let connections = CONNECTIONS.load(Relaxed);
                            let combined = serde_json::json!({
                                "workers": workers_json,
                                "miner_stats": stats_json,
                                "connections": connections,
                            });
                            return Ok(warp::reply::json(&combined));
                            // let data_res = DataRes { workers: mining::mining::USR_WORKERS_ALLOC.clone(), miner_stats: mining::mining::MINER_STATS.clone() };
                            // return Ok(warp::reply::json(&data_res));
                        }
                    }
                }
                Err(warp::reject::not_found())
            }
        });

    info!("Binding webserver to to {addr:?}");
    let route = ping.or(data);
    let serve = warp::serve(route).bind(addr).await;

    let web = tokio::spawn(serve.run());
    web
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
