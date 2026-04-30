use crate::mining;
use log::info;
use serde::Serialize;
use std::net::SocketAddr;
use dashmap::DashMap;
use tokio::task::JoinHandle;
use warp::Filter;
use crate::mining::stats::MinerStat;

#[derive(Serialize)]
struct HttpRes {
    result: String,
    success: bool,
}

#[derive(Serialize)]
struct DataRes {
    workers: DashMap<String, f32>,
    miner_stats: DashMap<String, MinerStat>
}

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
    let data = warp::path("data")
        // .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(warp::path::end())
        .and_then(move |params: std::collections::HashMap<String, String>| {
            let dash_key_clone = dash_key.to_string();
            async move {
                if params.contains_key("key") {
                    let provided_key = params.get("key").unwrap();
                    if provided_key == &dash_key_clone {
                        let data_res = DataRes { workers: mining::mining::USR_WORKERS.clone(), miner_stats: mining::mining::MINER_STATS.clone() };
                        return Ok(warp::reply::json(&data_res));
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
