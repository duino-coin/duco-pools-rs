use crate::mining;
use log::info;
use serde::Serialize;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use warp::Filter;

#[derive(Serialize)]
struct HttpRes {
    result: String,
    success: bool,
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
                        let mut temp = mining::mining::USR_WORKERS.clone();
                        temp.extend(mining::mining::WORKERS.clone());
                        return Ok(warp::reply::json(&temp));
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
