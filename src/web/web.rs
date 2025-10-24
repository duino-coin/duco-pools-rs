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

pub async fn start_webserver(addr: SocketAddr) -> JoinHandle<()> {
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
    info!("Binding webserver to to {addr:?}");
    let serve = warp::serve(ping).bind(addr).await;

    let web = tokio::spawn(serve.run());
    web
}
