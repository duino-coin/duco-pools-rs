mod cfg;
mod data;
mod handler;
mod kolka;
mod mining;
mod sync;
mod web;

use std::net::SocketAddr;
use crate::cfg::config::PoolConfig;
use crate::data::data::{Bans};
use crate::kolka::kolka::Kolka;
use env_logger::Target;
use log::{debug, error, info, LevelFilter};
use rand::Rng;
use serde::Serialize;
use serde_json::Value;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use sysinfo::System;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
pub static CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize)]
struct HttpRes {
    result: String,
    success: bool,
}

// #[cfg(not(debug_assertions))] are used for functions we do not want to be called while testing (as not to disrupt production services)

#[tokio::main]
async fn main() {
    // initialize logging
    env_logger::builder()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .init();

    let mut config = cfg::config::load_config().unwrap();
    let mut sys = Arc::new(Mutex::new(System::new()));

    if config.guess_port {
        let port = get_rand(1000, 9999) as u32;
        config.port = port;
        cfg::config::save_config(&config);
    }
    // TODO: impl ngrok

    sync::sync::update_pool_rewards(&config)
        .await
        .expect("failed to update pool rewards");
    #[cfg(not(debug_assertions))]
    #[cfg(not(compile_env="test"))]
    sync::sync::login(&config).await.expect("failed to login");

    let config_arc = Arc::new(config);
    let config_arc_clone = config_arc.clone();

    let mut sys_clone = sys.clone();
    let sync_thread = tokio::spawn(async move {
        info!("Starting sync thread..");
        let config = config_arc_clone;
        let sys = &mut sys_clone;
        #[cfg(not(debug_assertions))]
        #[cfg(not(compile_env="test"))]
        loop {
            match sync::sync::sync(&config, sys).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Sync failed: {}", err);
                }
            };
            tokio::time::sleep(tokio::time::Duration::from_secs(config.sync_time as u64)).await;
        }
    });

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    ctrlc::set_handler(move || {
        tx.send("s".to_owned()).unwrap();
    })
    .expect("Failed to handle ctrl-c");

    let config_arc_clone = config_arc.clone();
    start_tcp(config_arc_clone, rx).await;
    sync_thread.abort();
    // last sync before shutdown
    #[cfg(not(debug_assertions))]
    #[cfg(not(compile_env="test"))]
    sync::sync::sync(&config_arc, &mut sys)
        .await
        .expect("failed to sync database");
    #[cfg(not(debug_assertions))]
    #[cfg(not(compile_env="test"))]
    sync::sync::logout(&config_arc)
        .await
        .expect("failed to logout");
}

async fn start_tcp(config: Arc<PoolConfig>, mut rx: UnboundedReceiver<String>) {
    info!("Starting tcp server...");
    // read and serialize pool rewards
    let reward_content = std::fs::read_to_string("./config/poolRewards.json")
        .expect("failed to read config poolRewards.json");
    let pool_rewards: Value =
        serde_json::from_str(&reward_content).expect("failed to parse config poolRewards.json");

    // serialize Bans struct
    let bans = Bans::load_bans().expect("failed to load config bans.json");
    let ban_mutex = Arc::new(Mutex::new(bans));

    let pool_arc = Arc::new(pool_rewards);
    let kolka = Kolka::new(pool_arc.clone());
    let kolka_arc = Arc::new(kolka);

    let server = TcpListener::bind(format!("{}:{}", config.host, config.port))
        .await
        .expect("failed to bind to address");
    info!("Listening on {}:{}", config.host, config.port);
    let running = Arc::new(AtomicBool::new(true));

    let handles = JoinSet::new();
    let handle_arc = Arc::new(Mutex::new(handles));
    // let mut arc_clone = handle_arc.clone();
    let run_arc = running.clone();

    // start "webserver" and parse addr from config
    let addr: SocketAddr = (&format!("{}:{}", config.host, config.dashboard_port))
        .parse()
        .expect("failed to parse host:port");
    let web_handle = web::web::start_webserver(addr).await;

    let handle_clone = handle_arc.clone();
    let tcp = tokio::spawn(async move {
        while run_arc.load(Relaxed) {
            let (socket, addr) = server.accept().await.unwrap();

            // info!("Connection established");
            let ban_clone = ban_mutex.clone();
            let conf_clone = config.clone();
            let pool_arc_clone = pool_arc.clone();
            let kolka_clone = kolka_arc.clone();

            // add connection
            CONNECTIONS.fetch_add(1, Relaxed);
            handle_clone.lock().await.spawn(async move {
                // Self::handle_connection(stream, cpu_limit_clone).await;
                match handler::handler::handle_connection(
                    socket,
                    addr,
                    ban_clone,
                    conf_clone,
                    pool_arc_clone,
                    kolka_clone,
                )
                .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        if !err.to_string().contains("Connection reset by peer") && !err.to_string().contains("Connection timed out")
                        {
                            info!("Connection closed: {:?}", err);
                        }
                    }
                };
                // remove connection
                CONNECTIONS.fetch_sub(1, Relaxed);
            });
        }
    });

    while let Some(_) = rx.recv().await {
        // waits until it receives a message (signals a shutdown)
        info!("Received abort");
        running.store(false, Relaxed);
        break;
    }

    web_handle.abort();
    tcp.abort();
    handle_arc.lock().await.abort_all();
}

fn get_rand(min: usize, max: usize) -> usize {
    let mut rng = rand::rng();

    rng.random_range(min..max)
}
