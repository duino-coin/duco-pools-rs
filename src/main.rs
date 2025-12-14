mod cfg;
mod data;
mod handler;
mod kolka;
mod mining;
mod sync;
mod web;

use crate::cfg::config::PoolConfig;
use crate::data::data::{Bans, Connection};
use crate::kolka::kolka::Kolka;
use dashmap::DashMap;
use env_logger::Target;
use log::{error, info, LevelFilter};
use mimalloc::MiMalloc;
use rand::{Rng, RngCore};
use std::net::SocketAddr;
use std::process::{exit, Command};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;
use sysinfo::System;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
// use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub static CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

// doesn't look nice, but it's fine because these live during the entire runtime anyway
// and it's cleaner than cloning hundreds of arcs..
pub static BANS: LazyLock<Mutex<Bans>> = LazyLock::new(|| Mutex::new(Bans::load_bans().unwrap()));

// pub static POOL_REWARDS: OnceLock<HashMap<String, sync::sync::Difficulty>> = OnceLock::new();
pub static POOL_REWARDS: LazyLock<DashMap<String, sync::sync::Difficulty>> =
    LazyLock::new(|| DashMap::new());

pub static KOLKA: OnceLock<Kolka> = OnceLock::new();

pub static CONFIG: OnceLock<PoolConfig> = OnceLock::new();

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// #[cfg(not(debug_assertions))] are used for functions we do not want to be called while testing (as not to disrupt production services)

#[tokio::main]
async fn main() {
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
    let mut restarter: Option<JoinHandle<()>> = None;
    if config.auto_restart > 0 && !config.restart_script.is_empty() {
        info!(
            "Auto restart is enabled. Restarting every {} minutes.",
            config.auto_restart
        );
        let restart_delay = (config.auto_restart.clone() * 60) as u64;
        let script_path = config.restart_script.clone();

        restarter = Some(tokio::spawn(async move {
            loop {
                let mut is_error = false;
                tokio::time::sleep(Duration::from_secs(restart_delay)).await;

                match Command::new(&script_path).spawn() {
                    Ok(_) => {}
                    Err(err) => {
                        is_error = true;
                        error!("Failed to run restart script: {}", err);
                    }
                };
                // don't exit if the replacement server couldn't be spawned
                if !is_error {
                    exit(0);
                }
            }
        }));
    }
    // TODO: impl ngrok

    // TODO: test
    sync::sync::resync_pool_rewards(&config)
        .await
        .expect("failed to update pool rewards");
    // sync::sync::update_pool_rewards(&config)
    //     .await
    //     .expect("failed to update pool rewards");
    #[cfg(not(debug_assertions))]
    sync::sync::login(&config).await.expect("failed to login");

    CONFIG.get_or_init(|| config);

    let mut sys_clone = sys.clone();
    let sync_thread = tokio::spawn(async move {
        info!("Starting sync thread..");
        #[cfg(not(debug_assertions))]
        {
            let config = crate::CONFIG.get().unwrap();
            let pool_reward_sync_factor = (3600f64 / config.sync_time as f64).ceil() as u64;
            let mut count = 0;
            let sys = &mut sys_clone;
            loop {
                if count >= pool_reward_sync_factor {
                    count = 0;
                    match sync::sync::resync_pool_rewards(&config).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Failed to resync pool rewards: {}", err);
                        }
                    };
                }
                match sync::sync::sync(sys).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Sync failed: {}", err);
                    }
                };

                count += 1;
                tokio::time::sleep(tokio::time::Duration::from_secs(config.sync_time as u64)).await;
            }
        }
    });
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    ctrlc::set_handler(move || {
        tx.send("s".to_owned()).unwrap();
    })
    .expect("Failed to handle ctrl-c");

    start_tcp(rx).await;
    // when start_tcp yields, the server was ctrl-c'd
    if restarter.is_some() {
        restarter.take().unwrap().abort();
    }
    sync_thread.abort();
    // last sync before shutdown
    #[cfg(not(debug_assertions))]
    {
        sync::sync::sync(&mut sys)
            .await
            .expect("failed to sync database");
        sync::sync::logout(crate::CONFIG.get().unwrap())
            .await
            .expect("failed to logout");
    }
}

async fn start_tcp(mut rx: UnboundedReceiver<String>) {
    // set max open file descriptors to the maximum the platform allows, so we can handle *a lot* of sockets
    rlimit::increase_nofile_limit(u64::MAX).expect("failed to increase nofile limit");
    info!("Starting tcp server...");

    KOLKA.get_or_init(|| Kolka::new());

    let config = CONFIG.get().unwrap();
    let server = TcpListener::bind(format!("{}:{}", config.host, config.port))
        .await
        .expect("failed to bind to address");
    info!("Listening on {}:{}", config.host, config.port);

    // start "webserver" and parse addr from config
    let addr: SocketAddr = (&format!("{}:{}", config.host, config.dashboard_port))
        .parse()
        .expect("failed to parse host:port");
    let web_handle = web::web::start_webserver(addr).await;

    let tracker = tokio_util::task::TaskTracker::new();
    let token = CancellationToken::new();
    let token_clone = token.clone();
    let tcp = tokio::spawn(async move {
        loop {
            match server.accept().await {
                Ok((socket, addr)) => {
                    let local_token = token_clone.clone();
                    // add connection
                    CONNECTIONS.fetch_add(1, Relaxed);
                    tracker.spawn(async move {
                        // returns the future once *ONE* of the tasks completes
                        // e.g. either the miner disconnects OR the token is cancelled
                        tokio::select! {
                            () = async move {
                                let mut bytes = [0; 8];
                                rand::rng().fill_bytes(&mut bytes);
                                let id = hex::encode(&bytes);
                                let mut conn = Connection::new(socket, addr, String::new(), id);
                                match handler::handler::handle_connection(
                                    &mut conn,
                                )
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(err) => {
                                        let err_str = err.to_string();
                                        // this is impl seriously is horrendous
                                        if !err_str.contains("Connection reset by peer") && !err_str.contains("Connection timed out")
                                            && !err_str.contains("Shutting down") && !err_str.contains("os error 10054") && !err_str.contains("Connection closed")
                                            && !err_str.contains("deadline")
                                        {
                                            info!("Connection closed: {:?}", err);
                                        }
                                    }
                                };
                                handler::handler::handle_close(&mut conn);
                                // to drop the socket and (hopefully) close it
                                drop(conn);
                                // remove connection
                                CONNECTIONS.fetch_sub(1, Relaxed);
                            } => {

                            },
                            () = local_token.cancelled() => {
                                info!("Cancelled task.");
                            },
                        }
                    });
                }
                Err(err) => {
                    error!("Accepting connection failed: {}", err);
                }
            }
        }
    });
    while let Some(_) = rx.recv().await {
        // waits until it receives a message (signals a shutdown)
        info!("Received abort");
        #[cfg(compile_env = "test")]
        panic!("Received abort");
        break;
    }

    info!("Stopping tcp server...");
    web_handle.abort();
    tcp.abort();
    token.cancel();
}

fn get_rand(min: usize, max: usize) -> usize {
    let mut rng = rand::rng();

    rng.random_range(min..max)
}
