#![allow(clippy::module_inception)]

mod cfg;
mod data;
mod handler;
mod kolka;
mod mining;
mod sync;
mod updater;
mod web;

use crate::cfg::config::PoolConfig;
use crate::data::data::{Bans, ConnId, Connection, ConnectionClosed};
use compact_str::CompactString;
use env_logger::Target;
use log::{debug, error, info, LevelFilter};
use mimalloc::MiMalloc;
use rand::{Rng, RngExt};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{LazyLock, OnceLock};
use std::time::Duration;
use arc_swap::ArcSwap;
use sysinfo::System;
use tokio::net::TcpListener;
// tokio's Child reaps itself in the background when dropped, std's leaves a zombie
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use crate::sync::sync::Curve;

pub static CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

// pause after a failed accept, so closing connections can free descriptors
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

struct ConnectionCounterGuard;

impl ConnectionCounterGuard {
    fn new() -> Self {
        CONNECTIONS.fetch_add(1, Relaxed);
        Self
    }
}

impl Drop for ConnectionCounterGuard {
    fn drop(&mut self) {
        // can't underflow, every guard incremented the counter in `new`
        CONNECTIONS.fetch_sub(1, Relaxed);
    }
}

// doesn't look nice, but it's fine because these live during the entire runtime anyway
// and it's cleaner than cloning hundreds of arcs..
pub static BANS: LazyLock<Mutex<Bans>> = LazyLock::new(|| Mutex::new(Bans::load_bans().unwrap()));

pub static CURVE_VALUES: LazyLock<ArcSwap<Curve>> = LazyLock::new(|| ArcSwap::from_pointee(Curve::default()));


pub static CONFIG: OnceLock<PoolConfig> = OnceLock::new();

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// #[cfg(not(debug_assertions))] are used for functions we do not want to be called while testing (as not to disrupt production services)

#[tokio::main]
async fn main() {
    env_logger::builder()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    let mut config = cfg::config::load_config().unwrap();

    if config.guess_port {
        let port = get_rand(1000, 9999) as u32;
        config.port = port;
        cfg::config::save_config(&config);
    }
    let mut restarter: Option<JoinHandle<()>> = None;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    if config.auto_restart > 0 && !config.restart_script.is_empty() {
        info!(
            "Auto restart is enabled. Restarting every {} minutes.",
            config.auto_restart
        );
        let restart_delay = (config.auto_restart * 60) as u64;
        let script_path = config.restart_script.clone();
        let restart_tx = tx.clone();
        restarter = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(restart_delay)).await;

                match Command::new(&script_path).spawn() {
                    Ok(_) => restart_tx.send(()).unwrap(),
                    // don't exit if the replacement server couldn't be spawned
                    Err(err) => error!("Failed to run restart script: {}", err),
                }
            }
        }));
    }
    // TODO: impl ngrok

    sync::sync::resync_curve(&config)
        .await
        .expect("failed to update pool rewards");

    #[cfg(not(debug_assertions))]
    sync::sync::login(&config).await.expect("failed to login");
    // TODO: seems to work, needs to be tested in a production setting (and the repo set to public!)
    let mut update_thread = None;
    if config.auto_update && !config.update_restart_script.is_empty() {
        let update_shutdown = tx.clone();
        let update_restart_script = config.update_restart_script.clone();
        update_thread = Some(tokio::spawn(async move {
            loop {
                match updater::updater::fetch_latest_version(Duration::from_secs(
                    config.timeout as u64,
                ))
                    .await
                {
                    Ok(Some(update)) => {
                        match updater::updater::spawn_update_restart(
                            &update,
                            &update_restart_script,
                        )
                            .await
                        {
                            Ok(helper) => {
                                info!(
                                "Update {} is ready; shutting down before replacement",
                                update.version
                            );
                                if update_shutdown.send(()).is_ok() {
                                    helper.detach();
                                }
                            }
                            Err(err) => error!("Failed to prepare update restart: {err:#}"),
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        error!("Failed to download update: {err:#}");
                    }
                };
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    config.update_check_time_seconds,
                )).await;
            }
        }));
    }
    CONFIG.get_or_init(|| config);
    // before any share is accepted or synced
    #[cfg(all(not(feature = "dummy_sync"), not(debug_assertions)))]
    sync::sync::load_pending_rewards(CONFIG.get().unwrap())
        .expect("failed to load pending rewards");
    // stops the sync loop between syncs, aborting it could cut off an upload the server already took
    let sync_token = CancellationToken::new();
    #[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
    let sync_stop = sync_token.clone();
    // returns the last pool ip, so the final sync can fall back to it, and the System it refreshes
    let sync_thread = tokio::spawn(async move {
        info!("Starting sync thread..");
        #[cfg_attr(
            not(any(feature = "dummy_sync", not(debug_assertions))),
            allow(unused_mut)
        )]
        let mut last_ip = String::new();
        #[cfg_attr(
            not(any(feature = "dummy_sync", not(debug_assertions))),
            allow(unused_mut)
        )]
        let mut sys = System::new();
        // only start a sync thread if: we want dummy syncs or we are in release mode
        #[cfg(any(feature = "dummy_sync", not(debug_assertions)))]
        {
            let config = crate::CONFIG.get().unwrap();
            let pool_reward_sync_factor = (3600f64 / config.sync_time as f64).ceil() as u64;
            let mut count = 0;
            loop {
                if count >= pool_reward_sync_factor {
                    count = 0;
                    match sync::sync::resync_curve(config).await {
                        Ok(_) => {
                            info!("Resynced pool rewards.");
                        }
                        Err(err) => {
                            error!("Failed to resync pool rewards: {err}");
                        }
                    };
                }
                match sync::sync::sync(&mut sys, &last_ip).await {
                    Ok(ip) => {
                        // just to be safe...
                        if !ip.is_empty() {
                            last_ip = ip;
                        }
                    }
                    Err(err) => {
                        error!("Sync failed: {err}");
                    }
                };

                count += 1;
                tokio::select! {
                    () = tokio::time::sleep(tokio::time::Duration::from_secs(config.sync_time as u64)) => {}
                    () = sync_stop.cancelled() => break,
                }
            }
        }
        (last_ip, sys)
    });

    ctrlc::set_handler(move || {
        // the receiver is gone once shutdown has started, so a second ctrl-c skips the final sync and logout
        if tx.send(()).is_err() {
            error!("Forced shutdown, pending rewards weren't synced");
            std::process::exit(1);
        }
    })
    .expect("Failed to handle ctrl-c");

    start_tcp(rx).await;
    // Stop background work before the final sync/logout on any shutdown request.
    if let Some(restarter) = restarter {
        restarter.abort();
    }
    if let Some(update_thread) = update_thread {
        update_thread.abort();
    }
    sync_token.cancel();
    // wait for a running sync, so the final one doesn't upload the same work again
    #[cfg_attr(debug_assertions, allow(unused_variables, unused_mut))]
    let (last_ip, mut sys) = sync_thread.await.unwrap_or_default();
    // last sync before shutdown
    #[cfg(not(debug_assertions))]
    {
        if let Err(err) = sync::sync::sync(&mut sys, &last_ip).await {
            error!("Final sync failed: {err}");
        }
        sync::sync::logout(crate::CONFIG.get().unwrap())
            .await
            .expect("failed to logout");
    }
}

async fn start_tcp(mut rx: UnboundedReceiver<()>) {
    // set max open file descriptors to the maximum the platform allows, so we can handle *a lot* of sockets
    rlimit::increase_nofile_limit(u64::MAX).expect("failed to increase nofile limit");
    info!("Starting tcp server...");

    let config = CONFIG.get().unwrap();

    let server = TcpListener::bind(format!("{}:{}", config.host, config.port))
        .await
        .expect("failed to bind to address");
    info!("Listening on {}:{}", config.host, config.port);

    // start "webserver" and parse addr from config
    let addr: SocketAddr = format!("{}:{}", config.host, config.dashboard_port)
        .parse()
        .expect("failed to parse host:port");
    let web_handle = web::web::start_webserver(addr).await;

    let token = CancellationToken::new();
    let token_clone = token.clone();
    let tcp = tokio::spawn(async move {
        loop {
            match server.accept().await {
                Ok((socket, addr)) => {
                    let _ = socket.set_nodelay(true);
                    let local_token = token_clone.clone();
                    tokio::spawn(async move {
                        let _connection_counter = ConnectionCounterGuard::new();
                        // returns the future once *ONE* of the tasks completes
                        // e.g. either the miner disconnects OR the token is cancelled
                        tokio::select! {
                            () = async move {
                                let id = ConnId(rand::rng().next_u64());
                                let mut conn = Connection::new(socket, addr, CompactString::default(), id);
                                match handler::handler::handle_connection(
                                    &mut conn
                                )
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(err) => {
                                        debug!("Connection closed: {:?}", err);
                                        if !is_routine_close(&err) {
                                            info!("Connection closed: {:?}", err);
                                        }
                                    }
                                };

                                // to drop the socket and (hopefully) close it
                                drop(conn);
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
                    // out of file descriptors the listener stays readable, retrying right away would spin
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
            }
        }
    });
    if rx.recv().await.is_some() {
        // waits until it receives a message (signals a shutdown)
        info!("Received abort");
        #[cfg(compile_env = "test")]
        panic!("Received abort");
    }

    info!("Stopping tcp server...");
    web_handle.abort();
    tcp.abort();
    token.cancel();
}

// closes that happen all the time and aren't worth an info line
fn is_routine_close(err: &anyhow::Error) -> bool {
    use std::io::ErrorKind::{ConnectionAborted, ConnectionReset, InvalidData, TimedOut};
    match err.downcast_ref::<std::io::Error>() {
        // InvalidData: a line over the frame limit ("Message too long")
        Some(io_err) => matches!(
            io_err.kind(),
            ConnectionReset | ConnectionAborted | TimedOut | InvalidData
        ),
        None => {
            err.is::<ConnectionClosed>()
                || err.is::<tokio::time::error::Elapsed>()
                || err.is::<std::string::FromUtf8Error>()
        }
    }
}

fn get_rand(min: usize, max: usize) -> usize {
    let mut rng = rand::rng();

    rng.random_range(min..max)
}
