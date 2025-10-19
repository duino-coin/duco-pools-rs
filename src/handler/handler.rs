use crate::cfg::config::PoolConfig;
use crate::data::data::{Bans, Connection, ConnectionClosed};
use crate::kolka::kolka::Kolka;
use crate::mining;
use arcstr::ArcStr;
use log::{debug, error, info, warn};
use rand::RngCore;
use serde_json::Value;
use std::error::Error;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

//const CMD: &str = "csf -td ${ip}"; //csf
//const CMD: &str = "iptables -A INPUT -s ${ip} -j DROP"; //iptables
//const CMD: &str = "sudo ufw deny from ${ip} to any"; //ufw
const CMD: &str = ""; //none
async fn ban_ip(ip: &str) -> Result<(), anyhow::Error> {
    if !CMD.is_empty() {
        let cmd_split = CMD.split(" ").collect::<Vec<&str>>();
        let cmd = cmd_split[0].trim();
        Command::new(cmd).args(&cmd_split[1..]).spawn()?;
        warn!("Banned {ip}");
    }
    Ok(())
}

pub async fn handle_connection(
    socket: TcpStream,
    addr: SocketAddr,
    bans: Arc<Mutex<Bans>>,
    config: Arc<PoolConfig>,
    pool_rewards: Arc<Value>,
    kolka: Arc<Kolka>,
) -> Result<(), anyhow::Error> {
    // println!("Connection from {}", addr);
    let mut bytes = [0; 8];
    rand::rng().fill_bytes(&mut bytes);
    let id = hex::encode(&bytes);
    // println!("{}", id);
    let mut conn = Connection::new(socket, addr, ArcStr::new(), ArcStr::from(id));
    conn.write(&format!("{}\n", config.server_version)).await?;


    loop {
        let data = conn.read_to_string().await?;

        if data.len() == 0 {
            continue; // stream reached EOF
        }

        // println!("{}", data);
        let data_split = data.trim().split(",").collect::<Vec<&str>>();

        if addr.to_string().is_empty() || data_split.len() > 6
        // no clue what '!conn.remoteAddr' is supposed to do
        {
            info!("addr: {} len: {}, data: {data}", addr.to_string(), data_split.len());
            conn.write("BAD,Incorrect data\n").await?;
            return conn.shutdown().await;
        }

        // check for bans
        {
            // block so we lose the lock on the mutex
            let mut ban = bans.lock().await;
            // let ip = conn.addr.ip().to_string();
            if ban.banned_ips.contains(&conn.ip) && conn.ip != "127.0.0.1" {
                conn.write("BAD,User banned\n").await?;
                return conn.shutdown().await;
            }
            if data_split.len() >= 2 {
                let username = data_split[1];
                if ban.banned_usernames.contains(&username.to_string()) && conn.ip != "127.0.0.1" {
                    if !ban.banned_ips.contains(&conn.ip) {
                        ban_ip(&conn.ip).await?;
                        ban.banned_ips.push(conn.ip.clone());
                        ban.save_bans().expect("failed to save config bans.json");
                    }

                    conn.write("BAD,User banned\n").await?;
                    return conn.shutdown().await;
                }
            }
        }

        if data_split[0] == "JOB" {
            if data_split.len() == 1 {
                conn.write("BAD,No username specified\n").await?;
                return conn.shutdown().await;
            } else {
                if conn.username.is_empty() {
                    conn.username = ArcStr::from(data_split[1]);
                } else if conn.username != ArcStr::from(data_split[1]) {
                    return conn.shutdown().await;
                }
                match mining::mining::handle(
                    &mut conn,
                    data_split,
                    pool_rewards,
                    config,
                    kolka,
                    false,
                )
                .await
                {
                    Ok(_) => {
                        handle_close(&conn);
                    }
                    Err(err) => {
                        if !err.is::<ConnectionClosed>() {
                            if !err.to_string().contains("Connection reset by peer")
                            {
                                error!("Connection closed: {:?}", err);
                            }
                        }
                        handle_close(&conn);
                        // mining::mining::MINER_STATS.remove(&conn.id.to_string());
                        // mining::mining::WORKERS
                        //     .entry(conn.ip.clone())
                        //     .and_modify(|w| *w -= 1f32);
                        // let opt = mining::mining::WORKERS.get(&conn.ip);
                        // if opt.is_some() && *opt.unwrap().value() <= 0f32 {
                        //     mining::mining::WORKERS.remove(&conn.ip);
                        // }
                        //
                        // mining::mining::USR_WORKERS
                        //     .entry(conn.username.to_string())
                        //     .and_modify(|w| *w -= 1f32);
                        // let opt = mining::mining::USR_WORKERS.get(&conn.username.to_string());
                        // if opt.is_some() && *opt.unwrap().value() <= 0f32 {
                        //     mining::mining::USR_WORKERS.remove(&conn.username.to_string());
                        // }
                    }
                };
                break;
            }
        } else if data_split[0] == "MOTD" {
            conn.write(&config.motd).await?;
        }
    }

    //        if (data[0] === "JOB") {
    //             if (!data[1]) {
    //                 conn.write("BAD,No username specified\n");
    //                 return conn.destroy();
    //             } else {
    //                 if (!conn.username) conn.username = data[1];
    //                 else if (conn.username != data[1]) {
    //                     return conn.destroy();
    //                 }
    //                 mining.miningHandler(conn, data, mainListener, false);
    //             }
    //         } else if (data[0] === "MOTD") {
    //             conn.write(motd);
    //         }

    Ok(())
}

fn handle_close(conn: &Connection) {
    mining::mining::MINER_STATS.remove(&conn.id.to_string());
    
    mining::mining::WORKERS
        .entry(conn.ip.clone())
        .and_modify(|w| *w -= 1f32);
    let opt = mining::mining::WORKERS.get(&conn.ip);
    if opt.is_some() && *opt.unwrap().value() <= 0f32 {
        mining::mining::WORKERS.remove(&conn.ip);
    }

    mining::mining::USR_WORKERS
        .entry(conn.username.to_string())
        .and_modify(|w| *w -= 1f32);
    let opt = mining::mining::USR_WORKERS.get(&conn.username.to_string());
    if opt.is_some() && *opt.unwrap().value() <= 0f32 {
        mining::mining::USR_WORKERS.remove(&conn.username.to_string());
    }
}
