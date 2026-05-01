use std::cmp::{Ordering};
use crate::data::data::Connection;
use crate::{mining, BANS, CONFIG};
use log::{debug, warn};
use std::process::Command;
use crate::mining::mining::USR_WORKERS_ALLOC;

//const CMD: &str = "csf -td ${ip}"; //csf
//const CMD: &str = "iptables -A INPUT -s ${ip} -j DROP"; //iptables
//const CMD: &str = "sudo ufw deny from ${ip} to any"; //ufw
// const CMD: &str = ""; //none
async fn ban_ip(ip: &str) -> Result<(), anyhow::Error> {
    let mut command = CONFIG.get().unwrap().firewall_cmd.clone();
    if !command.is_empty() {
        command = command.replace("${ip}", ip);
        let cmd_split = command.split(" ").collect::<Vec<&str>>();
        let cmd = cmd_split[0].trim();
        Command::new(cmd).args(&cmd_split[1..]).spawn()?;
        warn!("Banned {ip}");
    }
    Ok(())
}

pub async fn handle_connection(mut conn: &mut Connection) -> Result<(), anyhow::Error> {
    let config = CONFIG.get().unwrap();
    conn.write(&format!("{}\n", config.server_version)).await?;

    // this loop only handles one JOB requests, however, it is necessary so the user can first request the MOTD
    loop {
        let data = conn.read_to_string().await?;
        let data_split = data.trim().split(",").collect::<Box<[&str]>>();

        if conn.ip.is_empty() || data_split.len() > 6
        // no clue what '!conn.remoteAddr' is supposed to do
        {
            conn.write("BAD,Incorrect data\n").await?;
            return Ok(());
        }

        // check for bans
        // block so we lose the lock on the mutex
        {
            let mut ban = BANS.lock().await;
            if ban.banned_ips.contains(&conn.ip) && conn.ip != "127.0.0.1" {
                conn.write("BAD,User banned\n").await?;
                return Ok(());
            }
            if data_split.len() >= 2 {
                let username = data_split[1];
                if ban.banned_usernames.contains(&username.to_string()) && conn.ip != "127.0.0.1" {
                    if !ban.banned_ips.contains(&conn.ip) {
                        ban_ip(&conn.ip).await?;
                        // ban.banned_ips.push(conn.ip.clone());
                        ban.banned_ips.insert(conn.ip.clone());
                        ban.save_bans().expect("failed to save config bans.json");
                    }

                    conn.write("BAD,User banned\n").await?;
                    return Ok(());
                }
            }
        }

        if data_split[0] == "JOB" {
            if data_split.len() == 1 {
                conn.write("BAD,No username specified\n").await?;
                return Ok(());
            } else {
                if conn.username.is_empty() {
                    conn.username = data_split[1].to_string();
                } else if conn.username != data_split[1] {
                    return Ok(());
                }
                mining::mining::handle(&mut conn, data_split, config, false).await?;
                break;
            }
        } else if data_split[0] == "MOTD" {
            conn.write(&config.motd).await?;
        }
    }

    Ok(())
}

pub fn handle_close(conn: &mut Connection) {
    debug!("{} Handling close", conn.id);
    mining::mining::MINER_STATS.remove(&conn.id);

    let should_remove = {
        let mut alloc = match USR_WORKERS_ALLOC.get_mut(&conn.username) {
            Some(a) => a,
            None => return,
        };
        alloc.release(
            conn.miner_stat.amount_of_workers,
            conn.miner_stat.is_req_diff_esp32,
        );
        alloc.is_empty()
    };
    // drop the RefMut before removing, so we don't deadlock

    if should_remove {
        USR_WORKERS_ALLOC.remove(&conn.username);
        debug!("Removed {}", conn.username);
    }

    debug!("Handling after-close");
}

