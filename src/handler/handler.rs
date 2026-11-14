use crate::data::data::Connection;
use crate::mining::mining::USR_WORKERS_ALLOC;
use crate::{mining, BANS, CONFIG};
use compact_str::CompactString;
use log::{debug, error, warn};
use std::sync::OnceLock;
use std::time::Instant;
use tokio::process::Command;

const CARGO_VERSION: &str = env!("CARGO_PKG_VERSION");
// longest username accepted; it is a map key and part of every sync
const MAX_USERNAME_BYTES: usize = 64;

// "<server_version>\n", built once instead of on every connection
static GREETING: OnceLock<String> = OnceLock::new();

//const CMD: &str = "csf -td ${ip}"; //csf
//const CMD: &str = "iptables -A INPUT -s ${ip} -j DROP"; //iptables
//const CMD: &str = "sudo ufw deny from ${ip} to any"; //ufw
async fn ban_ip(ip: &str) -> Result<(), anyhow::Error> {
    let firewall_cmd = &CONFIG.get().unwrap().firewall_cmd;
    if !firewall_cmd.is_empty() {
        let command = firewall_cmd.replace("${ip}", ip);
        // split always yields the program
        let mut parts = command.split(' ');
        let cmd = parts.next().unwrap().trim();
        let mut child = Command::new(cmd).args(parts).spawn()?;
        // reap it, otherwise every ban leaves a zombie process behind until the pool exits
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        warn!("Banned {ip}");
    }
    Ok(())
}

pub async fn handle_connection(conn: &mut Connection) -> Result<(), anyhow::Error> {
    let config = CONFIG.get().unwrap();
    let greeting = GREETING.get_or_init(|| format!("{}\n", config.server_version));
    conn.write(greeting).await?;
    let mut data = String::new();

    // this loop only handles one JOB requests, however, it is necessary so the user can first request the MOTD
    loop {
        conn.read_to_string(&mut data, Instant::now()).await?;
        // the split fields are scoped so they aren't kept in the future while mining,
        // mining::handle scans the line again itself
        let is_job = {
            let mut data_split = [""; 8];
            let data_len = mining::mining::scan_fields(data.trim(), &mut data_split);

            if conn.miner_stat.ip.is_empty() || data_len > 6
            // no clue what '!conn.remoteAddr' is supposed to do
            {
                conn.write("BAD,Incorrect data\n").await?;
                return Ok(());
            }

            // check for bans
            // only decide under the lock, the firewall call and the write happen after it's
            // released, so a slow client can't stall every other connection's ban check
            let (banned, newly_banned_ip) = {
                let mut ban = BANS.lock().await;
                if conn.miner_stat.ip == "127.0.0.1" {
                    (false, false)
                } else if ban.banned_ips.contains(&conn.miner_stat.ip) {
                    (true, false)
                } else if data_len >= 2 && ban.banned_usernames.contains(data_split[1]) {
                    // we don't want to ban the wss-bridge
                    if conn.miner_stat.ip != config.server_endpoint {
                        ban.banned_ips.insert(conn.miner_stat.ip.clone());
                    }
                    // the ban still applies for this run if the file can't be written
                    if let Err(err) = ban.save_bans().await {
                        error!("Failed to save bans.json: {err}");
                    }
                    (true, true)
                } else {
                    (false, false)
                }
            };
            if banned {
                if newly_banned_ip {
                    ban_ip(&conn.miner_stat.ip).await?;
                }
                conn.write("BAD,User banned\n").await?;
                return Ok(());
            }

            if data_split[0] == "JOB" {
                // a missing username field is "" as well
                if data_split[1].is_empty() {
                    conn.write("BAD,No username specified\n").await?;
                    return Ok(());
                }
                // control characters would end up in the log
                if data_split[1].len() > MAX_USERNAME_BYTES
                    || data_split[1].bytes().any(|b| b.is_ascii_control())
                {
                    conn.write("BAD,Invalid username\n").await?;
                    return Ok(());
                }

                if conn.miner_stat.username.is_empty() {
                    conn.miner_stat.username = CompactString::from(data_split[1]);
                } else if conn.miner_stat.username != data_split[1] {
                    return Ok(());
                }
                true
            } else {
                if data_split[0] == "MOTD" {
                    conn.write(&config.motd).await?;
                } else if data_split[0] == "DEBG_VERSION" {
                    conn.write(CARGO_VERSION).await?;
                }
                false
            }
        };

        if is_job {
            // hand the line over, its buffer is reused for the rest of the session
            mining::mining::handle(conn, std::mem::take(&mut data), config).await?;
            break;
        }
    }

    Ok(())
}

pub fn handle_close(conn: &mut Connection) {
    debug!("{} Handling close", conn.id);
    mining::mining::MINER_STATS.remove(&conn.id);

    USR_WORKERS_ALLOC.remove_if_mut(&conn.miner_stat.username, |_, alloc| {
        alloc.release(
            conn.miner_stat.worker_slot,
            conn.miner_stat.is_req_diff_esp32,
        );
        alloc.is_empty()
    });

    debug!("Handling after-close");
}
