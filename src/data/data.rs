use crate::mining::stats::MinerStat;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

const TIMEOUT_IN_SECS: u64 = 90;
const TIMEOUT: Duration = Duration::from_secs(TIMEOUT_IN_SECS); // it's 90s in the js pools
const INNER_LONG_TIMEOUT: Duration = Duration::from_secs(TIMEOUT_IN_SECS + 5);
const IDLE_TIMEOUT: Duration = Duration::from_millis(10);
#[derive(Debug, Clone)]
pub struct ConnectionClosed;

impl std::fmt::Display for ConnectionClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Connection closed")
    }
}

impl Error for ConnectionClosed {}

#[derive(Debug)]
pub struct Connection {
    // pub socket: TcpStream,
    reader: BufReader<OwnedReadHalf>, // persistent — buffer survives between reads
    writer: OwnedWriteHalf,
    // pub addr: SocketAddr,
    // pub ip: String,
    // pub username: String,
    pub id: Box<str>,
    pub miner_stat: MinerStat,
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Guaranteed cleanup on connection drop (normal or panic)
        crate::handler::handler::handle_close(self);
    }
}

impl Connection {
    pub fn new(socket: TcpStream, addr: SocketAddr, username: String, id: Box<str>) -> Self {
        let (read_half, write_half) = socket.into_split();
        Connection {
            // socket,
            // addr,
            // ip: addr.ip().to_string(),
            // username,
            reader: BufReader::with_capacity(256, read_half),
            writer: write_half,
            id,
            miner_stat: MinerStat::new(addr.ip().to_string(), username),
        }
    }

    pub async fn write(&mut self, content: &str) -> Result<(), anyhow::Error> {
        tokio::time::timeout(TIMEOUT, async {
            self.writer.write_all(content.as_bytes()).await?;
            Ok::<(), anyhow::Error>(())
        }).await??;
        Ok(())
    }

    // GPT 5.3
    pub async fn read_to_string(
        &mut self,
        output: &mut String,
    ) -> Result<(), anyhow::Error> {
        const MAX_FRAME_BYTES: usize = 1024;

        // Reuse the String's allocation without copying
        let mut bytes = std::mem::take(output).into_bytes();
        bytes.clear();

        output.clear();

        tokio::time::timeout(TIMEOUT, async {
            let mut first_read = true;

            loop {
                let step_timeout = if first_read {
                    INNER_LONG_TIMEOUT
                } else {
                    IDLE_TIMEOUT
                };
                let received =
                    match tokio::time::timeout(step_timeout, self.reader.fill_buf()).await {
                        Ok(Ok(received)) => received,
                        Ok(Err(err)) => return Err(anyhow::Error::from(err)),
                        Err(_) => break,
                    };

                if received.is_empty() {
                    break;
                }

                let newline = memchr::memchr(b'\n', received);
                let consumed = newline.map_or(received.len(), |pos| pos + 1);

                if consumed > MAX_FRAME_BYTES - bytes.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Message too long",
                    )
                        .into());
                }

                bytes.extend_from_slice(&received[..consumed]);
                self.reader.consume(consumed);

                if newline.is_some() {
                    break;
                }

                first_read = false;
            }

            Ok::<(), anyhow::Error>(())
        })
            .await??;

        if bytes.last() == Some(&b'\n') {
            bytes.pop();
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
        }

        if bytes.is_empty() {
            return Err(ConnectionClosed.into());
        }

        // Validate once; reuse the byte buffer's allocation.
        *output = String::from_utf8(bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid UTF-8 data",
            )
        })?;

        Ok(())
    }

}

#[derive(Deserialize, Serialize, Debug)]
#[allow(dead_code)] // only unused in debug/bench/local
pub struct LoginInfos {
    host: String,
    port: u32,
    version: String,
    identifier: String,
    name: String,
}

impl LoginInfos {
    #[allow(dead_code)] // only unused in debug/bench/local
    pub fn new(host: &str, port: u32, version: &str, identifier: &str, name: &str) -> Self {
        LoginInfos {
            host: host.to_string(),
            port,
            version: version.to_string(),
            identifier: identifier.to_string(),
            name: name.to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Bans {
    #[serde(rename = "bannedUsernames")]
    pub banned_usernames: HashSet<String>,
    #[serde(rename = "bannedIPs")]
    pub banned_ips: HashSet<CompactString>,
}

impl Bans {
    pub fn new() -> Self {
        Bans {
            banned_usernames: HashSet::new(),
            banned_ips: HashSet::new(),
        }
    }

    pub fn load_bans() -> Result<Self, Box<dyn Error>> {
        let path = "./config/bans.json";
        if std::fs::exists(path)? {
            let file_content = std::fs::read_to_string(path)?;
            let bans: Bans =
                serde_json::from_str::<Bans>(&file_content).unwrap_or_else(|_| Bans::new());
            return Ok(bans);
        }
        let bans = Bans::new();
        bans.save_bans()?;
        Ok(bans)
    }

    pub fn save_bans(&self) -> Result<(), Box<dyn Error>> {
        std::fs::write("./config/bans.json", serde_json::to_string(&self)?)?;

        Ok(())
    }
}
