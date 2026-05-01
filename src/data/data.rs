use std::collections::HashSet;
use crate::mining;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

static TIMEOUT: Duration = Duration::from_secs(90);// it's 90s in the js pools

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
    pub socket: TcpStream,
    // pub addr: SocketAddr,
    pub ip: String,
    pub username: String,
    pub id: Box<str>,
    pub miner_stat: mining::stats::MinerStat,
    // Reusable buffer for reads — avoids re-allocating on every read_to_string call
    read_buf: Vec<u8>,
}

impl Connection {
    pub fn new(socket: TcpStream, addr: SocketAddr, username: String, id: Box<str>) -> Self {
        Connection {
            socket,
            // addr,
            ip: addr.ip().to_string(),
            username,
            id,
            miner_stat: Default::default(),
            read_buf: Vec::with_capacity(128),
        }
    }

    pub async fn write(&mut self, content: &str) -> Result<(), anyhow::Error> {
        tokio::time::timeout(TIMEOUT, async {
            self.socket.write_all(content.as_bytes()).await?;
            self.socket.flush().await?;
            Ok::<(), anyhow::Error>(())
        }).await??;
        Ok(())
    }

    pub async fn read_to_string(&mut self) -> Result<Box<str>, anyhow::Error> {
        self.read_buf.clear();
        {
            // 256 bytes should be plenty and we avoid allocating 8KB everytime we read
            let mut reader = BufReader::with_capacity(256, &mut self.socket);
            let received = tokio::time::timeout(TIMEOUT, reader.fill_buf())
                .await??;
        
            if received.is_empty() {
                return Err(ConnectionClosed.into());
            }
            let len = received.len();
            self.read_buf.extend_from_slice(received);
            reader.consume(len);
        }

        // Strip trailing newlines in-place (protocol uses \n as delimiter)
        while self.read_buf.last() == Some(&b'\n') {
            self.read_buf.pop();
        }

        if self.read_buf.is_empty() {
            return Err(ConnectionClosed.into());
        }

        // Validate UTF-8 on the reused buffer and copy into Box<str>
        let s = std::str::from_utf8(&self.read_buf)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 data",
                )
            })?;

        Ok(s.into())
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
    pub banned_ips: HashSet<String>,
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
