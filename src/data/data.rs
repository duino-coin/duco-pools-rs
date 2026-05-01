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
    pub id: String,
    pub miner_stat: mining::stats::MinerStat,
}

impl Connection {
    pub fn new(socket: TcpStream, addr: SocketAddr, username: String, id: String) -> Self {
        Connection {
            socket,
            // addr,
            ip: addr.ip().to_string(),
            username,
            id,
            miner_stat: Default::default(),
        }
    }

    pub async fn write(&mut self, content: &str) -> Result<(), anyhow::Error> {
        tokio::time::timeout(TIMEOUT, self.socket.write_all(content.as_bytes())).await??;
        // self.socket.write_all(content.as_bytes()).await?;
        tokio::time::timeout(TIMEOUT, self.socket.flush()).await??;
        Ok(())
    }

    pub async fn read(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        let mut reader = BufReader::new(&mut self.socket);
        let received: Vec<u8> =
            tokio::time::timeout(TIMEOUT, reader.fill_buf())
                .await??
                .to_vec();

        if received.is_empty() {
            return Err(ConnectionClosed.into());
        }
        reader.consume(received.len());

        Ok(received)
    }

    pub async fn read_to_string(&mut self) -> Result<Box<str>, anyhow::Error> {
        let received = self.read().await?;
        if received.is_empty() {
            return Err(ConnectionClosed.into());
        }
        let data = String::from_utf8(received)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 data ".to_string() + self.username.as_str(),
                )
            })
            .map(|x| x.replace("\n", ""))?;

        Ok(data.into_boxed_str())
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
