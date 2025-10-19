use crate::mining;
use arcstr::ArcStr;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct ConnectionClosed;

impl std::fmt::Display for ConnectionClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Connection closed")
    }
}

impl std::error::Error for ConnectionClosed {}

#[derive(Debug)]
pub struct Connection {
    pub socket: TcpStream,
    pub addr: SocketAddr,
    pub ip: String,
    pub username: ArcStr, // has less overhead than a conventional String (8 bytes vs 24 bytes)
    pub id: ArcStr,
    pub miner_stat: mining::stats::MinerStat,
}

impl Connection {
    pub fn new(socket: TcpStream, addr: SocketAddr, username: ArcStr, id: ArcStr) -> Self {
        Connection {
            socket,
            addr,
            ip: addr.ip().to_string(),
            username,
            id,
            miner_stat: Default::default(),
        }
    }

    pub async fn write(&mut self, content: &str) -> Result<(), anyhow::Error> {
        self.socket.write_all(content.as_bytes()).await?;
        self.socket.flush().await?;

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), anyhow::Error> {
        self.socket.split().1.shutdown().await?;
        Ok(())
    }

    pub async fn read(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        let mut reader = BufReader::new(&mut self.socket);
        let received: Vec<u8> = reader.fill_buf().await?.to_vec();
        if received.is_empty() {
            return Err(ConnectionClosed.into());
        }
        reader.consume(received.len());

        Ok(received)
    }

    pub async fn read_to_string(&mut self) -> Result<String, anyhow::Error> {
        let received = self.read().await?;
        if received.is_empty() {
            return Err(ConnectionClosed.into());
        }
        let data = String::from_utf8(received.clone()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid UTF-8 data".to_string(),
            )
        })?;

        Ok(data)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct LoginInfos {
    host: String,
    port: u32,
    version: String,
    identifier: String,
    name: String,
}

impl LoginInfos {
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
    pub banned_usernames: Vec<String>,
    #[serde(rename = "bannedIPs")]
    pub banned_ips: Vec<String>,
}

impl Bans {
    pub fn new() -> Self {
        Bans {
            banned_usernames: vec![],
            banned_ips: vec![],
        }
    }

    pub fn load_bans() -> Result<Self, Box<dyn Error>> {
        let path = "./config/bans.json";
        if std::fs::exists(path)? {
            let file_content = std::fs::read_to_string(path)?;
            let bans: Bans = match serde_json::from_str::<Bans>(&file_content) {
                Ok(bans) => bans,
                Err(_) => Bans::new(),
            };
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
// const loginInfos = {
//     host: ip,
//     port: port,
//     version: poolVersion,
//     identifier: poolID,
//     name: poolName,
// };
