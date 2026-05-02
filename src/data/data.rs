use std::collections::HashSet;
use crate::mining;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use compact_str::CompactString;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use crate::mining::stats::MinerStat;

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
    // pub socket: TcpStream,
    reader: BufReader<OwnedReadHalf>, // persistent — buffer survives between reads
    writer: OwnedWriteHalf,
    // pub addr: SocketAddr,
    // pub ip: String,
    // pub username: String,
    pub id: Box<str>,
    pub miner_stat: mining::stats::MinerStat,
    // Reusable buffer for reads — avoids re-allocating on every read_to_string call
    read_buf: Vec<u8>,
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
            read_buf: Vec::with_capacity(128),
        }
    }

    pub async fn write(&mut self, content: &str) -> Result<(), anyhow::Error> {
        // tokio::time::timeout(TIMEOUT, async {
        //     self.socket.write_all(content.as_bytes()).await?;
        //     self.socket.flush().await?;
        //     Ok::<(), anyhow::Error>(())
        // }).await??;
        tokio::time::timeout(TIMEOUT, async {
            self.writer.write_all(content.as_bytes()).await?;
            self.writer.flush().await?;
            Ok::<(), anyhow::Error>(())
        }).await??;
        Ok(())
    }

    // written by Opus 4.6
    pub async fn read_to_string(&mut self) -> Result<Box<str>, anyhow::Error> {
        self.read_buf.clear();

        // Wrap the entire read in the 90s overall timeout
        tokio::time::timeout(TIMEOUT, async {
            let mut first_read = true;

            loop {
                // First read waits indefinitely (up to overall TIMEOUT) -> we wait until we receive the first bit of data
                // after first bit of data we have a 10ms idle timeout, in case the client doesn't send a '\n'
                let step_timeout = if first_read {
                    Duration::from_secs(u64::MAX) // Effectively no inner timeout (only runs up until TIMEOUT)
                } else {
                    Duration::from_millis(10)
                };

                match tokio::time::timeout(step_timeout, self.reader.fill_buf()).await {
                    Ok(Ok(received)) => {
                        if received.is_empty() {
                            if self.read_buf.is_empty() {
                                return Err(ConnectionClosed.into());
                            } else {
                                // we received data during this read_to_string() operation, but have run out of new data
                                break; // EOF, return what we have
                            }
                        }

                        if let Some(pos) = received.iter().position(|&b| b == b'\n') {
                            // has \n -> append data up until newline
                            self.read_buf.extend_from_slice(&received[..=pos]);
                            self.reader.consume(pos + 1);
                            break;
                        } else {
                            // no '\n' -> append and continue loop
                            let len = received.len();
                            self.read_buf.extend_from_slice(received);
                            self.reader.consume(len);
                        }
                        first_read = false;
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => {
                        // 10ms idle timeout hit (u64::MAX will never be hit, outer timeout hits first)
                        // no \n was sent, so we just take what we received up until now
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }).await??;

        // Strip trailing newlines/CR in-place
        while matches!(self.read_buf.last(), Some(&b'\n') | Some(&b'\r')) {
            self.read_buf.pop();
        }

        if self.read_buf.is_empty() {
            return Err(ConnectionClosed.into());
        }

        // Validate UTF-8 on the reused buffer and copy into Box<str>
        let s = std::str::from_utf8(&self.read_buf).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 data")
        })?;

        Ok(s.into())
    }


    // pub async fn read_to_string(&mut self) -> Result<Box<str>, anyhow::Error> {
    //     self.read_buf.clear();
    //     {
    //         // 256 bytes should be plenty and we avoid allocating 8KB everytime we read
    //         let mut reader = BufReader::with_capacity(256, &mut self.socket);
    //         let received = tokio::time::timeout(TIMEOUT, reader.fill_buf())
    //             .await??;
    //
    //         if received.is_empty() {
    //             return Err(ConnectionClosed.into());
    //         }
    //         let len = received.len();
    //         self.read_buf.extend_from_slice(received);
    //         reader.consume(len);
    //     }
    //
    //     // Strip trailing newlines in-place (protocol uses \n as delimiter)
    //     while self.read_buf.last() == Some(&b'\n') {
    //         self.read_buf.pop();
    //     }
    //
    //     if self.read_buf.is_empty() {
    //         return Err(ConnectionClosed.into());
    //     }
    //
    //     // Validate UTF-8 on the reused buffer and copy into Box<str>
    //     let s = std::str::from_utf8(&self.read_buf)
    //         .map_err(|_| {
    //             std::io::Error::new(
    //                 std::io::ErrorKind::InvalidData,
    //                 "Invalid UTF-8 data",
    //             )
    //         })?;
    //
    //     Ok(s.into())
    // }
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
