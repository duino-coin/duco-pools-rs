use crate::mining::stats::MinerStat;
use compact_str::{CompactString, ToCompactString};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashSet;
use std::error::Error;
use std::net::{Shutdown, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(90); // it's 90s in the js pools
const IDLE_TIMEOUT: Duration = Duration::from_millis(10);
#[derive(Debug, Clone)]
pub struct ConnectionClosed;

impl std::fmt::Display for ConnectionClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Connection closed")
    }
}

impl Error for ConnectionClosed {}

// Connection id, kept as a number and only rendered as 16 lowercase hex chars
// (dashboard/sync keys, logs)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub u64);

impl std::fmt::Display for ConnId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl Serialize for ConnId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut buf = [0u8; 16];
        hex::encode_to_slice(self.0.to_be_bytes(), &mut buf).unwrap();
        // hex output is always valid utf-8
        serializer.serialize_str(std::str::from_utf8(&buf).unwrap())
    }
}

#[derive(Debug)]
pub struct Connection {
    // unsplit: reads and writes are sequential, and BufReader passes writes through
    stream: BufReader<TcpStream>,
    pub id: ConnId,
    pub miner_stat: MinerStat,
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Guaranteed cleanup on connection drop (normal or panic)
        crate::handler::handler::handle_close(self);
        // same as dropping the old OwnedWriteHalf: send FIN before the socket is closed
        let _ = socket2::SockRef::from(self.stream.get_ref()).shutdown(Shutdown::Write);
    }
}

impl Connection {
    pub fn new(
        socket: TcpStream,
        addr: SocketAddr,
        username: CompactString,
        id: ConnId,
    ) -> Self {
        Connection {
            stream: BufReader::with_capacity(256, socket),
            id,
            miner_stat: MinerStat::new(addr.ip().to_compact_string(), username),
        }
    }

    pub async fn write(&mut self, content: &str) -> Result<(), anyhow::Error> {
        tokio::time::timeout(TIMEOUT, async {
            self.stream.write_all(content.as_bytes()).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    // GPT 5.3
    pub async fn read_to_string(&mut self, output: &mut String) -> Result<(), anyhow::Error> {
        const MAX_FRAME_BYTES: usize = 1024;

        // Reuse the String's allocation without copying
        let mut bytes = std::mem::take(output).into_bytes();
        bytes.clear();

        output.clear();

        // One timer per wait: the first read waits for the whole deadline, later reads
        // (rest of a partial line) wait IDLE_TIMEOUT but never past the deadline
        let deadline = Instant::now() + TIMEOUT;
        let mut first_read = true;

        loop {
            let step_deadline = if first_read {
                deadline
            } else {
                deadline.min(Instant::now() + IDLE_TIMEOUT)
            };
            let received =
                match tokio::time::timeout_at(step_deadline, self.stream.fill_buf()).await {
                    Ok(Ok(received)) => received,
                    Ok(Err(err)) => return Err(anyhow::Error::from(err)),
                    // the overall deadline passed
                    Err(elapsed) if step_deadline == deadline => return Err(elapsed.into()),
                    // idle gap within a partial line
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
            self.stream.consume(consumed);

            if newline.is_some() {
                break;
            }

            first_read = false;
        }

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
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 data")
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
    pub banned_usernames: HashSet<CompactString>,
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
