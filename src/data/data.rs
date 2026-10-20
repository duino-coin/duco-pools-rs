use crate::mining::stats::MinerStat;
use compact_str::{CompactString, ToCompactString};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashSet;
use std::error::Error;
use std::future::Future;
use std::net::{Shutdown, SocketAddr};
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{Instant, Sleep};

const TIMEOUT: Duration = Duration::from_secs(90); // it's 90s in the js pools
const IDLE_TIMEOUT: Duration = Duration::from_millis(10);
const BANS_PATH: &str = "./config/bans.json";
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
    // reused timeout timer for wait_for_data(); may be set to an earlier read's deadline, but that doesn't matter
    read_timer: Pin<Box<Sleep>>,
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
    pub fn new(socket: TcpStream, addr: SocketAddr, username: CompactString, id: ConnId) -> Self {
        Connection {
            stream: BufReader::with_capacity(256, socket),
            // never later than the deadline of any read on this connection
            read_timer: Box::pin(tokio::time::sleep(TIMEOUT)),
            id,
            miner_stat: MinerStat::new(addr.ip().to_compact_string(), username),
        }
    }

    pub async fn write(&mut self, content: &str) -> Result<(), anyhow::Error> {
        // first try to immediately write to the stream, so we can save a timeout timer
        // should the stream not be ready (for whatever reaason) it returns a WouldBlock and we fall back to the timeout
        let written = match self.stream.get_ref().try_write(content.as_bytes()) {
            Ok(written) => written,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => 0,
            Err(err) => return Err(err.into()),
        };
        if written < content.len() {
            tokio::time::timeout(
                TIMEOUT,
                self.stream.write_all(&content.as_bytes()[written..]),
            )
            .await??;
        }
        Ok(())
    }

    // GPT 5.3 + Opus 5.5
    // `impl Future` is a workaround to a known rustc bug, where arguments are artificially bloated and use up more memory than needed
    // -> see comment below about async fn
    pub fn read_to_string<'a>(
        &'a mut self,
        output: &'a mut String,
        waiting_since: std::time::Instant,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + 'a {
        // the first read waits for TIMEOUT, any further reads (partial lines), wait for IDLE_TIMEOUT
        let deadline = Instant::from_std(waiting_since) + TIMEOUT;
        // not an async fn: that would keep a second copy of `waiting_since` in the future
        async move {
            const MAX_FRAME_BYTES: usize = 1024;

            // Reuse the String's allocation without copying
            let mut bytes = std::mem::take(output).into_bytes();
            bytes.clear();

            output.clear();

            let mut first_read = true;

            loop {
                let received = if first_read {
                    self.wait_for_data(deadline).await?;
                    // data or EOF is there now, so this doesn't wait
                    self.stream.fill_buf().await?
                } else {
                    let step_deadline = deadline.min(Instant::now() + IDLE_TIMEOUT);
                    match tokio::time::timeout_at(step_deadline, self.stream.fill_buf()).await {
                        Ok(Ok(received)) => received,
                        Ok(Err(err)) => return Err(anyhow::Error::from(err)),
                        // the overall deadline passed
                        Err(elapsed) if step_deadline == deadline => return Err(elapsed.into()),
                        Err(_) => break,
                    }
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
            *output = String::from_utf8(bytes)?;

            Ok(())
        }
    }

    /// waits until the stream has data, eof, an error or until deadline passes
    ///
    /// saves constantly adding and removing a timer from tokio's timer wheel
    /// `read_timer` can lag behind, but early fires are taken care of
    /// optimization proposed by Opus 5.5
    async fn wait_for_data(&mut self, deadline: Instant) -> Result<(), anyhow::Error> {
        loop {
            tokio::select! {
                // poll based on order (first stream, then timer)
                biased;

                filled = self.stream.fill_buf() => {
                    return filled.map(|_| ()).map_err(anyhow::Error::from);
                }

                () = self.read_timer.as_mut() => {
                    if self.read_timer.deadline() >= deadline {
                        // actual timeout from this write
                        // because `Elapsed` has no public constructor, we hack one together with an already elapsed timeout_at..
                        let elapsed = tokio::time::timeout_at(deadline, std::future::pending::<()>())
                            .await
                            .unwrap_err();
                        return Err(elapsed.into());
                    }
                    // it was not our timeout, move it to the correct one
                    self.read_timer.as_mut().reset(deadline);
                }
            }
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

    // runs once when BANS is first locked, so reading and creating the file may block
    pub fn load_bans() -> Result<Self, Box<dyn Error>> {
        if std::fs::exists(BANS_PATH)? {
            let file_content = std::fs::read_to_string(BANS_PATH)?;
            let bans: Bans =
                serde_json::from_str::<Bans>(&file_content).unwrap_or_else(|_| Bans::new());
            return Ok(bans);
        }
        let bans = Bans::new();
        std::fs::write(BANS_PATH, serde_json::to_string(&bans)?)?;
        Ok(bans)
    }

    // called while holding the BANS lock on the runtime, so the write must not block a worker thread
    pub async fn save_bans(&self) -> Result<(), Box<dyn Error>> {
        tokio::fs::write(BANS_PATH, serde_json::to_string(&self)?).await?;

        Ok(())
    }
}

impl Default for Bans {
    fn default() -> Self {
        Bans::new()
    }
}
