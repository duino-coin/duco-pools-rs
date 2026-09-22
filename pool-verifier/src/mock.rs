//! Loopback replacements for the pool's HTTP sync service and master TCP server.
//!
//! The mock records requests as test evidence and supports queued responses,
//! transport faults, delays, and held uploads. A small TCP front proxy supplies
//! real disconnect/timeout behavior that an HTTP status code cannot emulate.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    task::JoinHandle,
};
use warp::{Filter, Reply};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Part {
    pub filename: String,
    pub json: Value,
}
#[derive(Clone, Debug, Serialize)]
pub struct Upload {
    pub query: BTreeMap<String, String>,
    pub parts: BTreeMap<String, Part>,
    pub error: Option<String>,
    pub completed: bool,
    pub status: u16,
}

pub struct Shared {
    // Tests mutate this state between requests. Short std mutex sections are used
    // deliberately; no guard is held across an async await.
    pub uploads: Mutex<Vec<Upload>>,
    pub gets: Mutex<Vec<String>>,
    pub master: Mutex<Vec<String>>,
    pub status: AtomicU16,
    pub hold: AtomicBool,
    pub gate: Semaphore,
    pub rewards: Mutex<Value>,
    pub bans: Mutex<Value>,
    pub responses: Mutex<BTreeMap<String, VecDeque<Response>>>,
    pub faults: Mutex<BTreeMap<String, VecDeque<Fault>>>,
    pub fault_log: Mutex<Vec<String>>,
    pub master_reply: Mutex<MasterReply>,
}

#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub body: String,
}
#[derive(Clone, Debug)]
pub enum Fault {
    Disconnect,
    Delay(u64),
}
#[derive(Clone, Debug)]
pub struct MasterReply {
    pub greeting: Vec<u8>,
    pub reply: Vec<u8>,
    pub delay_ms: u64,
}
impl Default for MasterReply {
    fn default() -> Self {
        Self {
            greeting: b"3.0\n".to_vec(),
            reply: b"LoginOK".to_vec(),
            delay_ms: 0,
        }
    }
}

pub struct Mock {
    pub shared: Arc<Shared>,
    pub http: String,
    pub master_port: u16,
    http_task: JoinHandle<()>,
    master_task: JoinHandle<()>,
    proxy_task: JoinHandle<()>,
}

pub fn unused_port() -> Result<u16> {
    // The listener is released immediately, so this is inherently advisory. Pool
    // startup detects a bind race and reports it as setup failure rather than
    // allowing a scenario to target an unrelated service.
    Ok(std::net::TcpListener::bind(("127.0.0.1", 0))?
        .local_addr()?
        .port())
}

pub fn rewards(blocks: bool, transition: bool) -> Value {
    // Distinct sequential difficulties make tier selection observable. Optional
    // flags enable block and adaptive-transition branches only when a case needs
    // them, keeping the default fixture deterministic.
    let mut result = serde_json::Map::new();
    for (name, difficulty) in [
        ("AVR", 1),
        ("MEGA", 2),
        ("ARM", 3),
        ("DUE", 4),
        ("ESP8266", 5),
        ("ESP8266H", 6),
        ("ESP8266N", 7),
        ("ESP8266NH", 8),
        ("ESP32", 9),
        ("ESP32S", 10),
        ("LOW", 11),
        ("MEDIUM", 12),
        ("NET", 13),
        ("EXTREME", 14),
    ] {
        result.insert(
            name.into(),
            json!({
                "difficulty": difficulty, "reward": 1000,
                "min_hashrate": if transition && name == "MEGA" { 200 } else { 1 },
                "max_hashrate": if transition && name == "AVR" { 100 } else { 1_000_000_000u64 },
                "kolka_decrease_perc": 90, "expected_sharetime": 1,
                "block_chance": if blocks { 1u32 } else { u32::MAX },
            }),
        );
    }
    Value::Object(result)
}

fn parse_parts(content_type: &str, body: &[u8]) -> Result<BTreeMap<String, Part>> {
    // The verifier needs semantic JSON and filenames from the pool's multipart
    // upload. This intentionally small parser rejects ambiguous duplicate fields
    // instead of trying to be a general-purpose multipart implementation.
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|s| s.strip_prefix("boundary="))
        .context("Missing multipart boundary")?
        .trim_matches('"');
    anyhow::ensure!(!boundary.is_empty(), "Empty multipart boundary");
    let text = std::str::from_utf8(body)?;
    let mut parts = BTreeMap::new();
    for piece in text.split(&format!("--{boundary}")).skip(1) {
        if piece.starts_with("--") {
            break;
        }
        let piece = piece
            .strip_prefix("\r\n")
            .context("Invalid multipart delimiter")?;
        let (headers, payload) = piece
            .split_once("\r\n\r\n")
            .context("Missing part headers")?;
        let disposition = headers
            .lines()
            .find(|s| s.to_ascii_lowercase().starts_with("content-disposition:"))
            .context("Missing disposition")?;
        let attr = |key: &str| -> Result<String> {
            disposition
                .split(';')
                .map(str::trim)
                .find_map(|s| {
                    s.strip_prefix(&format!("{key}=\""))
                        .and_then(|v| v.strip_suffix('"'))
                })
                .map(str::to_owned)
                .with_context(|| format!("Missing {key}"))
        };
        let name = attr("name")?;
        anyhow::ensure!(
            !parts.contains_key(&name),
            "Duplicate multipart field {name}"
        );
        parts.insert(
            name,
            Part {
                filename: attr("filename")?,
                json: serde_json::from_str(
                    payload
                        .strip_suffix("\r\n")
                        .context("Missing part terminator")?,
                )?,
            },
        );
    }
    anyhow::ensure!(
        parts.len() == 2 && parts.contains_key("workers") && parts.contains_key("rewards"),
        "Expected exactly workers and rewards parts"
    );
    Ok(parts)
}

impl Mock {
    pub async fn start(blocks: bool, transition: bool) -> Result<Self> {
        // Three owned tasks are created: HTTP application routes, the fake master
        // server, and a front proxy that can cut or delay HTTP transport.
        let shared = Arc::new(Shared {
            uploads: Mutex::new(vec![]),
            gets: Mutex::new(vec![]),
            master: Mutex::new(vec![]),
            status: AtomicU16::new(503),
            hold: AtomicBool::new(false),
            gate: Semaphore::new(0),
            rewards: Mutex::new(rewards(blocks, transition)),
            bans: Mutex::new(json!(["banned-verifier-user"])),
            responses: Mutex::new(BTreeMap::new()),
            faults: Mutex::new(BTreeMap::new()),
            fault_log: Mutex::new(vec![]),
            master_reply: Mutex::new(MasterReply::default()),
        });
        let http_port = unused_port()?;
        let state = shared.clone();
        let routes = warp::path::full()
            .and(warp::method())
            .and(warp::query::<BTreeMap<String, String>>())
            .and(warp::header::optional::<String>("content-type"))
            .and(warp::body::bytes())
            .and_then(
                move |path: warp::path::FullPath,
                      method: warp::http::Method,
                      query: BTreeMap<String, String>,
                      content_type: Option<String>,
                      body: bytes::Bytes| {
                    let state = state.clone();
                    async move {
                        let path = path.as_str();
                        let scripted = state
                            .responses
                            .lock()
                            .unwrap()
                            .get_mut(path)
                            .and_then(VecDeque::pop_front);
                        let mut response = if method == warp::http::Method::GET {
                            state.gets.lock().unwrap().push(path.to_string());
                            if let Some(response) = scripted {
                                warp::reply::with_status(
                                    response.body,
                                    warp::http::StatusCode::from_u16(response.status).unwrap(),
                                )
                                .into_response()
                            } else {
                                match path {
                                    "/poolrewards.json" => {
                                        warp::reply::json(&*state.rewards.lock().unwrap())
                                            .into_response()
                                    }
                                    "/bans.json" => warp::reply::json(&*state.bans.lock().unwrap())
                                        .into_response(),
                                    "/ip" => warp::reply::with_status(
                                        "127.0.0.1",
                                        warp::http::StatusCode::OK,
                                    )
                                    .into_response(),
                                    _ => warp::reply::with_status(
                                        "Unexpected GET",
                                        warp::http::StatusCode::NOT_FOUND,
                                    )
                                    .into_response(),
                                }
                            }
                        } else if method == warp::http::Method::POST && path == "/pool_sync/" {
                            let parsed = parse_parts(content_type.as_deref().unwrap_or(""), &body);
                            let (parts, error) = match parsed {
                                Ok(p) => (p, None),
                                Err(e) => (BTreeMap::new(), Some(format!("{e:#}"))),
                            };
                            let id = {
                                let mut uploads = state.uploads.lock().unwrap();
                                let id = uploads.len();
                                uploads.push(Upload {
                                    query,
                                    parts,
                                    error,
                                    completed: false,
                                    status: 0,
                                });
                                id
                            };
                            if state.hold.load(Ordering::SeqCst) {
                                if let Ok(permit) = state.gate.acquire().await {
                                    permit.forget();
                                }
                            }
                            let status = scripted
                                .as_ref()
                                .map(|r| r.status)
                                .unwrap_or_else(|| state.status.load(Ordering::SeqCst));
                            {
                                let mut uploads = state.uploads.lock().unwrap();
                                uploads[id].completed = true;
                                uploads[id].status = status;
                            }
                            warp::reply::with_status(
                                scripted
                                    .map(|r| r.body)
                                    .unwrap_or_else(|| "mock sync response".into()),
                                warp::http::StatusCode::from_u16(status).unwrap(),
                            )
                            .into_response()
                        } else {
                            warp::reply::with_status(
                                "Unexpected request",
                                warp::http::StatusCode::NOT_FOUND,
                            )
                            .into_response()
                        };
                        // One request per connection makes scripted transport actions deterministic.
                        response.headers_mut().insert(
                            warp::http::header::CONNECTION,
                            warp::http::HeaderValue::from_static("close"),
                        );
                        Ok::<_, Infallible>(response)
                    }
                },
            )
            .boxed();
        let server = warp::serve(routes).bind(([127, 0, 0, 1], http_port)).await;
        let http_task = tokio::spawn(server.run());
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let master_port = listener.local_addr()?.port();
        let state = shared.clone();
        let master_task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let state = state.clone();
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async move {
                    let response = state.master_reply.lock().unwrap().clone();
                    tokio::time::sleep(std::time::Duration::from_millis(response.delay_ms)).await;
                    if response.greeting.is_empty() {
                        return Ok(());
                    }
                    socket.write_all(&response.greeting).await?;
                    let mut message = Vec::new();
                    let mut chunk = [0; 4096];
                    loop {
                        let n = socket.read(&mut chunk).await?;
                        if n == 0 {
                            break;
                        }
                        message.extend_from_slice(&chunk[..n]);
                        let text = String::from_utf8_lossy(&message);
                        if text.starts_with("PoolLogin,")
                            && serde_json::from_str::<Value>(&text[10..]).is_ok()
                        {
                            state.master.lock().unwrap().push(text.to_string());
                            socket.write_all(&response.reply).await?;
                            break;
                        }
                        if text == "PoolLogout,verifier-id" {
                            state.master.lock().unwrap().push(text.to_string());
                            socket.write_all(b"LogoutOK").await?;
                            break;
                        }
                        if message.len() > 4096 {
                            break;
                        }
                    }
                    Ok::<_, std::io::Error>(())
                })
                .await;
            }
        });
        // Front proxy permits real transport failure without replacing pool logic.
        let proxy = TcpListener::bind(("127.0.0.1", 0)).await?;
        let proxy_port = proxy.local_addr()?.port();
        let proxy_state = shared.clone();
        let proxy_task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = proxy.accept() => {
                        let Ok((mut incoming, _)) = accepted else { break };
                        let state = proxy_state.clone();
                        children.spawn(async move {
                            let mut first = vec![0; 8192];
                            let n = incoming.read(&mut first).await?;
                            if n == 0 { return Ok::<(), std::io::Error>(()); }
                            let text = String::from_utf8_lossy(&first[..n]);
                            let path = text.split_whitespace().nth(1).unwrap_or("").split('?').next().unwrap_or("").to_owned();
                            let fault = state.faults.lock().unwrap().get_mut(&path).and_then(VecDeque::pop_front);
                            if let Some(fault) = fault {
                                state.fault_log.lock().unwrap().push(format!("{path}: {fault:?}"));
                                match fault {
                                    Fault::Disconnect => return Ok(()),
                                    Fault::Delay(ms) => tokio::time::sleep(std::time::Duration::from_millis(ms)).await,
                                }
                            }
                            let mut upstream = tokio::net::TcpStream::connect(("127.0.0.1", http_port)).await?;
                            upstream.write_all(&first[..n]).await?;
                            tokio::io::copy_bidirectional(&mut incoming, &mut upstream).await?;
                            Ok(())
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => {}
                }
            }
        });
        Ok(Self {
            shared,
            http: format!("http://127.0.0.1:{proxy_port}"),
            master_port,
            http_task,
            master_task,
            proxy_task,
        })
    }

    pub fn respond(&self, path: &str, status: u16, body: impl Into<String>) {
        // Responses are FIFO per path so a scenario can script failure then retry.
        self.shared
            .responses
            .lock()
            .unwrap()
            .entry(path.into())
            .or_default()
            .push_back(Response {
                status,
                body: body.into(),
            });
    }
    pub fn fault(&self, path: &str, fault: Fault) {
        // Faults are consumed by the proxy before HTTP sees the request.
        self.shared
            .faults
            .lock()
            .unwrap()
            .entry(path.into())
            .or_default()
            .push_back(fault);
    }
    pub fn uploads(&self) -> Vec<Upload> {
        self.shared.uploads.lock().unwrap().clone()
    }
    pub fn status(&self, status: u16) {
        self.shared.status.store(status, Ordering::SeqCst);
    }
    pub fn hold(&self, hold: bool) {
        self.shared.hold.store(hold, Ordering::SeqCst);
    }
    pub fn release(&self) {
        self.shared.gate.add_permits(1);
    }
    pub fn diagnostics(&self) -> Value {
        json!({ "uploads": self.uploads(), "get_requests": *self.shared.gets.lock().unwrap(), "master_requests": *self.shared.master.lock().unwrap(), "transport_faults": *self.shared.fault_log.lock().unwrap() })
    }
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.shared.gate.close();
        self.http_task.abort();
        self.master_task.abort();
        self.proxy_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multipart_parses_json_and_rejects_duplicate_fields() {
        let body = "--test\r\nContent-Disposition: form-data; name=\"workers\"; filename=\"workers_x.json\"\r\n\r\n{}\r\n--test\r\nContent-Disposition: form-data; name=\"rewards\"; filename=\"rewards_x.json\"\r\n\r\n{\"alice\":1.2}\r\n--test--\r\n";
        let parts = parse_parts("multipart/form-data; boundary=test", body.as_bytes()).unwrap();
        assert_eq!(parts["rewards"].json["alice"], 1.2);
        assert!(parse_parts(
            "multipart/form-data; boundary=test",
            body.replace("name=\"rewards\"", "name=\"workers\"")
                .as_bytes()
        )
        .is_err());
    }
}
