//! Loopback replacements for the pool's HTTP sync service and master TCP server.
//!
//! A [`Mock`] runs three tasks:
//!
//! * an **HTTP server** serving rewards, bans, the public-IP lookup and the
//!   multipart `/pool_sync/` upload;
//! * a **fault proxy** in front of the HTTP server. The pool only ever talks to
//!   the proxy, which can drop or delay a connection before HTTP sees it. That
//!   produces real transport failures that a status code cannot emulate;
//! * a fake **master server** that answers `PoolLogin` / `PoolLogout`.
//!
//! Every request is recorded as test evidence. Scenarios can queue scripted
//! responses and faults, change the upload status, and hold uploads in flight.

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
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};
use warp::{
    http::{header, HeaderValue, Method, StatusCode},
    reply::Response as HttpResponse,
    Filter, Reply,
};

/// One multipart field of a sync upload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Part {
    pub filename: String,
    pub json: Value,
}

/// One `/pool_sync/` request as seen by the mock.
#[derive(Clone, Debug, Serialize)]
pub struct Upload {
    /// Query string (`host`, `blockIncrease`, `connections`, ...).
    pub query: BTreeMap<String, String>,
    /// Parsed `workers` and `rewards` parts; empty if parsing failed.
    pub parts: BTreeMap<String, Part>,
    /// Multipart parse error, if any.
    pub error: Option<String>,
    /// `false` while the request is held in flight, `true` once answered.
    pub completed: bool,
    /// HTTP status that was returned (0 until completed).
    pub status: u16,
}

/// State shared between the mock's tasks and the scenario.
///
/// Scenarios change this state between requests. The std mutexes are only held
/// for short sections, never across an `.await`.
pub struct Shared {
    pub uploads: Mutex<Vec<Upload>>,
    /// Paths of every GET request, in order.
    pub gets: Mutex<Vec<String>>,
    /// Every recognized master-server message (login and logout).
    pub master: Mutex<Vec<String>>,
    /// Default status for uploads without a scripted response. Starts at 503,
    /// so accounting state is kept until a scenario asks for success.
    pub status: AtomicU16,
    /// While `true`, each upload waits for a permit on `gate` before answering.
    pub hold: AtomicBool,
    pub gate: Semaphore,
    /// Body of `/bans.json`.
    pub bans: Mutex<Value>,
    /// Body of `/curve.json`.
    pub curve: Mutex<Value>,
    /// Scripted HTTP responses, FIFO per path.
    pub responses: Mutex<BTreeMap<String, VecDeque<Response>>>,
    /// Scripted transport faults, FIFO per path (consumed by the proxy).
    pub faults: Mutex<BTreeMap<String, VecDeque<Fault>>>,
    /// Faults that were actually applied.
    pub fault_log: Mutex<Vec<String>>,
    pub master_reply: Mutex<MasterReply>,
}

/// A scripted HTTP response.
#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub body: String,
}

/// A transport fault applied by the proxy to the next request for a path.
#[derive(Clone, Debug)]
pub enum Fault {
    /// Close the connection without forwarding the request.
    Disconnect,
    /// Wait this many milliseconds, then forward normally.
    Delay(u64),
}

/// How the fake master server answers a login.
#[derive(Clone, Debug)]
pub struct MasterReply {
    /// Sent on connect. Empty means "close immediately without greeting".
    pub greeting: Vec<u8>,
    /// Sent after a well-formed `PoolLogin,<json>`.
    pub reply: Vec<u8>,
    /// Delay before the greeting.
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

/// A running set of loopback services. Dropping it stops all tasks.
pub struct Mock {
    pub shared: Arc<Shared>,
    /// Base URL the pool should use (the fault proxy, not the HTTP server).
    pub http: String,
    pub master_port: u16,
    http_task: JoinHandle<()>,
    master_task: JoinHandle<()>,
    proxy_task: JoinHandle<()>,
}

/// Picks a currently free loopback port.
///
/// The listener is released immediately, so this is inherently advisory. Pool
/// startup detects a bind race and reports it as a setup failure rather than
/// allowing a scenario to target an unrelated service.
pub fn unused_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind(("127.0.0.1", 0))?
        .local_addr()?
        .port())
}

/// The repository's reward curve, the base of the `/curve.json` fixture.
const CURVE: &str = include_str!("../../config/curve.json");

/// The default `/curve.json` fixture.
///
/// `blocks` makes every accepted share a block (`block_probability = 1`), the
/// curve's only block setting.
pub fn curve(blocks: bool) -> Value {
    let mut curve: Value = serde_json::from_str(CURVE).expect("config/curve.json is valid JSON");
    if blocks {
        curve["block_probability"] = json!(1);
    }
    curve
}

/// Extracts the `workers` and `rewards` JSON parts from a sync upload.
///
/// This parser is intentionally small, not a general-purpose multipart
/// implementation. It rejects ambiguous input such as duplicate fields.
fn parse_parts(content_type: &str, body: &[u8]) -> Result<BTreeMap<String, Part>> {
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|s| s.strip_prefix("boundary="))
        .context("Missing multipart boundary")?
        .trim_matches('"');
    anyhow::ensure!(!boundary.is_empty(), "Empty multipart boundary");
    let text = std::str::from_utf8(body)?;
    let mut parts = BTreeMap::new();
    // Skip the preamble before the first delimiter; stop at the closing `--`.
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
        // Reads `key="value"` from the Content-Disposition header.
        let attribute = |key: &str| -> Result<String> {
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
        let name = attribute("name")?;
        anyhow::ensure!(
            !parts.contains_key(&name),
            "Duplicate multipart field {name}"
        );
        let json = payload
            .strip_suffix("\r\n")
            .context("Missing part terminator")?;
        parts.insert(
            name,
            Part {
                filename: attribute("filename")?,
                json: serde_json::from_str(json)?,
            },
        );
    }
    anyhow::ensure!(
        parts.len() == 2 && parts.contains_key("workers") && parts.contains_key("rewards"),
        "Expected exactly workers and rewards parts"
    );
    Ok(parts)
}

fn text_reply(status: u16, body: impl Into<String>) -> HttpResponse {
    let status = StatusCode::from_u16(status).expect("valid scripted HTTP status");
    warp::reply::with_status(body.into(), status).into_response()
}

/// One HTTP request, after warp has extracted the parts the mock needs.
struct Request {
    method: Method,
    path: String,
    query: BTreeMap<String, String>,
    content_type: Option<String>,
    body: bytes::Bytes,
}

/// Routes one HTTP request. A scripted response for the path, if any, wins.
async fn handle_request(state: Arc<Shared>, request: Request) -> HttpResponse {
    let scripted = state
        .responses
        .lock()
        .unwrap()
        .get_mut(&request.path)
        .and_then(VecDeque::pop_front);
    let mut response = if request.method == Method::GET {
        handle_get(&state, &request.path, scripted)
    } else if request.method == Method::POST && request.path == "/pool_sync/" {
        handle_sync_upload(&state, request, scripted).await
    } else {
        text_reply(404, "Unexpected request")
    };
    // One request per connection makes scripted transport faults deterministic:
    // a later fault cannot be bypassed by connection reuse.
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));
    response
}

fn handle_get(state: &Shared, path: &str, scripted: Option<Response>) -> HttpResponse {
    state.gets.lock().unwrap().push(path.to_string());
    if let Some(response) = scripted {
        return text_reply(response.status, response.body);
    }
    match path {
        "/bans.json" => warp::reply::json(&*state.bans.lock().unwrap()).into_response(),
        "/curve.json" => warp::reply::json(&*state.curve.lock().unwrap()).into_response(),
        "/ip" => text_reply(200, "127.0.0.1"),
        _ => text_reply(404, "Unexpected GET"),
    }
}

/// Records an upload, optionally holds it in flight, then answers it.
async fn handle_sync_upload(
    state: &Shared,
    request: Request,
    scripted: Option<Response>,
) -> HttpResponse {
    let (parts, error) =
        match parse_parts(request.content_type.as_deref().unwrap_or(""), &request.body) {
            Ok(parts) => (parts, None),
            Err(e) => (BTreeMap::new(), Some(format!("{e:#}"))),
        };
    // Record it before holding, so a scenario can see the in-flight upload.
    let id = {
        let mut uploads = state.uploads.lock().unwrap();
        uploads.push(Upload {
            query: request.query,
            parts,
            error,
            completed: false,
            status: 0,
        });
        uploads.len() - 1
    };
    if state.hold.load(Ordering::SeqCst) {
        // One permit releases one held upload. A closed gate (mock dropped)
        // also releases it.
        if let Ok(permit) = state.gate.acquire().await {
            permit.forget();
        }
    }
    let status = scripted
        .as_ref()
        .map_or_else(|| state.status.load(Ordering::SeqCst), |r| r.status);
    {
        let mut uploads = state.uploads.lock().unwrap();
        uploads[id].completed = true;
        uploads[id].status = status;
    }
    // The real server puts its verdict in a JSON body; a 2xx status alone is
    // not an acknowledgement.
    let body = scripted.map_or_else(
        || {
            let success = (200..300).contains(&status);
            json!({ "success": success, "message": "mock sync response" }).to_string()
        },
        |r| r.body,
    );
    text_reply(status, body)
}

/// Fake master server: greet, then wait for a login or logout message.
async fn serve_master_connection(state: Arc<Shared>, mut socket: TcpStream) -> std::io::Result<()> {
    let response = state.master_reply.lock().unwrap().clone();
    tokio::time::sleep(Duration::from_millis(response.delay_ms)).await;
    if response.greeting.is_empty() {
        return Ok(());
    }
    socket.write_all(&response.greeting).await?;
    // The pool may split its message across reads; accumulate until it parses.
    let mut message = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        message.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&message);
        let is_login = text
            .strip_prefix("PoolLogin,")
            .is_some_and(|json| serde_json::from_str::<Value>(json).is_ok());
        if is_login {
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
    Ok(())
}

/// Applies any queued fault for the request's path, then forwards the
/// connection to the real HTTP server.
async fn proxy_connection(
    state: Arc<Shared>,
    mut incoming: TcpStream,
    http_port: u16,
) -> std::io::Result<()> {
    // The first read holds the request line, which is all the routing needs.
    let mut first = vec![0; 8192];
    let n = incoming.read(&mut first).await?;
    if n == 0 {
        return Ok(());
    }
    // "GET /path?query HTTP/1.1" -> "/path"
    let request_line = String::from_utf8_lossy(&first[..n]);
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .split('?')
        .next()
        .unwrap_or("")
        .to_owned();
    let fault = state
        .faults
        .lock()
        .unwrap()
        .get_mut(&path)
        .and_then(VecDeque::pop_front);
    if let Some(fault) = fault {
        state
            .fault_log
            .lock()
            .unwrap()
            .push(format!("{path}: {fault:?}"));
        match fault {
            Fault::Disconnect => return Ok(()),
            Fault::Delay(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
    }
    let mut upstream = TcpStream::connect(("127.0.0.1", http_port)).await?;
    upstream.write_all(&first[..n]).await?;
    tokio::io::copy_bidirectional(&mut incoming, &mut upstream).await?;
    Ok(())
}

impl Mock {
    pub async fn start(blocks: bool) -> Result<Self> {
        let shared = Arc::new(Shared {
            uploads: Mutex::new(vec![]),
            gets: Mutex::new(vec![]),
            master: Mutex::new(vec![]),
            status: AtomicU16::new(503),
            hold: AtomicBool::new(false),
            gate: Semaphore::new(0),
            bans: Mutex::new(json!(["banned-verifier-user"])),
            curve: Mutex::new(curve(blocks)),
            responses: Mutex::new(BTreeMap::new()),
            faults: Mutex::new(BTreeMap::new()),
            fault_log: Mutex::new(vec![]),
            master_reply: Mutex::new(MasterReply::default()),
        });

        // HTTP application server.
        let http_port = unused_port()?;
        let state = shared.clone();
        let routes = warp::path::full()
            .and(warp::method())
            .and(warp::query::<BTreeMap<String, String>>())
            .and(warp::header::optional::<String>("content-type"))
            .and(warp::body::bytes())
            .and_then(
                move |path: warp::path::FullPath,
                      method: Method,
                      query: BTreeMap<String, String>,
                      content_type: Option<String>,
                      body: bytes::Bytes| {
                    let request = Request {
                        method,
                        path: path.as_str().to_owned(),
                        query,
                        content_type,
                        body,
                    };
                    let state = state.clone();
                    async move { Ok::<_, Infallible>(handle_request(state, request).await) }
                },
            )
            .boxed();
        let server = warp::serve(routes).bind(([127, 0, 0, 1], http_port)).await;
        let http_task = tokio::spawn(server.run());

        // Fake master server. Each connection gets five seconds in total.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let master_port = listener.local_addr()?.port();
        let state = shared.clone();
        let master_task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let connection = serve_master_connection(state.clone(), socket);
                let _ = tokio::time::timeout(Duration::from_secs(5), connection).await;
            }
        });

        // Fault proxy in front of the HTTP server.
        let proxy = TcpListener::bind(("127.0.0.1", 0)).await?;
        let proxy_port = proxy.local_addr()?.port();
        let state = shared.clone();
        let proxy_task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = proxy.accept() => {
                        let Ok((incoming, _)) = accepted else { break };
                        connections.spawn(proxy_connection(state.clone(), incoming, http_port));
                    }
                    // Reap finished connections so the set does not grow unbounded.
                    _ = connections.join_next(), if !connections.is_empty() => {}
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

    /// Queues a scripted response for the next request to `path`. Responses
    /// are FIFO per path, so a scenario can script a failure followed by a retry.
    pub fn respond(&self, path: &str, status: u16, body: impl Into<String>) {
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

    /// Queues a transport fault for the next connection requesting `path`.
    pub fn fault(&self, path: &str, fault: Fault) {
        self.shared
            .faults
            .lock()
            .unwrap()
            .entry(path.into())
            .or_default()
            .push_back(fault);
    }

    /// Snapshot of all uploads received so far.
    pub fn uploads(&self) -> Vec<Upload> {
        self.shared.uploads.lock().unwrap().clone()
    }

    /// Sets the default status for unscripted uploads.
    pub fn status(&self, status: u16) {
        self.shared.status.store(status, Ordering::SeqCst);
    }

    /// While `true`, new uploads stay in flight until [`release`](Self::release).
    pub fn hold(&self, hold: bool) {
        self.shared.hold.store(hold, Ordering::SeqCst);
    }

    /// Lets one held upload complete.
    pub fn release(&self) {
        self.shared.gate.add_permits(1);
    }

    /// Everything the mock observed, for `requests.json`.
    pub fn diagnostics(&self) -> Value {
        json!({
            "uploads": self.uploads(),
            "get_requests": *self.shared.gets.lock().unwrap(),
            "master_requests": *self.shared.master.lock().unwrap(),
            "transport_faults": *self.shared.fault_log.lock().unwrap(),
        })
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        // Closing the gate releases any upload still held in flight.
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
