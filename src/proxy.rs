//! Transparent L7 proxy: one request = one backend.
//!
//! Contract (see hakodb/hakobalancer#1): method/path/headers/body cross
//! verbatim, status preserved, bodies stream (never buffered), hop-by-hop
//! headers stripped, X-Forwarded-For set to the real peer (replaced, never
//! appended — phase 1 has no trusted-proxy concept), /api/admin/reload
//! rejected at the edge, empty pool fails closed.
//!
//! Upgrade-flagged requests (WS) relay over a raw socket to the picked
//! (sticky-pinned) backend: handshake bytes verbatim, 101 relays both
//! directions, refused upgrades forward status with an empty body
//! (documented: refusals here are status-only). SSE needs nothing special
//! (plain streaming GET) and passes through today.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::pool::{Pool, Target};

/// Per-backend timeout (issue #1: timeouts + 502/503). Fixed in phase 1;
// TODO(config): per-backend timeouts.
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(60);

/// Hop-by-hop headers: terminated here, never forwarded either way.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn err(status: StatusCode, msg: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Body::from(msg.to_string()))
        .unwrap()
}

/// Admin reload targets backends directly (split-config states otherwise).
fn is_admin_reload(req: &Request<Body>) -> bool {
    req.uri().path() == "/api/admin/reload"
}

fn wants_upgrade(req: &Request<Body>) -> bool {
    req.headers().contains_key("upgrade")
}

/// Sticky pin cookie: first response pins hb_route=<pool index>; later
/// requests carrying a valid pin for a healthy backend go there, so WS
/// subscriptions and their publishers land together. Anything else
/// round-robins and re-pins. Ejected pins fall back + re-pin.
pub const PIN_COOKIE: &str = "hb_route";

/// Pool index from the pin cookie, or None (absent/malformed).
pub fn pin_from(req: &Request<Body>) -> Option<usize> {
    let cookies = req.headers().get("cookie")?.to_str().ok()?;
    for pair in cookies.split(';') {
        let pair = pair.trim();
        if let Some(v) = pair.strip_prefix("hb_route=") {
            if let Ok(n) = v.parse::<usize>() {
                return Some(n);
            }
        }
    }
    None
}

fn pin_cookie_value(index: usize) -> String {
    format!("{PIN_COOKIE}={index}; Path=/")
}

/// Minimal HTTP response-head parser for the upgrade path: status +
/// headers + bytes consumed (rest stays on the wire for relay).
/// None = garbage/oversize (fail the tunnel, don't guess).
fn parse_response_head(buf: &[u8]) -> Option<(u16, HeaderMap, usize)> {
    const CAP: usize = 32 * 1024;
    if buf.len() > CAP {
        return None;
    }
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&buf[..end]).ok()?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next()?;
    let mut parts = status_line.splitn(3, ' ');
    parts.next()?; // HTTP-version
    let status: u16 = parts.next()?.parse().ok()?;
    let mut headers = HeaderMap::new();
    for line in lines {
        let (k, v) = line.split_once(':')?;
        let name: axum::http::HeaderName = k.trim().parse().ok()?;
        // ponytail: skip (don't fail) one bad header — a single junk
        // header must not kill an otherwise good upgrade.
        if let Ok(val) = v.trim().parse() {
            headers.append(name, val);
        }
    }
    Some((status, headers, end + 4))
}

pub struct Proxy {
    pool: Arc<Pool>,
    http: HttpClient,
    #[cfg(unix)]
    uds: UdsClient,
}

type HttpClient = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    Body,
>;
#[cfg(unix)]
type UdsClient =
    hyper_util::client::legacy::Client<hyperlocal::UnixConnector, Body>;

impl Proxy {
    pub fn new(pool: Arc<Pool>) -> Self {
        let http = hyper_util::client::legacy::Client::builder(
            hyper_util::rt::TokioExecutor::new(),
        )
        .build_http();
        #[cfg(unix)]
        let uds = hyper_util::client::legacy::Client::builder(
            hyper_util::rt::TokioExecutor::new(),
        )
        .build(hyperlocal::UnixConnector);
        Self {
            pool,
            http,
            #[cfg(unix)]
            uds,
        }
    }

    /// Route + forward one request. `peer` is the TCP peer IP (XFF source).
    /// Pin cookie honored when it names a healthy backend; otherwise
    /// round-robin + re-pin. Served responses carry the (possibly fresh)
    /// pin; edge errors carry none.
    pub async fn handle(&self, peer: IpAddr, req: Request<Body>) -> Response<Body> {
        if is_admin_reload(&req) {
            return err(
                StatusCode::METHOD_NOT_ALLOWED,
                "admin targets backends directly",
            );
        }
        let pinned = pin_from(&req)
            .filter(|&i| self.pool.healthy_at(i))
            .and_then(|i| self.pool.backend_at(i).map(|b| (i, b)));
        let (index, backend) = match pinned {
            Some(p) => p,
            None => match self.pool.pick_index() {
                Some(p) => p,
                None => {
                    return err(StatusCode::SERVICE_UNAVAILABLE, "no healthy backends")
                }
            },
        };
        let mut resp = if wants_upgrade(&req) {
            self.tunnel(&backend.target, req).await
        } else {
            match self.forward(peer, &backend.target, req).await {
                Ok(r) => r,
                Err(_) => err(StatusCode::BAD_GATEWAY, "upstream error"),
            }
        };
        // (Re-)pin on every served response: self-heals stale cookies,
        // costs one small header.
        if let Ok(v) = pin_cookie_value(index).parse() {
            resp.headers_mut().insert("set-cookie", v);
        }
        resp
    }

    async fn forward(
        &self,
        peer: IpAddr,
        target: &Target,
        req: Request<Body>,
    ) -> Result<Response<Body>, ()> {
        let (mut parts, body) = req.into_parts();
        // Strip hop-by-hop, then stamp the real peer (replace, not append:
        // nothing upstream of a phase-1 balancer is trusted to sanitize).
        let mut headers = HeaderMap::new();
        for (k, v) in parts.headers.iter() {
            if !is_hop_by_hop(k.as_str()) {
                headers.append(k, v.clone());
            }
        }
        headers.insert("x-forwarded-for", peer.to_string().parse().unwrap());
        parts.headers = headers;
        match target {
            Target::Tcp(addr) => {
                let uri = format!(
                    "http://{addr}{}{}",
                    parts.uri.path(),
                    parts
                        .uri
                        .query()
                        .map(|q| format!("?{q}"))
                        .unwrap_or_default()
                );
                parts.uri = uri.parse().map_err(|_| ())?;
                let up = Request::from_parts(parts, body);
                let resp = tokio::time::timeout(UPSTREAM_TIMEOUT, self.http.request(up))
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())?;
                Ok(strip_hop_headers(resp))
            }
            #[cfg(unix)]
            Target::Sock(sock) => {
                let path = format!(
                    "{}{}",
                    parts.uri.path(),
                    parts
                        .uri
                        .query()
                        .map(|q| format!("?{q}"))
                        .unwrap_or_default()
                );
                parts.uri = hyperlocal::Uri::new(sock, &path).into();
                let up = Request::from_parts(parts, body);
                let resp = tokio::time::timeout(UPSTREAM_TIMEOUT, self.uds.request(up))
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())?;
                Ok(strip_hop_headers(resp))
            }
            #[cfg(not(unix))]
            Target::Sock(_) => Err(()),
        }
    }
}

/// Byte relay with an upstream preface: bytes already read past the
/// response head go downstream first, then both directions copy until
/// either side closes. The preface is the load-bearing detail — pipelined
/// post-101 frames would otherwise be silently dropped.
async fn relay_with_preface<A>(down_fut: hyper::upgrade::OnUpgrade, mut up: A, preface: Vec<u8>)
where
    A: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let down = match down_fut.await {
        Ok(d) => d,
        Err(_) => return,
    };
    // ponytail: hyper's Upgraded speaks hyper::rt IO, not tokio's —
    // TokioIo adapts it (same wrapper hyper-util uses everywhere).
    let mut down = hyper_util::rt::TokioIo::new(down);
    if !preface.is_empty() && down.write_all(&preface).await.is_err() {
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut down, &mut up).await;
}

impl Proxy {
    /// Raw-socket upgrade path for one picked backend. Returns the
    /// response to send downstream (101 + spawned relay, a forwarded
    /// non-101, 413 on over-limit bodies, 502 on dial/parse failure).
    async fn tunnel(&self, target: &Target, req: Request<Body>) -> Response<Body> {
        // Handshake bodies are header-only in practice; bound the
        // collection so a hostile upgrade can't OOM the balancer.
        const BODY_CAP: usize = 64 * 1024;
        const HEAD_CAP: usize = 32 * 1024;
        let (parts, body) = req.into_parts();
        let body_bytes = match Limited::new(body, BODY_CAP).collect().await {
            Ok(b) => b.to_bytes(),
            Err(_) => return err(StatusCode::PAYLOAD_TOO_LARGE, "upgrade body too large"),
        };
        // Shell request for the downstream upgrade future. hyper's
        // OnUpgrade future is owned ('static): take it out now, resolve
        // it inside the spawned relay after we answer 101.
        let mut shell = Request::builder()
            .method(parts.method.clone())
            .uri(parts.uri.clone())
            .body(Body::empty())
            .unwrap();
        for (k, v) in parts.headers.iter() {
            shell.headers_mut().append(k, v.clone());
        }
        let down_fut = hyper::upgrade::on(&mut shell);
        // Raw request bytes upstream (same method/path/query/version and
        // headers the client sent, minus framing headers that would
        // corrupt the relay). Host passes through untouched (backend
        // host-gate sees the real domain).
        let mut raw = Vec::with_capacity(512 + body_bytes.len());
        let ver = match parts.version {
            ::axum::http::Version::HTTP_10 => "1.0",
            _ => "1.1",
        };
        let path_q = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        raw.extend_from_slice(format!("{} {} HTTP/{}\r\n", parts.method, path_q, ver).as_bytes());
        // ponytail: tunnel keeps upgrade framing (Upgrade/Connection/
        // Sec-* are the handshake). Only proxy credentials and STALE
        // framing go: body_bytes are written raw with no chunking, so an
        // inherited content-length/transfer-encoding would desync the relay.
        for (k, v) in parts.headers.iter() {
            let n = k.as_str();
            if matches!(
                n,
                "proxy-authenticate"
                    | "proxy-authorization"
                    | "te"
                    | "trailer"
                    | "content-length"
                    | "transfer-encoding"
            ) {
                continue;
            }
            raw.extend_from_slice(n.as_bytes());
            raw.extend_from_slice(b": ");
            raw.extend_from_slice(v.as_bytes());
            raw.extend_from_slice(b"\r\n");
        }
        raw.extend_from_slice(b"\r\n");
        raw.extend_from_slice(&body_bytes);

        // Dial + write + head parse, budgeted (handshakes are quick).
        // ponytail: BufReader threaded through (not a bare Vec): bytes
        // read past the head stay available as the relay preface.
        // Generic arms (Tcp/Uds) share read_head; relay is generic too.
        let dialed = async {
            match target {
                Target::Tcp(addr) => {
                    let mut up = tokio::net::TcpStream::connect(addr).await?;
                    up.write_all(&raw).await?;
                    let rd = tokio::io::BufReader::new(up);
                    let (status, headers, rd, tail) = read_head(rd, HEAD_CAP).await?;
                    Ok((RelayStream::Tcp(rd), status, headers, tail))
                }
                #[cfg(unix)]
                Target::Sock(sock) => {
                    let mut up = tokio::net::UnixStream::connect(sock).await?;
                    up.write_all(&raw).await?;
                    let rd = tokio::io::BufReader::new(up);
                    let (status, headers, rd, tail) = read_head(rd, HEAD_CAP).await?;
                    Ok((RelayStream::Uds(rd), status, headers, tail))
                }
                #[cfg(not(unix))]
                Target::Sock(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "unix upstream",
                )),
            }
        };
        let (up, status, mut headers, tail) =
            match tokio::time::timeout(Duration::from_secs(10), dialed).await {
                Ok(Ok(v)) => v,
                _ => return err(StatusCode::BAD_GATEWAY, "upgrade upstream failed"),
            };
        if status != 101 {
            // Refused upgrade: status + headers cross, empty body, close.
            // (Documented deviation: refused-upgrade bodies are dropped —
            // handshake refusals here are status-only.)
            headers.remove("content-length");
            headers.remove("transfer-encoding");
            let mut b = Response::builder().status(status);
            for (k, v) in headers.iter() {
                b = b.header(k, v);
            }
            return b
                .header("connection", "close")
                .body(Body::empty())
                .unwrap();
        }
        // 101: pass the handshake headers through, relay bytes both ways.
        let mut b = Response::builder().status(101);
        for (k, v) in headers.iter() {
            b = b.header(k, v);
        }
        let resp = b.body(Body::empty()).unwrap();
        tokio::spawn(async move {
            match up {
                RelayStream::Tcp(rd) => {
                    let up = rd.into_inner();
                    relay_with_preface(down_fut, up, tail).await
                }
                #[cfg(unix)]
                RelayStream::Uds(rd) => {
                    let up = rd.into_inner();
                    relay_with_preface(down_fut, up, tail).await
                }
            }
        });
        resp
    }
}

/// Upstream byte stream behind a BufReader (keeps post-head bytes for
/// the relay preface instead of dropping them).
enum RelayStream {
    Tcp(tokio::io::BufReader<tokio::net::TcpStream>),
    #[cfg(unix)]
    Uds(tokio::io::BufReader<tokio::net::UnixStream>),
}

/// Read until end-of-head (\r\n\r\n) or cap; parse status + headers.
/// Returns the reader (positioned past everything read) plus the
/// post-head tail bytes for the relay preface.
async fn read_head<S>(
    mut rd: tokio::io::BufReader<S>,
    cap: usize,
) -> std::io::Result<(u16, HeaderMap, tokio::io::BufReader<S>, Vec<u8>)>
where
    S: AsyncReadExt + Unpin,
{
    use tokio::io::AsyncBufReadExt;
    let mut head = Vec::with_capacity(512);
    loop {
        let buf = rd.fill_buf().await?;
        if buf.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "upstream closed during head",
            ));
        }
        head.extend_from_slice(buf);
        let n = buf.len();
        rd.consume(n);
        if head.len() > cap {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "response head too large",
            ));
        }
        if let Some((status, headers, used)) = parse_response_head(&head) {
            return Ok((status, headers, rd, head[used..].to_vec()));
        }
    }
}

/// Strip hop-by-hop response headers; status + body cross verbatim.
fn strip_hop_headers(resp: Response<hyper::body::Incoming>) -> Response<Body> {
    let (mut parts, body) = resp.into_parts();
    // ponytail: no HeaderMap::retain on this http version — collect then
    // remove (responses carry a handful of headers; no perf story here).
    let drop: Vec<_> = parts
        .headers
        .keys()
        .filter(|k| is_hop_by_hop(k.as_str()))
        .cloned()
        .collect();
    for k in drop {
        parts.headers.remove(k);
    }
    Response::from_parts(parts, Body::new(body))
}
