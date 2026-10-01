//! Transparent L7 proxy: one request = one backend.
//!
//! Contract (see hakodb/hakobalancer#1): method/path/headers/body cross
//! verbatim, status preserved, bodies stream (never buffered), hop-by-hop
//! headers stripped, X-Forwarded-For set to the real peer (replaced, never
//! appended — phase 1 has no trusted-proxy concept), /api/admin/reload
//! rejected at the edge, empty pool fails closed.
//!
//! Upgrade-flagged requests (WS) get 501 in phase 1: tunneling needs a raw
//! socket relay + sticky routing, both phase-2 work. SSE needs nothing
//! special (plain streaming GET) and passes through today. A loud 501
//! beats a silently hanging half-upgrade.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, Response, StatusCode};

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
    pub async fn handle(&self, peer: IpAddr, req: Request<Body>) -> Response<Body> {
        if is_admin_reload(&req) {
            return err(
                StatusCode::METHOD_NOT_ALLOWED,
                "admin targets backends directly",
            );
        }
        if wants_upgrade(&req) {
            return err(
                StatusCode::NOT_IMPLEMENTED,
                "upgrade tunneling lands with sticky routing (phase 2)",
            );
        }
        let Some(backend) = self.pool.pick() else {
            return err(StatusCode::SERVICE_UNAVAILABLE, "no healthy backends");
        };
        match self.forward(peer, &backend.target, req).await {
            Ok(resp) => resp,
            Err(_) => err(StatusCode::BAD_GATEWAY, "upstream error"),
        }
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
