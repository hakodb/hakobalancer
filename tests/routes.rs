//! Microservices endpoint routing (hakobalancer#3): prefix table maps
//! request paths to backends, default backend catches the rest. Mapped
//! traffic is deterministic (the route IS the stickiness — pins neither
//! read nor set there); unmapped traffic keeps pool+pin behavior.
//! Ejected mapped backend: safe methods fail over to the default,
//! unsafe methods fail closed (no split-brain writes).

use std::sync::Arc;

use hakobalancer::pool::{Pool, Target};
use hakobalancer::proxy::{Proxy, Route};

async fn spawn_tagged(
    name: &'static str,
    hits: Arc<std::sync::Mutex<Vec<&'static str>>>,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let hits = hits.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
                let (rd, mut wr) = stream.into_split();
                let mut lines = tokio::io::BufReader::new(rd);
                let mut request_line = String::new();
                if lines.read_line(&mut request_line).await.is_err() {
                    return;
                }
                loop {
                    let mut h = String::new();
                    if lines.read_line(&mut h).await.unwrap_or(0) == 0 {
                        break;
                    }
                    if h.trim_end().is_empty() {
                        break;
                    }
                }
                hits.lock().unwrap().push(name);
                let body = request_line.trim().to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = wr.write_all(resp.as_bytes()).await;
            });
        }
    });
    addr
}

fn get(path: &str, cookie: Option<&str>) -> http::Request<axum::body::Body> {
    let mut b = http::Request::builder()
        .method("GET")
        .uri(format!("http://balancer{path}"));
    if let Some(c) = cookie {
        b = b.header("cookie", c);
    }
    b.body(axum::body::Body::empty()).unwrap()
}

fn post(path: &str) -> http::Request<axum::body::Body> {
    http::Request::builder()
        .method("POST")
        .uri(format!("http://balancer{path}"))
        .body(axum::body::Body::empty())
        .unwrap()
}

#[tokio::test]
async fn mapped_prefix_routes_to_backend() {
    let ha2 = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hb2 = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hc2 = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_tagged("A", ha2.clone()).await;
    let ab = spawn_tagged("B", hb2.clone()).await;
    let ac = spawn_tagged("C", hc2.clone()).await;
    let pool = Arc::new(Pool::new(
        vec![Target::Tcp(aa), Target::Tcp(ab), Target::Tcp(ac)],
        1,
    ));
    let mut proxy = Proxy::new(pool);
    proxy.with_routing(
        vec![
            Route { prefix: "/api/endpoint-a".into(), backend: 0 },
            Route { prefix: "/api/endpoint-c".into(), backend: 1 },
        ],
        Some(2),
    );
    let peer = "127.0.0.1".parse().unwrap();
    assert_eq!(proxy.handle(peer, get("/api/endpoint-a/1", None)).await.status(), 200);
    assert_eq!(proxy.handle(peer, get("/api/endpoint-c", None)).await.status(), 200);
    // Default catches the rest.
    assert_eq!(proxy.handle(peer, get("/api/other", None)).await.status(), 200);
    // Prefix boundary: /api/endpoint-abc is NOT under /api/endpoint-a.
    assert_eq!(proxy.handle(peer, get("/api/endpoint-abc", None)).await.status(), 200);
    let (na, nb, nc) = (
        ha2.lock().unwrap().len(),
        hb2.lock().unwrap().len(),
        hc2.lock().unwrap().len(),
    );
    assert_eq!((na, nb, nc), (1, 1, 2), "a->A, c->B, other+abc->C");
}

#[tokio::test]
async fn unmatched_without_default_is_503() {
    let pool = Arc::new(Pool::new(vec![Target::Tcp("x".into())], 1));
    let mut p = Proxy::new(pool);
    p.with_routing(vec![Route { prefix: "/api/a".into(), backend: 0 }], None);
    let peer = "127.0.0.1".parse().unwrap();
    let r = p.handle(peer, get("/api/other", None)).await;
    assert_eq!(r.status(), 503);
}

#[tokio::test]
async fn ejected_mapped_safe_method_fails_over_unsafe_503() {
    let ha = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hc = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_tagged("A", ha.clone()).await;
    let ab = spawn_tagged("B", Arc::new(std::sync::Mutex::new(Vec::new()))).await;
    let ac = spawn_tagged("C", hc.clone()).await;
    let pool = Arc::new(Pool::new(
        vec![Target::Tcp(aa), Target::Tcp(ab), Target::Tcp(ac)],
        1,
    ));
    let mut proxy = Proxy::new(pool.clone());
    proxy.with_routing(vec![Route { prefix: "/api/a".into(), backend: 0 }], Some(2));
    let peer = "127.0.0.1".parse().unwrap();
    pool.probe(0, false); // eject A
    // Safe method fails over to default C.
    let r = proxy.handle(peer, get("/api/a/1", None)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(hc.lock().unwrap().len(), 1);
    assert!(ha.lock().unwrap().is_empty());
    // Unsafe method fails closed (no split-brain writes).
    let r = proxy.handle(peer, post("/api/a/1")).await;
    assert_eq!(r.status(), 503);
}

#[tokio::test]
async fn mapped_path_ignores_pin_cookie() {
    let hb = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hc = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_tagged("A", Arc::new(std::sync::Mutex::new(Vec::new()))).await;
    let ab = spawn_tagged("B", hb.clone()).await;
    let ac = spawn_tagged("C", hc.clone()).await;
    let pool = Arc::new(Pool::new(
        vec![Target::Tcp(aa), Target::Tcp(ab), Target::Tcp(ac)],
        1,
    ));
    let mut proxy = Proxy::new(pool);
    proxy.with_routing(vec![Route { prefix: "/api/b".into(), backend: 1 }], Some(2));
    let peer = "127.0.0.1".parse().unwrap();
    // Pin says C(2), route says B(1): route wins, no re-pin emitted.
    let r = proxy.handle(peer, get("/api/b/1", Some("hb_route=2"))).await;
    assert_eq!(r.status(), 200);
    assert_eq!(hb.lock().unwrap().len(), 1);
    assert!(hc.lock().unwrap().is_empty());
    assert!(r.headers().get("set-cookie").is_none());
}
