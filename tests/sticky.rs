//! Sticky routing (pin cookie) + upgrade tunneling (101 relay).
//!
//! Pin contract: first response sets hb_route=<index>; later requests
//! carrying a valid pin for a healthy backend go there, everything else
//! round-robins and re-pins. Ejected pins fall back + re-pin.

use std::sync::Arc;

use hakobalancer::pool::{Pool, Target};
use hakobalancer::proxy::Proxy;

async fn spawn_upstream(
    name: &'static str,
    hits: Arc<std::sync::Mutex<Vec<&'static str>>>,
    refuse_upgrade_path: Option<&'static str>,
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
                let mut is_upgrade = false;
                loop {
                    let mut h = String::new();
                    if lines.read_line(&mut h).await.unwrap_or(0) == 0 {
                        break;
                    }
                    let t = h.trim_end().to_string();
                    if t.is_empty() {
                        break;
                    }
                    if t.to_ascii_lowercase().starts_with("upgrade:") {
                        is_upgrade = true;
                    }
                }
                hits.lock().unwrap().push(name);
                let path = request_line.split_whitespace().nth(1).unwrap_or("/");
                if is_upgrade {
                    if Some(path) == refuse_upgrade_path {
                        let r = "HTTP/1.1 426 Upgrade Required\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
                        let _ = wr.write_all(r.as_bytes()).await;
                        return;
                    }
                    let r = "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: TESTKEY\r\n\r\n";
                    let _ = wr.write_all(r.as_bytes()).await;
                    return;
                }
                let body = format!("{name}:{req}", req = request_line.trim());
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

fn pin_cookie(resp: &http::Response<axum::body::Body>) -> Option<String> {
    resp.headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

#[tokio::test]
async fn pin_sticks_to_one_backend() {
    let ha = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hb = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_upstream("A", ha.clone(), None).await;
    let ab = spawn_upstream("B", hb.clone(), None).await;
    let pool = Arc::new(Pool::new(
        vec![Target::Tcp(aa), Target::Tcp(ab)],
        3,
    ));
    let proxy = Proxy::new(pool);
    let peer = "127.0.0.1".parse().unwrap();

    let r1 = proxy.handle(peer, get("/api/x", None)).await;
    assert_eq!(r1.status(), 200);
    let pin = pin_cookie(&r1).expect("first response pins");
    assert!(pin.starts_with("hb_route="), "got: {pin}");

    // Three pinned requests land on the SAME backend.
    for _ in 0..3 {
        let r = proxy.handle(peer, get("/api/x", Some(&pin))).await;
        assert_eq!(r.status(), 200);
    }
    let (na, nb) = (ha.lock().unwrap().len(), hb.lock().unwrap().len());
    assert_eq!((na, nb), (4, 0), "all four hit one backend");
}

#[tokio::test]
async fn bad_pin_falls_back_and_repins() {
    let ha = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_upstream("A", ha.clone(), None).await;
    let proxy = Proxy::new(Arc::new(Pool::new(vec![Target::Tcp(aa)], 3)));
    let peer = "127.0.0.1".parse().unwrap();
    let r = proxy.handle(peer, get("/api/x", Some("hb_route=7"))).await;
    assert_eq!(r.status(), 200, "bad pin serves, not 503");
    let pin = pin_cookie(&r).expect("re-pinned");
    assert!(pin.starts_with("hb_route=0"), "got: {pin}");
}

#[tokio::test]
async fn ejected_pin_falls_back() {
    let ha = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hb = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_upstream("A", ha.clone(), None).await;
    let ab = spawn_upstream("B", hb.clone(), None).await;
    let pool = Arc::new(Pool::new(vec![Target::Tcp(aa), Target::Tcp(ab)], 1));
    let proxy = Proxy::new(pool.clone());
    let peer = "127.0.0.1".parse().unwrap();
    let r1 = proxy.handle(peer, get("/api/x", None)).await;
    let pin = pin_cookie(&r1).unwrap();
    let idx: usize = pin["hb_route=".len()..].split(';').next().unwrap().parse().unwrap();
    // Eject the pinned backend outright.
    pool.probe(idx, false);
    let r2 = proxy.handle(peer, get("/api/x", Some(&pin))).await;
    assert_eq!(r2.status(), 200);
    let pin2 = pin_cookie(&r2).expect("re-pinned away");
    let idx2: usize = pin2["hb_route=".len()..].split(';').next().unwrap().parse().unwrap();
    assert_ne!(idx, idx2, "must move off the ejected backend");
}

#[tokio::test]
async fn upgrade_tunnel_passes_101_through() {
    let ha = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_upstream("A", ha.clone(), None).await;
    let proxy = Proxy::new(Arc::new(Pool::new(vec![Target::Tcp(aa)], 3)));
    let peer = "127.0.0.1".parse().unwrap();
    let r = http::Request::builder()
        .method("GET")
        .uri("http://balancer/ws")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(axum::body::Body::empty())
        .unwrap();
    let r = proxy.handle(peer, r).await;
    assert_eq!(r.status(), 101);
    assert_eq!(
        r.headers().get("sec-websocket-accept").unwrap(),
        "TESTKEY"
    );
}

#[tokio::test]
async fn refused_upgrade_forwards_status_without_body() {
    let ha = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aa = spawn_upstream("A", ha.clone(), Some("/nope")).await;
    let proxy = Proxy::new(Arc::new(Pool::new(vec![Target::Tcp(aa)], 3)));
    let peer = "127.0.0.1".parse().unwrap();
    let r = http::Request::builder()
        .method("GET")
        .uri("http://balancer/nope")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .body(axum::body::Body::empty())
        .unwrap();
    let r = proxy.handle(peer, r).await;
    assert_eq!(r.status(), 426);
}
