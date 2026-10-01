//! Live proxy contract (TCP): one request = one backend, byte-transparent
//! method/path/headers/body, status preserved, X-Forwarded-For sanitized,
//! /api/admin/reload rejected at the edge, empty pool fails closed.

use std::sync::Arc;

use hakobalancer::pool::{Pool, Target};
use hakobalancer::proxy::Proxy;

/// Dummy upstream: records the X-Forwarded-For it saw, answers with its
/// name + echoed path. Raw TCP (no framework) so the test proves the
/// balancer speaks plain HTTP/1.1 to anything.
async fn dummy_upstream(name: &'static str) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen_c = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let seen_c = seen_c.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
                let (rd, mut wr) = stream.into_split();
                let mut lines = tokio::io::BufReader::new(rd);
                let mut request_line = String::new();
                if lines.read_line(&mut request_line).await.is_err() {
                    return;
                }
                let mut xff = String::from("-");
                let mut content_len = 0usize;
                loop {
                    let mut h = String::new();
                    if lines.read_line(&mut h).await.unwrap_or(0) == 0 {
                        break;
                    }
                    let t = h.trim_end().to_string();
                    if t.is_empty() {
                        break;
                    }
                    if let Some(v) = t.strip_prefix("x-forwarded-for:") {
                        xff = v.trim().to_string();
                    }
                    if let Some(v) = t.strip_prefix("content-length:") {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                }
                // Drain body (streaming must not deadlock the proxy).
                if content_len > 0 {
                    let mut buf = vec![0u8; content_len];
                    use tokio::io::AsyncReadExt;
                    let _ = lines.read_exact(&mut buf).await;
                    seen_c.lock().unwrap().push(format!("{xff}|{content_len}"));
                } else {
                    seen_c.lock().unwrap().push(xff);
                }
                let body = format!("{name}:{req}", req = request_line.trim());
                let resp = format!(
                    "HTTP/1.1 201 Created\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = wr.write_all(resp.as_bytes()).await;
            });
        }
    });
    (addr, seen)
}

fn req(method: &str, path: &str) -> http::Request<axum::body::Body> {
    http::Request::builder()
        .method(method)
        .uri(format!("http://balancer{path}"))
        .body(axum::body::Body::empty())
        .unwrap()
}

#[tokio::test]
async fn forwards_verbatim_to_one_backend() {
    let (addr, seen) = dummy_upstream("A").await;
    let proxy = Proxy::new(Arc::new(Pool::new(vec![Target::Tcp(addr)], 3)));
    let resp = proxy.handle("127.0.0.1".parse().unwrap(), req("GET", "/api/pages?limit=1")).await;
    assert_eq!(resp.status(), 201);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        ["127.0.0.1"],
        "XFF must carry the real peer, single entry"
    );
}

#[tokio::test]
async fn post_body_streams_without_deadlock() {
    let (addr, seen) = dummy_upstream("A").await;
    let proxy = Proxy::new(Arc::new(Pool::new(vec![Target::Tcp(addr)], 3)));
    let r = http::Request::builder()
        .method("POST")
        .uri("http://balancer/api/collections/c")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(vec![0u8; 64 * 1024]))
        .unwrap();
    let resp = proxy.handle("127.0.0.1".parse().unwrap(), r).await;
    assert_eq!(resp.status(), 201);
    assert_eq!(seen.lock().unwrap().as_slice(), ["127.0.0.1|65536"]);
}

#[tokio::test]
async fn admin_reload_rejected_at_edge() {
    let (addr, _) = dummy_upstream("A").await;
    let proxy = Proxy::new(Arc::new(Pool::new(vec![Target::Tcp(addr)], 3)));
    let resp = proxy
        .handle("127.0.0.1".parse().unwrap(), req("POST", "/api/admin/reload"))
        .await;
    assert_eq!(resp.status(), 405);
}

#[tokio::test]
async fn empty_pool_fails_closed() {
    let proxy = Proxy::new(Arc::new(Pool::new(vec![], 3)));
    let resp = proxy.handle("127.0.0.1".parse().unwrap(), req("GET", "/api/ready")).await;
    assert_eq!(resp.status(), 503);
}

#[tokio::test]
async fn spoofed_xff_is_replaced_not_appended() {
    let (addr, seen) = dummy_upstream("A").await;
    let proxy = Proxy::new(Arc::new(Pool::new(vec![Target::Tcp(addr)], 3)));
    let r = http::Request::builder()
        .method("GET")
        .uri("http://balancer/api/ready")
        .header("x-forwarded-for", "9.9.9.9")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = proxy.handle("127.0.0.1".parse().unwrap(), r).await;
    assert_eq!(resp.status(), 201);
    // Balancer is not behind a sanitizing proxy here: peer IP wins,
    // spoofed value dropped (never "9.9.9.9, 127.0.0.1").
    assert_eq!(seen.lock().unwrap().as_slice(), ["127.0.0.1"]);
}
