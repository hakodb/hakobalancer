//! Unix-socket upstream contract (unix-only): same transparency as TCP,
//! over a socket path. Mirrors proxy_tcp with a socket dummy.

#![cfg(unix)]

use std::sync::Arc;

use hakobalancer::pool::{Pool, Target};
use hakobalancer::proxy::Proxy;

async fn dummy_sock_upstream(
    name: &'static str,
    sock: &std::path::Path,
) -> Arc<std::sync::Mutex<Vec<String>>> {
    let _ = std::fs::remove_file(sock);
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen_c = seen.clone();
    let listener = tokio::net::UnixListener::bind(sock).unwrap();
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
                }
                seen_c.lock().unwrap().push(xff);
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
    seen
}

fn tmp_sock(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hb-uds-{label}-{nanos}.sock"))
}

#[tokio::test]
async fn forwards_over_unix_socket() {
    let sock = tmp_sock("a");
    let seen = dummy_sock_upstream("A", &sock).await;
    let proxy = Proxy::new(Arc::new(Pool::new(
        vec![Target::Sock(sock.to_string_lossy().into_owned())],
        3,
    )));
    let req = http::Request::builder()
        .method("GET")
        .uri("http://balancer/api/ready")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = proxy
        .handle("127.0.0.1".parse().unwrap(), req)
        .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(seen.lock().unwrap().as_slice(), ["127.0.0.1"]);
    let _ = std::fs::remove_file(&sock);
}
