//! Health probing: GET /api/ready per backend on a tick; 2xx = healthy.
//! Results feed Pool::probe (flap damping both directions lives there).

use std::sync::Arc;
use std::time::Duration;

use crate::pool::{Pool, Target};

/// One probe: true when the backend answers 2xx on /api/ready within budget.
pub async fn probe_once(target: &Target) -> bool {
    let timeout = Duration::from_secs(5);
    match target {
        Target::Tcp(addr) => {
            let url = format!("http://{addr}/api/ready");
            match tokio::time::timeout(timeout, reqwest_get(&url)).await {
                Ok(true) => true,
                _ => false,
            }
        }
        #[cfg(unix)]
        Target::Sock(sock) => {
            // ponytail: raw HTTP over the socket, no client facade — one
            // request, one status line, nothing else. 12 lines beats a dep.
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = match tokio::time::timeout(
                timeout,
                tokio::net::UnixStream::connect(sock),
            )
            .await
            {
                Ok(Ok(s)) => s,
                _ => return false,
            };
            let req = b"GET /api/ready HTTP/1.1\r\nhost: balancer\r\nconnection: close\r\n\r\n";
            if tokio::time::timeout(timeout, stream.write_all(req))
                .await
                .is_err()
            {
                return false;
            }
            let mut buf = [0u8; 64];
            match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
                Ok(Ok(n)) => {
                    let head = String::from_utf8_lossy(&buf[..n]);
                    head.starts_with("HTTP/1.1 2") || head.starts_with("HTTP/1.0 2")
                }
                _ => false,
            }
        }
        #[cfg(not(unix))]
        Target::Sock(_) => false,
    }
}

async fn reqwest_get(url: &str) -> bool {
    // ponytail: std-only TCP probe (same shape as the sock arm — no HTTP
    // client dep for a status line). Connect + GET + 2xx check.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // Strip scheme AND path: connect needs bare host:port, not a URL.
    let addr = url
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or("");
    let mut stream = match tokio::net::TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(_) => return false,
    };
    let req = b"GET /api/ready HTTP/1.1\r\nhost: balancer\r\nconnection: close\r\n\r\n";
    if stream.write_all(req).await.is_err() {
        return false;
    }
    let mut buf = [0u8; 64];
    match stream.read(&mut buf).await {
        Ok(n) => {
            let head = String::from_utf8_lossy(&buf[..n]);
            head.starts_with("HTTP/1.1 2") || head.starts_with("HTTP/1.0 2")
        }
        Err(_) => false,
    }
}

/// Tick loop: probe every backend each interval until cancelled.
pub async fn run(pool: Arc<Pool>, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        // Snapshot targets first (pool borrows end before awaits).
        let n = pool.len();
        for i in 0..n {
            // Reach in without holding anything across the probe.
            let target = pool.target_at(i);
            if let Some(t) = target {
                let ok = probe_once(&t).await;
                pool.probe(i, ok);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn ok_server() -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 512];
                    let _ = s.read(&mut buf).await;
                    let _ = s
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                        .await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn probe_marks_200_healthy_and_refused_down() {
        let addr = ok_server().await;
        assert!(probe_once(&Target::Tcp(addr)).await);
        // Nothing listens here.
        assert!(!probe_once(&Target::Tcp("127.0.0.1:1".into())).await);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn probe_marks_missing_socket_down() {
        assert!(!probe_once(&Target::Sock("/nonexistent-hb.sock".into())).await);
    }
}
