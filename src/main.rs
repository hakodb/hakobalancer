//! hakobalancer binary: parse config, build pool, health loop, serve.
//! One request = one backend (see proxy); /api/admin/reload never leaves.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::Response;
use axum::routing::any;
use axum::Router;
use clap::Parser;

use hakobalancer::config::BalancerConfig;
use hakobalancer::health;
use hakobalancer::pool::Pool;
use hakobalancer::proxy::Proxy;

#[derive(Parser, Debug)]
#[command(name = "hakobalancer", about = "stateless L7 balancer over hakobackend upstreams")]
struct Args {
    /// Config file (TOML). Absent = defaults (no backends: refuses traffic).
    #[arg(long)]
    config: Option<String>,
}

#[derive(Clone)]
struct AppState {
    proxy: Arc<Proxy>,
}

async fn handle(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response<Body> {
    st.proxy.handle(peer.ip(), req).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let raw = match &args.config {
        Some(p) => std::fs::read_to_string(p)
            .map_err(|e| format!("cannot read config {p}: {e}"))?,
        None => String::new(),
    };
    let cfg = BalancerConfig::parse(&raw).map_err(|e| format!("bad config: {e}"))?;
    hakobalancer::config::validate_routes(&cfg).map_err(|e| format!("bad routes: {e}"))?;
    let pool = Arc::new(Pool::new(cfg.targets(), 3));
    let mut proxy = Proxy::new(pool.clone());
    proxy.with_routing(cfg.routes.clone(), cfg.default_backend);
    let proxy = Arc::new(proxy);
    let interval = std::time::Duration::from_secs(cfg.health_interval_secs.max(1));
    tokio::spawn(health::run(pool.clone(), interval));

    let state = AppState { proxy };
    let app = Router::new()
        .route("/{*path}", any(handle))
        .with_state(state);
    // TLS termination (both files set) or plain HTTP (yesterday default).
    // HSTS only under TLS, same rule as the backends.
    if hakobalancer::tls::tls_pair(&cfg).is_some() {
        let rustls = hakobalancer::tls::load_pair(&cfg)
            .await
            .map_err(|e| format!("[hb] {e}"))?;
        println!("[hb] listening (TLS) on https://{}", cfg.listen);
        let app = app.layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            axum::http::header::STRICT_TRANSPORT_SECURITY,
            axum::http::HeaderValue::from_static("max-age=31536000; includeSubDomains"),
        ));
        axum_server::bind_rustls(
            cfg.listen.parse().map_err(|e| format!("[hb] bad listen: {e}"))?,
            rustls,
        )
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
        println!("[hb] listening on http://{}", cfg.listen);
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;
    }
    Ok(())
}
