//! TLS termination: pair resolution + rustls config loading.
//!
//! Both files required to enable; half pair = plain HTTP is NOT served
//! (fail closed — half-TLS would downgrade clients silently). Missing or
//! unreadable files = loud boot error, same rule.

use crate::config::BalancerConfig;

/// Resolved cert/key paths, or None when TLS is off (either key absent).
pub fn tls_pair(cfg: &BalancerConfig) -> Option<(String, String)> {
    match (&cfg.tls_cert, &cfg.tls_key) {
        (Some(c), Some(k)) => Some((c.clone(), k.clone())),
        _ => None,
    }
}

/// Load a rustls server config from the pair. Errors fail boot loudly.
pub async fn load_pair(
    cfg: &BalancerConfig,
) -> Result<axum_server::tls_rustls::RustlsConfig, String> {
    let (cert, key) = tls_pair(cfg).ok_or("TLS misconfigured (unreachable)")?;
    for (label, p) in [("tls_cert", &cert), ("tls_key", &key)] {
        std::fs::metadata(p).map_err(|_| format!("{label} unreadable: {p}"))?;
    }
    axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
        .await
        .map_err(|e| format!("TLS failed to load: {e}"))
}
