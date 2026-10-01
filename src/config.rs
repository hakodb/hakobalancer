//! Balancer configuration: file (TOML) + flags, flags win.
//!
//! ```toml
//! listen = "0.0.0.0:8080"
//! strategy = "round_robin"   # only strategy in phase 1
//! health_interval_secs = 10
//! backends = [
//!   { addr = "127.0.0.1:3005" },
//!   { addr = "127.0.0.1:3010" },
//!   # { sock = "/run/hakobackend/hako.sock" },
//! ]
//! ```

use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    #[default]
    RoundRobin,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct BackendDecl {
    /// TCP upstream ("127.0.0.1:3005"). Exactly one of addr/sock.
    #[serde(default)]
    pub addr: Option<String>,
    /// Unix-socket upstream ("/run/hakobackend/hako.sock").
    #[serde(default)]
    pub sock: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BalancerConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default)]
    pub strategy: Strategy,
    #[serde(default = "default_health_interval")]
    pub health_interval_secs: u64,
    #[serde(default)]
    pub backends: Vec<BackendDecl>,
    /// TLS termination pair (both required to enable). Absent = plain
    /// HTTP (yesterday's default). Same certs nginx used work here.
    #[serde(default)]
    pub tls_cert: Option<String>,
    #[serde(default)]
    pub tls_key: Option<String>,
}

fn default_listen() -> String {
    "0.0.0.0:8080".into()
}

fn default_health_interval() -> u64 {
    10
}

impl Default for BalancerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            strategy: Strategy::default(),
            health_interval_secs: default_health_interval(),
            backends: Vec::new(),
            tls_cert: None,
            tls_key: None,
        }
    }
}

impl BalancerConfig {
    /// Parse + validate: every backend needs exactly one of addr/sock.
    pub fn parse(toml_text: &str) -> Result<Self, String> {
        let cfg: Self =
            toml::from_str(toml_text).map_err(|e| format!("config parse: {e}"))?;
        for (n, b) in cfg.backends.iter().enumerate() {
            match (&b.addr, &b.sock) {
                (Some(_), None) | (None, Some(_)) => {}
                _ => {
                    return Err(format!(
                        "backends[{n}]: exactly one of addr/sock is required"
                    ))
                }
            }
        }
        Ok(cfg)
    }

    /// Backend targets in config order (for the pool).
    pub fn targets(&self) -> Vec<crate::pool::Target> {
        self.backends
            .iter()
            .map(|b| {
                if let Some(a) = &b.addr {
                    crate::pool::Target::Tcp(a.clone())
                } else {
                    crate::pool::Target::Sock(b.sock.clone().unwrap_or_default())
                }
            })
            .collect()
    }

    /// Flags/env overlay placeholder (phase 1: file only; keep the merge
    /// point so flags don't require restructuring later).
    pub fn from_file_or_default(path: Option<&str>) -> Result<(Self, HashMap<String, String>), String> {
        let _ = path;
        Ok((Self::default(), HashMap::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_needs_exactly_one_of_addr_sock() {
        assert!(BalancerConfig::parse("backends = [{ addr = \"127.0.0.1:3005\" }]").is_ok());
        assert!(BalancerConfig::parse("backends = [{ sock = \"/x.sock\" }]").is_ok());
        assert!(BalancerConfig::parse("backends = [{}]").is_err());
        assert!(BalancerConfig::parse(
            "backends = [{ addr = \"127.0.0.1:3005\", sock = \"/x.sock\" }]"
        )
        .is_err());
    }

    #[test]
    fn tls_pair_needs_both_files_present() {
        // Absent = plain HTTP (yesterday's default).
        let cfg = BalancerConfig::parse("").unwrap();
        assert!(crate::tls::tls_pair(&cfg).is_none());
        // Half pair = fail closed, never half-TLS.
        let cfg = BalancerConfig::parse("tls_cert = \"/x.pem\"").unwrap();
        assert!(crate::tls::tls_pair(&cfg).is_none());
    }

    #[tokio::test]
    async fn tls_missing_files_fail_loud() {
        // Pointing at missing files = loud boot error, not silent plain.
        let cfg = BalancerConfig::parse(
            "tls_cert = \"/nope.pem\"\ntls_key = \"/nope-key.pem\"",
        )
        .unwrap();
        assert!(crate::tls::load_pair(&cfg).await.is_err());
    }

    #[test]
    fn defaults_are_sane() {
        let cfg = BalancerConfig::parse("").unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:8080");
        assert_eq!(cfg.strategy, Strategy::RoundRobin);
        assert!(cfg.backends.is_empty());
    }
}
