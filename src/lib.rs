//! hakobalancer: stateless L7 balancer over hakobackend upstreams.
//!
//! Design: hakodb/hakobalancer#1 (pool, strategy, health, sticky, TLS),
//! lease/fencing: #2 (needs balancer HA first — NOT this crate's phase 1).
//!
//! Phase 1 scope: TCP + unix-socket upstreams, round-robin, /api/ready
//! healthchecks with flap damping, byte-transparent proxying (streamed
//! bodies, status preserved), X-Forwarded-For sanitized, one request =
//! one backend, /api/admin/reload rejected at the edge (405).
//! Sticky WS/SSE + TLS termination follow once the second backend is
//! routinely live (both exist now: :3005 + :3010).

pub mod config;
pub mod health;
pub mod pool;
pub mod proxy;
pub mod tls;
