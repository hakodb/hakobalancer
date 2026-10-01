//! Backend pool: targets + health + round-robin picking.
//!
//! Health model (phase 1): each backend probed on a tick; consecutive
//! failures past the threshold eject it, one success re-admits (flap
//! damping both directions). Picking skips ejected backends; all-ejected
//! fails closed (503, no blind forwarding).

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// One upstream: TCP addr or unix socket path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Tcp(String),
    Sock(String),
}

/// Per-backend health state.
#[derive(Debug)]
pub struct Backend {
    pub target: Target,
    /// Consecutive failed probes.
    pub fails: AtomicU64,
    /// 0 = in rotation, >0 = ejected (stores eject generation for logs).
    pub ejected: AtomicU64,
}

impl Backend {
    pub fn new(target: Target) -> Self {
        Self {
            target,
            fails: AtomicU64::new(0),
            ejected: AtomicU64::new(0),
        }
    }

    pub fn healthy(&self) -> bool {
        self.ejected.load(Ordering::Relaxed) == 0
    }
}

/// Round-robin pool over healthy backends.
pub struct Pool {
    backends: Vec<Backend>,
    cursor: AtomicUsize,
    /// Consecutive failures before eject.
    pub fail_threshold: u64,
}

impl Pool {
    pub fn new(targets: Vec<Target>, fail_threshold: u64) -> Self {
        Self {
            backends: targets.into_iter().map(Backend::new).collect(),
            cursor: AtomicUsize::new(0),
            fail_threshold: fail_threshold.max(1),
        }
    }

    /// Next healthy backend in rotation, or None when all ejected.
    pub fn pick(&self) -> Option<&Backend> {
        let n = self.backends.len();
        if n == 0 {
            return None;
        }
        // ponytail: wrapping cursor, at most one full sweep — ejected
        // backends are skipped, never retried inside pick (probes own that).
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for k in 0..n {
            let b = &self.backends[(start + k) % n];
            if b.healthy() {
                return Some(b);
            }
        }
        None
    }

    /// Record a probe result: eject past threshold, re-admit on success.
    pub fn probe(&self, index: usize, ok: bool) {
        let Some(b) = self.backends.get(index) else {
            return;
        };
        if ok {
            b.fails.store(0, Ordering::Relaxed);
            b.ejected.store(0, Ordering::Relaxed);
        } else {
            let f = b.fails.fetch_add(1, Ordering::Relaxed) + 1;
            if f >= self.fail_threshold {
                b.ejected.store(f, Ordering::Relaxed);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.backends.len()
    }

    /// Target snapshot for probing (cloned; never held across awaits).
    pub fn target_at(&self, index: usize) -> Option<Target> {
        self.backends.get(index).map(|b| b.target.clone())
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }

    /// Healthy count (for status/debug).
    pub fn healthy_count(&self) -> usize {
        self.backends.iter().filter(|b| b.healthy()).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_cycles_and_skips_ejected() {
        let pool = Pool::new(
            vec![
                Target::Tcp("a:1".into()),
                Target::Tcp("b:2".into()),
                Target::Tcp("c:3".into()),
            ],
            2,
        );
        assert_eq!(pool.pick().unwrap().target, Target::Tcp("a:1".into()));
        assert_eq!(pool.pick().unwrap().target, Target::Tcp("b:2".into()));
        // Eject b (2 fails); rotation skips it, cursor continues.
        pool.probe(1, false);
        assert!(pool.pick().is_some());
        pool.probe(1, false);
        assert_eq!(pool.pick().unwrap().target, Target::Tcp("a:1".into()));
        assert_eq!(pool.pick().unwrap().target, Target::Tcp("c:3".into()));
        // One success re-admits.
        pool.probe(1, true);
        assert_eq!(pool.healthy_count(), 3);
    }

    #[test]
    fn all_ejected_fails_closed() {
        let pool = Pool::new(vec![Target::Tcp("a:1".into())], 1);
        assert!(pool.pick().is_some());
        pool.probe(0, false);
        assert!(pool.pick().is_none());
        assert_eq!(pool.healthy_count(), 0);
    }

    #[test]
    fn empty_pool_picks_nothing() {
        let pool = Pool::new(vec![], 3);
        assert!(pool.pick().is_none());
    }
}
