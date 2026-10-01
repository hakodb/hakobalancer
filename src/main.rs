//! hakobalancer binary: parse config, build pool, serve (proxy loop lands
//! in the next commit — this one establishes config + pool + health).

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "hakobalancer", about = "stateless L7 balancer over hakobackend upstreams")]
struct Args {
    /// Config file (TOML). Absent = defaults (no backends: refuses traffic).
    #[arg(long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let raw = match &args.config {
        Some(p) => std::fs::read_to_string(p)
            .map_err(|e| format!("cannot read config {p}: {e}"))?,
        None => String::new(),
    };
    let cfg = hakobalancer::config::BalancerConfig::parse(&raw)
        .map_err(|e| format!("bad config: {e}"))?;
    let pool = hakobalancer::pool::Pool::new(cfg.targets(), 3);
    println!(
        "[hb] listen={} strategy={:?} backends={} health_every={}s",
        cfg.listen, cfg.strategy, pool.len(), cfg.health_interval_secs
    );
    // Proxy loop follows; this commit proves config + pool parse + health.
    Ok(())
}
