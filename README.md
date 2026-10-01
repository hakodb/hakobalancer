# hakobalancer

Stateless L7 balancer over hakobackend upstreams (TCP + unix socket).
Design: [issue #1](../../issues/1); lease/fencing: [issue #2](../../issues/2)
(needs balancer HA first — not phase 1).

Status: scaffold (config + pool + health). Proxy loop next.

```toml
[dependencies]
hakobalancer = "0.1"
```
