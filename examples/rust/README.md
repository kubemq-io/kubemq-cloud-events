# KubeMQ CloudEvents — Rust Examples

All Rust examples use the official [CloudEvents Rust SDK](https://github.com/cloudevents/sdk-rust) with `reqwest` for HTTP and `tokio` async runtime.

## Prerequisites

- Rust 1.75+ (stable)
- Cargo
- KubeMQ server running with CE enabled on `http://localhost:9090`

## Run Any Example

```bash
export KUBEMQ_CE_URL=http://localhost:9090
cargo run --manifest-path examples/rust/events/basic-pubsub/Cargo.toml
```

Or from the workspace root:
```bash
cd examples/rust
KUBEMQ_CE_URL=http://localhost:9090 cargo run -p basic-pubsub
```
