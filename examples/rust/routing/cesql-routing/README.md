# Rust — Routing: CESQL Routing

Demonstrates KubeMQ server-side CESQL routing. Events published to a source channel are automatically routed to destination channels based on CE attribute expressions.

## Prerequisites

- Rust 1.75+ (stable)
- KubeMQ server with CE enabled AND CESQL routing configured (see Go routing example for config)

## How to Run

```bash
cd examples/rust
cargo run -p cesql-routing
```

## Expected Output (with routing configured)

```
CESQL Routing Example — Rust
Requires KubeMQ with CESQL routing configured.

Published type=com.kubemq.examples.routing.order (status=202 Accepted)
Published type=com.kubemq.examples.routing.alert (status=202 Accepted)
Published type=com.kubemq.examples.routing.info (status=202 Accepted)
  [order-archive] type="com.kubemq.examples.routing.order"
  [alert-stream] type="com.kubemq.examples.routing.alert"
  [all-events] type="com.kubemq.examples.routing.order"
  ...

CESQL routing demonstration complete.
```

## What's Happening

- Three Tokio tasks subscribe to destination channels (`order-archive`, `alert-stream`, `all-events`) via `reqwest` streaming.
- Three events with different `type` values are published to the `routing-source` channel.
- CESQL routing rules on the server match event types and forward to destination channels.
- Events matching `type = 'com.kubemq.examples.routing.order'` route to `order-archive`.
- Events matching `type LIKE 'com.kubemq.examples.routing.%'` match ALL three events and route to `all-events`.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.routing.* | CESQL expression match target |
| source | urn:kubemq-ce-rust-cesql | ClientID |
| subject | routing-source | Source channel |

## Related Examples

- [docs/guides/cesql-routing.md](../../../../docs/guides/cesql-routing.md) — full CESQL guide
