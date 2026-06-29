# Rust — Events: Basic Pub/Sub

Fire-and-forget event pub/sub using `cloudevents-sdk`, `reqwest`, and `tokio`.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p basic-pubsub
```

## Expected Output

```
Published: status=202 is_error=false
Received event:
  type:    "com.kubemq.examples.events.sent"
  source:  "urn:kubemq-ce-rust-example"
  subject: "rust-ce-events.basic-pubsub"
  data:    {"message":"Hello from Rust CloudEvents example!"}
```

## What's Happening

- A Tokio task opens `reqwest` with `.bytes_stream()` and reads SSE chunks manually.
- `EventBuilderV10` from `cloudevents-sdk` builds the CE, `serde_json::to_string` serializes it.
- `POST /ce/send/event` with `Content-Type: application/cloudevents+json`.
- `tokio::sync::oneshot` transfers the received CE from the subscriber task to the main task.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.sent | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-events.basic-pubsub | Channel |

## Related Examples

- [events/consumer-group](../consumer-group/) — load-balanced subscriber groups
- [events/content-modes](../content-modes/) — structured vs binary mode
- [events-store/basic](../../events-store/basic/) — persistent events with replay
