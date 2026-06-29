# Rust — Events: Content Modes

Demonstrates both CloudEvents HTTP content modes: **structured** (`serde_json::to_string(&event)` with `application/cloudevents+json`) and **binary** (CE attributes as `ce-*` HTTP headers, raw JSON data body).

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p content-modes
```

## Expected Output

```
Sending in structured mode:
[structured] status=202 is_error=false

Sending in binary mode:
[binary]     status=202 is_error=false

Both content modes accepted.
```

## What's Happening

- **Structured mode**: `serde_json::to_string(&event)` serializes the entire CloudEvent as a single JSON body; the request uses `Content-Type: application/cloudevents+json`.
- **Binary mode**: CE attributes are placed manually in `ce-*` HTTP headers via `reqwest`'s `.header()` builder. The body contains only the raw data with `Content-Type: application/json`.
- The KubeMQ server auto-detects binary mode when the `ce-specversion` header is present.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.content-mode | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-events.content-modes | Channel |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single mode send
- [docs/guides/content-modes.md](../../../../docs/guides/content-modes.md) — full guide
