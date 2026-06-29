# Rust — Queries: Round-Trip

Demonstrates an RPC query round-trip. A sender posts a query; a responder subscribes via SSE using Tokio async tasks and `oneshot` channels, looks up inventory data, and sends back a data response.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p round-trip-queries
```

## Expected Output

```
[responder] query sku=WIDGET-100 qty=42 request_id=...
[responder] response sent: is_error=false
[sender] sending query for sku=WIDGET-100...
[sender] query response: status=200 is_error=false
[sender] response data: {"sku":"WIDGET-100","quantity":42}
```

## What's Happening

1. A Tokio task subscribes to `GET /ce/subscribe/queries?channel=rust-ce-queries.round-trip` via `reqwest` streaming.
2. The main task sends a query via `POST /ce/send/query` — this call **blocks** until a response arrives or timeout expires.
3. The subscriber extracts `_kubemq_request_id` and `_kubemq_reply_channel` from the CE JSON.
4. The subscriber performs a lookup in the `inventory` HashMap and sends a data response via `POST /ce/send/response?request_id={id}`.
5. KubeMQ correlates the response and returns the data to the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (query) | com.kubemq.examples.queries.inventory-check | Classification |
| source (query) | urn:kubemq-ce-rust-sender | ClientID |
| subject (query) | rust-ce-queries.round-trip | Channel |
| type (response) | com.kubemq.examples.queries.inventory-result | Response classification |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [commands/round-trip](../../commands/round-trip/) — same pattern, execution ack only (no data)
