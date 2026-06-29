# Rust — Events-Store: Basic

Publishes to an events-store channel and subscribes with `StartNewOnly` (`events_store_type=1`) to receive only new messages. Events-store messages are persisted by KubeMQ and can be replayed later.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p events-store-basic
```

## Expected Output

```
Published to events-store: status=202 Accepted
Received: type="com.kubemq.examples.eventsstore.stored" data={"msg":"hello events-store from Rust!"}
```

## What's Happening

- A Tokio task subscribes to `GET /ce/subscribe/events-store?events_store_type=1` (`StartNewOnly`) via `reqwest` streaming.
- The publisher sends to `POST /ce/send/event-store` — the server persists the message.
- The subscriber receives the message via SSE and the main task prints the CE attributes.
- Unlike regular events, this message is durable and can be replayed (see replay examples).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-events-store.basic | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay all stored events
- [events-store/replay-at-sequence](../replay-at-sequence/) — replay from sequence N
- [events-store/reconnect-resume](../reconnect-resume/) — Last-Event-ID reconnect
