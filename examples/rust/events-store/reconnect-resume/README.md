# Rust — Events-Store: Reconnect & Resume

Demonstrates Last-Event-ID reconnect for durable event streams. Receives 2 events recording the SSE `id:` field, then reconnects using `.header("Last-Event-ID", last_id)` (no `events_store_type` param) to resume from the next sequence.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p events-store-reconnect-resume
```

## Expected Output

```
Publishing 4 events...
First connection — receiving 2 events:
  Received: seq=1
  Received: seq=2
  Last-Event-ID recorded: 2
Reconnecting with Last-Event-ID=2...
  Resumed: seq=3
  Resumed: seq=4
Reconnect-resume demonstration complete.
```

## What's Happening

- Four events are published to `POST /ce/send/event-store`.
- First connection uses `?events_store_type=2` (StartFromFirst) and reads 2 events, recording the SSE `id:` field.
- The connection is closed (simulating a disconnect).
- Second connection sends the `Last-Event-ID` HTTP header and **omits** `events_store_type` — the server uses the stored offset to resume from event 3.
- This pattern enables crash-safe consumption of durable event streams.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-events-store.reconnect-resume | Channel |

## Related Examples

- [events-store/basic](../basic/) — StartNewOnly subscription
- [events-store/replay-from-first](../replay-from-first/) — replay all from beginning
