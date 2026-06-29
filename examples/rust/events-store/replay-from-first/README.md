# Rust — Events-Store: Replay From First

Publishes 3 events then subscribes with `StartFromFirst` (`events_store_type=2`) to replay all stored events — even those published before the subscriber connected.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p events-store-replay-from-first
```

## Expected Output

```
Publishing 3 events to events-store...
  Published seq=1
  Published seq=2
  Published seq=3
Replayed 3 events from first:
  seq=1 id="..."
  seq=2 id="..."
  seq=3 id="..."
```

## What's Happening

- Three events are published to `POST /ce/send/event-store` before any subscriber connects.
- A Tokio task then subscribes with `?events_store_type=2` (StartFromFirst) via `reqwest` streaming.
- The server immediately starts streaming all stored messages from the beginning of the channel's history.
- `mpsc::channel` collects events from the subscriber task to the main task.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-events-store.replay-from-first | Channel |

## Related Examples

- [events-store/basic](../basic/) — StartNewOnly subscription
- [events-store/replay-at-sequence](../replay-at-sequence/) — replay from sequence N
