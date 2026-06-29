# Rust — Events-Store: Replay At Sequence

Publishes 5 events then subscribes with `StartAtSequence` (`events_store_type=4`) and `events_store_value=3` to replay only events from sequence 3 onward.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p events-store-replay-at-sequence
```

## Expected Output

```
Publishing 5 events to events-store...
Published 5 events. Subscribing from sequence 3...
Expecting 3 events from sequence 3:
  Received: seq=3
  Received: seq=4
  Received: seq=5
```

## What's Happening

- Five events are published to `POST /ce/send/event-store`.
- A Tokio task subscribes with `?events_store_type=4&events_store_value=3` via `reqwest` streaming.
- `events_store_type=4` means StartAtSequence — the server streams from the specified sequence number.
- Only events with sequence >= 3 are delivered (3 out of 5).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-events-store.replay-at-sequence | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay from beginning
- [events-store/reconnect-resume](../reconnect-resume/) — resume via Last-Event-ID
