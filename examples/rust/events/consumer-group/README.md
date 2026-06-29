# Rust — Events: Consumer Group

Demonstrates load-balanced subscriber groups (`?group=`). Two Tokio tasks subscribe with the same group; when two events are published, each subscriber receives exactly one.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p consumer-group
```

## Expected Output

```
Published event seq=1 (status=202 Accepted)
Published event seq=2 (status=202 Accepted)
[worker-1] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
[worker-2] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
```

## What's Happening

- Two Tokio tasks subscribe with `?group=workers` — the server treats them as a load-balancing pool.
- Two events are published to the same channel via `POST /ce/send/event`.
- KubeMQ delivers each event to exactly one subscriber in the group (round-robin).
- Without `?group=`, both subscribers would receive every event (fan-out).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.grouped | Event classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-events.consumer-group | Channel |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — fan-out (no group)
- [events/content-modes](../content-modes/) — structured vs binary mode
