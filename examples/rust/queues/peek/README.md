# Rust — Queues: Peek

Sends 3 messages to a queue, then uses `is_peek=true` to inspect them without consuming. Peeks twice to prove messages remain, then consumes normally.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p queues-peek
```

## Expected Output

```
Sending 3 messages to queue...
  Sent message 1: status=202 Accepted
  Sent message 2: status=202 Accepted
  Sent message 3: status=202 Accepted
[peek #1] messages_received=3
[peek #2] messages_received=3
[consume] messages_received=3
```

## What's Happening

- Three messages are sent to `POST /ce/queue/send` as CloudEvents.
- `POST /ce/queue/receive?is_peek=true` inspects messages without advancing the queue read pointer.
- Peeking twice returns the same 3 messages both times.
- A final `POST /ce/queue/receive?is_peek=false` consumes the messages.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.peek | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-queues.peek | Channel |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — basic send & receive
- [queues/ack-all](../ack-all/) — drain queue atomically
