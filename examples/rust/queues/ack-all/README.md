# Rust — Queues: Ack All

Sends 5 messages, peeks to verify the count, then calls `POST /ce/queue/ack_all` to atomically drain all pending messages. A final peek confirms the queue is empty.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p queues-ack-all
```

## Expected Output

```
Sending 5 messages to queue...
Peek before ack_all: 5 messages
ack_all: is_error=false message="OK"
Peek after ack_all: 0 messages remaining
```

## What's Happening

- Five messages are sent to the queue via `POST /ce/queue/send`.
- A peek (`is_peek=true`) confirms 5 messages are waiting.
- `POST /ce/queue/ack_all?channel=...&client_id=...&wait_timeout=5` atomically discards all pending messages.
- A second peek confirms 0 messages remain.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.ackall | Classification |
| source | urn:kubemq-ce-rust-example | ClientID |
| subject | rust-ce-queues.ack-all | Channel |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — basic send & receive
- [queues/peek](../peek/) — inspect without consuming
