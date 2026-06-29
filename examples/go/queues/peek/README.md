# Go — Queues: Peek

Sends 3 messages to a queue, then uses `is_peek=true` to inspect them without consuming. A final normal receive confirms messages are still in the queue.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./queues/peek/main.go
```

## Expected Output

```
Sent 3 messages to queue.

Peeking (is_peek=true) — messages NOT consumed:
[peek #1] messages_received=3

Peeking again — same messages still in queue:
[peek #2] messages_received=3

Normal receive — messages consumed:
[consume] messages_received=3

Peek1=3 Peek2=3 Consumed=3 (peek does not remove messages)
```

## What's Happening

- `POST /ce/queue/receive?is_peek=true` returns messages but does not advance the queue read pointer.
- Two consecutive peeks return the same 3 messages.
- A final receive with `is_peek=false` (default) actually consumes the messages.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.sent | Event classification (reverse-DNS) |
| source | kubemq-ce-go-example | ClientID for KubeMQ |
| subject | go-ce-queues.peek | KubeMQ channel name |
| datacontenttype | application/json | Data format |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — normal consume
- [queues/ack-all](../ack-all/) — drain atomically
