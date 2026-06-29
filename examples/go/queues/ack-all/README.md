# Go — Queues: Ack All

Sends 5 messages to a queue, then drains it atomically with `POST /ce/queue/ack_all`.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./queues/ack-all/main.go
```

## Expected Output

```
Sent 5 messages to queue 'go-ce-queues.ack-all'.
Peek: 5 messages in queue.
ack_all: is_error=false message=OK
After ack_all: 0 messages remaining.
```

## What's Happening

- 5 messages are sent, then a peek confirms they are queued.
- `POST /ce/queue/ack_all?channel=...&client_id=...&wait_timeout=5` atomically discards all pending messages.
- A final peek confirms the queue is empty.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.sent | Event classification (reverse-DNS) |
| source | kubemq-ce-go-example | ClientID for KubeMQ |
| subject | go-ce-queues.ack-all | KubeMQ channel name |
| datacontenttype | application/json | Data format |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — receive individually
- [queues/peek](../peek/) — inspect without consuming
