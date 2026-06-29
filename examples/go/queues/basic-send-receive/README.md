# Go — Queues: Basic Send & Receive

Sends 3 messages to a KubeMQ queue, then polls to receive them one at a time.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./queues/basic-send-receive/main.go
```

## Expected Output

```
Sending 3 messages to queue 'go-ce-queues.basic':
Sent task 1: status=202 is_error=false
Sent task 2: status=202 is_error=false
Sent task 3: status=202 is_error=false

Receiving 3 messages from queue:
Received: type=com.kubemq.examples.queues.task data=map[...]
Received: type=com.kubemq.examples.queues.task data=map[...]
Received: type=com.kubemq.examples.queues.task data=map[...]
```

## What's Happening

- Messages are sent via `POST /ce/queue/send` as CloudEvents (structured mode).
- Messages are polled via `POST /ce/queue/receive?channel=...&client_id=...&max_messages=1&wait_timeout=5`.
- Queue receive is a **control operation** — the body is ignored, all params are query strings.
- Received CE messages are reconstructed from KubeMQ tags back into CE JSON objects.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.task | Classification |
| source | kubemq-ce-go-example | ClientID |
| subject | go-ce-queues.basic | Channel |

## Related Examples

- [queues/peek](../peek/) — inspect without consuming
- [queues/ack-all](../ack-all/) — drain queue atomically
