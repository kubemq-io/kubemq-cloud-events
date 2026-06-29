# JavaScript/TypeScript — Queues: Ack All

Sends 5 messages to a queue, peeks to confirm, then uses `POST /ce/queue/ack_all` to drain all messages atomically without receiving them individually.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx queues/ack-all/index.ts
```

## Expected Output

```
Sent 5 messages to queue 'js-ce-queues.ack-all'.
Peek: 5 messages in queue.
ack_all: is_error=false message=OK
After ack_all: 0 messages remaining.
```

## What's Happening

- Five messages are sent to the queue via `POST /ce/queue/send` using `HTTP.structured()`.
- A peek (`is_peek=true`) confirms 5 messages are present.
- `POST /ce/queue/ack_all?channel=...&client_id=...&wait_timeout=5` removes all messages atomically.
- A second peek confirms the queue is now empty.
- This is useful for draining dead-letter queues or resetting test state.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.ackall | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-queues.ack-all | Channel |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — normal consume
- [queues/peek](../peek/) — inspect without consuming
