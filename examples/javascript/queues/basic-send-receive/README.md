# JavaScript/TypeScript — Queues: Basic Send & Receive

Sends 3 messages to a KubeMQ queue, then polls to receive them one at a time.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx queues/basic-send-receive/index.ts
```

## Expected Output

```
Sending 3 messages to queue 'js-ce-queues.basic':
  Sent task 1: status=202 is_error=false
  Sent task 2: status=202 is_error=false
  Sent task 3: status=202 is_error=false

Receiving 3 messages:
  Received: type=com.kubemq.examples.queues.task data={"task_id":1,"task":"process-item"}
  Received: type=com.kubemq.examples.queues.task data={"task_id":2,"task":"process-item"}
  Received: type=com.kubemq.examples.queues.task data={"task_id":3,"task":"process-item"}
```

## What's Happening

- Messages are sent via `POST /ce/queue/send` as CloudEvents using `HTTP.structured()` (structured mode).
- Messages are polled via `POST /ce/queue/receive?channel=...&client_id=...&max_messages=1&wait_timeout=5` using `fetch()`.
- Queue receive is a **control operation** -- the body is ignored, all params are query strings.
- Received CE messages are reconstructed from KubeMQ tags back into CE JSON objects.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.task | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-queues.basic | Channel |

## Related Examples

- [queues/peek](../peek/) — inspect without consuming
- [queues/ack-all](../ack-all/) — drain queue atomically
