# JavaScript/TypeScript — Queues: Peek

Sends 3 messages to a queue, then uses `is_peek=true` to inspect them without consuming. A final normal receive confirms messages are still in the queue.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx queues/peek/index.ts
```

## Expected Output

```
Sent 3 messages.

[peek #1] messages_received=3
[peek #2] messages_received=3
[consume] messages_received=3
```

## What's Happening

- `POST /ce/queue/receive?is_peek=true` returns messages but does not advance the queue read pointer.
- Two consecutive peeks return the same 3 messages.
- A final receive with `is_peek=false` (default) actually consumes the messages.
- All queue operations use `fetch()` with query-string parameters in the URL.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.peek | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-queues.peek | Channel |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — normal consume
- [queues/ack-all](../ack-all/) — drain atomically
