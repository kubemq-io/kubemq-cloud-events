# JavaScript/TypeScript — Events-Store: Basic

Publishes to an events-store channel and subscribes with `StartNewOnly` to receive only new messages. Events-store messages are persisted by KubeMQ and can be replayed later.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx events-store/basic/index.ts
```

## Expected Output

```
Published to events-store: status=202
Received: type=com.kubemq.examples.eventsstore.stored data={"msg":"hello events-store from JS!"}
```

## What's Happening

- An `EventSource` subscribes to `GET /ce/subscribe/events-store?events_store_type=1` (`StartNewOnly`).
- The publisher sends a CloudEvent via `POST /ce/send/event-store` using `HTTP.structured()` -- the server persists the message.
- The `EventSource` receives the message via the `cloudevent` SSE event and the program prints the result.
- Unlike regular events, this message is durable and can be replayed (see replay examples).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-events-store.basic | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay all stored events
- [events-store/replay-at-sequence](../replay-at-sequence/) — replay from sequence N
