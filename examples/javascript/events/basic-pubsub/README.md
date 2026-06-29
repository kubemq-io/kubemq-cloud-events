# JavaScript/TypeScript — Events: Basic Pub/Sub

Demonstrates fire-and-forget event pub/sub using the KubeMQ CloudEvents HTTP connector.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/javascript
npm install
npx tsx events/basic-pubsub/index.ts
```

## Expected Output

```
Published: status=202 is_error=false
Received event:
  type:    com.kubemq.examples.events.sent
  source:  kubemq-ce-js-example
  subject: js-ce-events.basic-pubsub
  data:    {"message":"Hello from JavaScript/TypeScript CloudEvents example!"}
```

## What's Happening

- Uses the `eventsource` npm package for the SSE client (browser-compatible EventSource API).
- `CloudEvent` from `cloudevents` SDK, `HTTP.structured(event)` serializes to structured mode.
- `EventSource.addEventListener('cloudevent', ...)` receives KubeMQ CE messages.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.sent | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-events.basic-pubsub | Channel |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/consumer-group](../consumer-group/) — load-balanced subscriber groups
- [events/content-modes](../content-modes/) — structured vs binary mode
- [events-store/basic](../../events-store/basic/) — persistent events with replay
