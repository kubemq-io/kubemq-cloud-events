# JavaScript/TypeScript — Events: Consumer Group

Demonstrates load-balanced subscriber groups (`?group=`). Two subscribers join the same group; when two events are published, each subscriber receives exactly one.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx events/consumer-group/index.ts
```

Override server URL:
```bash
KUBEMQ_CE_URL=http://my-server:9090 npx tsx events/consumer-group/index.ts
```

## Expected Output

```
Published event seq=1 (status=202)
Published event seq=2 (status=202)
[worker-1] received: type=com.kubemq.examples.events.grouped data={"seq":1}
[worker-2] received: type=com.kubemq.examples.events.grouped data={"seq":2}
```

## What's Happening

- Two `EventSource` instances subscribe with `?group=workers` -- the server treats them as a load-balancing pool.
- Two events are published using `HTTP.structured(event)` from the `cloudevents` SDK.
- KubeMQ delivers each event to exactly one subscriber in the group (round-robin).
- Without `?group=`, both subscribers would receive every event (fan-out).
- Each `EventSource` listens for the `cloudevent` SSE event type and also handles `error` events.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.grouped | Event classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-events.consumer-group | Channel |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — fan-out (no group)
- [events/content-modes](../content-modes/) — structured vs binary mode
