# JavaScript/TypeScript — Events-Store: Reconnect & Resume

Demonstrates SSE reconnection with `Last-Event-ID` to resume from the last received sequence. Uses `EventSource` for the initial connection and `fetch` streaming for the reconnect (to support custom headers).

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx events-store/reconnect-resume/index.ts
```

## Expected Output

```
Published 6 events.

First connection (reading first 3 events):
  [1] id=1 data={"n":1}
  [2] id=2 data={"n":2}
  [3] id=3 data={"n":3}
Disconnected. Last-Event-ID: 3

Reconnecting with Last-Event-ID=3:
  [1] id=4 data={"n":4}
  [2] id=5 data={"n":5}
  [3] id=6 data={"n":6}

Reconnect-resume complete.
```

## What's Happening

- Six events are published to an events-store channel.
- The first connection uses `EventSource` with `?events_store_type=2` (StartFromFirst) and reads 3 events, tracking `evt.lastEventId`.
- The `EventSource` is closed after 3 events, simulating a disconnect.
- The second connection uses `fetch()` with a `Last-Event-ID` header (since `EventSource` does not natively support custom headers).
- The `events_store_type` param is omitted on reconnect so `Last-Event-ID` takes precedence.
- The server streams only events 4-6, proving gapless reconnection.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.reconnect | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-events-store.reconnect-resume | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay all from beginning
- [events-store/basic](../basic/) — basic events-store pub/sub
