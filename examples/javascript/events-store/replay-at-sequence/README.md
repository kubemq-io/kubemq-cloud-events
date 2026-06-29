# JavaScript/TypeScript — Events-Store: Replay At Sequence

Publishes 10 events then subscribes with `StartAtSequence` (`events_store_type=4`, `events_store_value=5`) to receive only events with sequence >= 5.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx events-store/replay-at-sequence/index.ts
```

## Expected Output

```
Published event 1/10
...
Published event 10/10

Subscribing StartAtSequence=5 — expecting 6 events:
  [seq=5] data={"seq":5}
  ...
  [seq=10] data={"seq":10}

Received 6 events from sequence 5.
```

## What's Happening

- Ten events are published to `POST /ce/send/event-store` using `HTTP.structured()`.
- An `EventSource` connects with `?events_store_type=4&events_store_value=5` to replay from sequence 5.
- The `cloudevent` event listener uses `evt.lastEventId` to extract the server-assigned SSE `id:` field (sequence number).
- Only events at or after sequence 5 are delivered (6 out of 10).
- The `EventSource` is closed once the expected count is reached.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.seqreplay | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-events-store.replay-at-sequence | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay from beginning
- [events-store/reconnect-resume](../reconnect-resume/) — resume after disconnect
