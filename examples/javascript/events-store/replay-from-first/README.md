# JavaScript/TypeScript — Events-Store: Replay From First

Publishes 5 events then subscribes with `StartFromFirst` (`events_store_type=2`) to replay all stored events -- even those published before the subscriber connected.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx events-store/replay-from-first/index.ts
```

## Expected Output

```
Published event 1/5
...
Published event 5/5

Subscribing StartFromFirst — expecting 5 events:
  [1/5] data={"seq":1}
  ...
  [5/5] data={"seq":5}

All stored events replayed.
```

## What's Happening

- Five events are published to `POST /ce/send/event-store` using `HTTP.structured()` before any subscriber connects.
- An `EventSource` then connects with `?events_store_type=2` (StartFromFirst).
- The server immediately starts streaming all stored messages from the beginning of the channel's history.
- The `EventSource` `cloudevent` listener counts received events and closes when all are received.
- This is the key difference from regular events: messages are durable and replayable.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.replay | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-events-store.replay-from-first | Channel |

## Related Examples

- [events-store/replay-at-sequence](../replay-at-sequence/) — start from specific sequence
- [events-store/reconnect-resume](../reconnect-resume/) — resume after disconnect
