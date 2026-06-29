# Go — Events-Store: Replay At Sequence

Publishes 10 events then subscribes with `StartAtSequence` (`events_store_type=4`, `events_store_value=5`) to receive only events with sequence >= 5.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./events-store/replay-at-sequence/main.go
```

## Expected Output

```
Published event 1/10 ... Published event 10/10

Subscribing with StartAtSequence=5 (expecting events 5-10):
  [seq=5] data=map[seq:5]
  ...
  [seq=10] data=map[seq:10]

Received 6 events starting from sequence 5.
```

## What's Happening

- 10 events are published and persisted.
- A subscriber connects with `?events_store_type=4&events_store_value=5`.
- The server streams only events from sequence 5 onwards.
- The SSE `id:` field carries the KubeMQ sequence number for each message.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.seqreplay | Classification |
| subject | go-ce-events-store.replay-at-sequence | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay from beginning
- [events-store/reconnect-resume](../reconnect-resume/) — use Last-Event-ID
