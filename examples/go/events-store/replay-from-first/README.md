# Go — Events-Store: Replay From First

Publishes 5 events then subscribes with `StartFromFirst` (`events_store_type=2`) to replay all stored events — even those published before the subscriber connected.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./events-store/replay-from-first/main.go
```

## Expected Output

```
Published event 1 of 5
...
Published event 5 of 5

Subscribed with StartFromFirst — replaying all 5 events:
  [1/5] data=map[seq:1]
  ...
  [5/5] data=map[seq:5]

All stored events replayed successfully.
```

## What's Happening

- Five events are published to `POST /ce/send/event-store` before any subscriber connects.
- A subscriber then connects with `?events_store_type=2` (StartFromFirst).
- The server immediately starts streaming all stored messages from the beginning of the channel's history.
- This is the key difference from regular events: messages are durable and replayable.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.replay | Classification |
| source | kubemq-ce-go-example | ClientID |
| subject | go-ce-events-store.replay-from-first | Channel |

## Related Examples

- [events-store/replay-at-sequence](../replay-at-sequence/) — start from specific sequence
- [events-store/reconnect-resume](../reconnect-resume/) — resume after disconnect
