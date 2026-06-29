# Go — Events: Consumer Group

Demonstrates load-balanced subscriber groups (`?group=`). Two subscribers join the same group; when two events are published, each subscriber receives exactly one.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./events/consumer-group/main.go
```

## Expected Output

```
Published event seq=1
Published event seq=2
[worker-1] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
[worker-2] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
```

## What's Happening

- Two goroutines subscribe with `?group=workers` — the server treats them as a load-balancing pool.
- Two events are published to the same channel.
- KubeMQ delivers each event to exactly one subscriber in the group (round-robin).
- Without `?group=`, both subscribers would receive every event (fan-out).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.grouped | Event classification |
| source | kubemq-ce-go-example | ClientID |
| subject | go-ce-events.consumer-group | Channel |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — fan-out (no group)
