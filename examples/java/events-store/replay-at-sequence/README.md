# Java — Events-Store: Replay At Sequence

Publishes 5 events then subscribes with `events_store_type=4` (StartAtSequence) and `events_store_value=3` to replay from sequence 3 onwards.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/events-store/replay-at-sequence
mvn compile exec:java
```

## Expected Output

```
Publishing 5 events to events-store...
Published 5 events. Subscribing from sequence 3...
Expecting 3 events from sequence 3:
  Received: seq=3
  Received: seq=4
  Received: seq=5
```

## What's Happening

- Five events are published to `POST /ce/send/event-store` with incrementing sequence data.
- A virtual thread subscribes with `events_store_type=4` and `events_store_value=3` — KubeMQ replays only events from sequence 3 onwards.
- The subscriber reads 3 events (seq 3, 4, 5) via SSE and exits.
- This is useful for catching up from a known checkpoint without replaying the entire history.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-events-store.replay-at-sequence | Channel |

## Related Examples

- [events-store/basic](../basic/) — StartNewOnly subscription
- [events-store/replay-from-first](../replay-from-first/) — replay all from beginning
- [events-store/reconnect-resume](../reconnect-resume/) — resume after disconnect
