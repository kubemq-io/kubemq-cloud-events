# Java — Events-Store: Reconnect & Resume

Demonstrates Last-Event-ID reconnect for events-store. Publishes 4 events, receives 2 on the first connection, records the SSE `id:` field, then reconnects using `Last-Event-ID` header (no `events_store_type` param) to resume from the next sequence.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/events-store/reconnect-resume
mvn compile exec:java
```

## Expected Output

```
Publishing 4 events...
First connection — receiving 2 events:
  Received: seq=1
  Received: seq=2
  Last-Event-ID recorded: 2
Reconnecting with Last-Event-ID=2...
  Resumed: seq=3
  Resumed: seq=4
Reconnect-resume demonstration complete.
```

## What's Happening

- Four events are published to an events-store channel.
- A first SSE connection subscribes and reads 2 events, capturing the `id:` SSE field from each (the KubeMQ sequence number).
- The connection is closed intentionally, simulating a disconnect.
- A second SSE connection is opened with the `Last-Event-ID` HTTP header set to the last recorded ID — no `events_store_type` query param is needed.
- KubeMQ resumes delivery from the next sequence after the `Last-Event-ID`, delivering events 3 and 4.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-events-store.reconnect-resume | Channel |

## Related Examples

- [events-store/basic](../basic/) — StartNewOnly subscription
- [events-store/replay-from-first](../replay-from-first/) — replay all stored events
- [events-store/replay-at-sequence](../replay-at-sequence/) — replay from sequence N
