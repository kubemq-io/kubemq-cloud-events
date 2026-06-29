# Java — Events: Consumer Group

Demonstrates load-balanced subscriber groups (`?group=`). Two subscribers join the same group; when two events are published, each subscriber receives exactly one — the server distributes messages across the group using round-robin.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/events/consumer-group
mvn compile exec:java
```

## Expected Output

```
Published event seq=1 (status=202)
Published event seq=2 (status=202)
[java-worker-1] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
[java-worker-2] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
```

## What's Happening

- Two virtual threads subscribe via `HttpURLConnection` to the SSE endpoint with `?group=workers` — the server treats them as a load-balancing pool.
- Two events are published using `HttpClient` with structured CE JSON.
- KubeMQ delivers each event to exactly one subscriber in the group (round-robin).
- Without `?group=`, both subscribers would receive every event (fan-out).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.grouped | Event classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-events.consumer-group | Channel |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — fan-out (no group)
- [events/content-modes](../content-modes/) — structured vs binary mode
