# Java — Events: Basic Pub/Sub

Fire-and-forget event pub/sub using Java HttpURLConnection for SSE and `java.net.http.HttpClient` for publishing.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/events/basic-pubsub
mvn compile exec:java
```

## Expected Output

```
Published: status=202 is_error=false
Received event:
  type:    com.kubemq.examples.events.sent
  source:  kubemq-ce-java-example
  subject: java-ce-events.basic-pubsub
  data:    {message=Hello from Java CloudEvents example!}
```

## What's Happening

- A virtual thread opens `HttpURLConnection` to the SSE endpoint and reads lines via `BufferedReader`.
- The main thread builds a `CloudEvent` using `CloudEventBuilder.v1()` and serializes with `JsonFormat`.
- `HttpClient` POSTs the structured CE JSON.
- The SSE thread parses `event:` / `data:` lines and puts the payload in a `BlockingQueue`.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.sent | Classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-events.basic-pubsub | Channel |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/consumer-group](../consumer-group/) — load-balanced subscriber groups
- [events/content-modes](../content-modes/) — structured vs binary mode
- [events-store/basic](../../events-store/basic/) — persistent events with replay
