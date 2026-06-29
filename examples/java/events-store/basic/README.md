# Java — Events-Store: Basic

Publishes to an events-store channel and subscribes with `StartNewOnly` (`events_store_type=1`) to receive only new messages. Events-store messages are persisted by KubeMQ and can be replayed later.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/events-store/basic
mvn compile exec:java
```

## Expected Output

```
Published to events-store: status=202
Received: type=com.kubemq.examples.eventsstore.stored data={msg=hello events-store from Java!}
```

## What's Happening

- A virtual thread subscribes to `GET /ce/subscribe/events-store?events_store_type=1` (`StartNewOnly`) via `HttpURLConnection`.
- The main thread builds a CloudEvent and POSTs to `POST /ce/send/event-store` — the server persists the message.
- The subscriber receives the message via SSE and the program exits.
- Unlike regular events, this message is durable and can be replayed (see replay examples).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-events-store.basic | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay all stored events
- [events-store/replay-at-sequence](../replay-at-sequence/) — replay from sequence N
- [events-store/reconnect-resume](../reconnect-resume/) — resume after disconnect
