# Java — Events: Content Modes

Demonstrates both CloudEvents HTTP Protocol Binding content modes accepted by the KubeMQ CE connector: **structured** (CE attributes + data in one JSON body) and **binary** (CE attributes as `ce-*` HTTP headers, raw data body). Uses the CloudEvents Java SDK for serialization.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/events/content-modes
mvn compile exec:java
```

## Expected Output

```
Sending in structured mode:
[structured] status=202 is_error=false

Sending in binary mode:
[binary]     status=202 is_error=false

Both content modes accepted.
```

## What's Happening

- A CloudEvent is built with `CloudEventBuilder.v1()` and serialized via `JsonFormat` for structured mode.
- For binary mode, the same event's CE attributes are extracted and set as `ce-*` HTTP headers; the raw JSON data is the body with `Content-Type: application/json`.
- Both POSTs go to `POST /ce/send/event` — KubeMQ accepts either content mode interchangeably.
- The response envelope `{is_error, message}` confirms each was accepted.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.content-mode | Event classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-events.content-modes | Channel |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — structured mode only
- [events/consumer-group](../consumer-group/) — load-balanced groups
