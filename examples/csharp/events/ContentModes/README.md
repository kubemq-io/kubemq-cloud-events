# C# — Events: Content Modes

Demonstrates both CloudEvents HTTP Protocol Binding content modes accepted by the KubeMQ CE connector: **structured** (via `JsonEventFormatter.EncodeStructuredModeMessage`) and **binary** (CE attributes as `ce-*` HTTP headers on `ByteArrayContent`).

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/events/ContentModes
dotnet run
```

## Expected Output

```
Sending in structured mode:
[structured] status=Accepted is_error=false

Sending in binary mode:
[binary]     status=Accepted is_error=false

Both content modes accepted.
```

## What's Happening

- A CloudEvent is built with `new CloudEvent { ... }` and serialized via `JsonEventFormatter.EncodeStructuredModeMessage` for structured mode.
- For binary mode, the CE attributes are set as `ce-*` HTTP headers on a `ByteArrayContent` with the raw JSON data body.
- Both POSTs go to `POST /ce/send/event` — KubeMQ accepts either content mode interchangeably.
- The response envelope `{is_error, message}` confirms each was accepted.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.content-mode | Event classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-events.content-modes | Channel |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/BasicPubSub](../BasicPubSub/) — structured mode only
- [events/ConsumerGroup](../ConsumerGroup/) — load-balanced groups
