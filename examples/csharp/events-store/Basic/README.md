# C# — Events-Store: Basic

Publishes to an events-store channel and subscribes with `StartNewOnly` (`events_store_type=1`) to receive only new messages. Events-store messages are persisted by KubeMQ and can be replayed later.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/events-store/Basic
dotnet run
```

## Expected Output

```
Published to events-store: status=Accepted
Received: type=com.kubemq.examples.eventsstore.stored data={"msg":"hello events-store from C#!"}
```

## What's Happening

- An async task subscribes to `GET /ce/subscribe/events-store?events_store_type=1` (`StartNewOnly`) via SSE using `HttpClient.SendAsync` with `ResponseHeadersRead`.
- The main task builds a CloudEvent using `JsonEventFormatter` and POSTs to `POST /ce/send/event-store` — the server persists the message.
- The subscriber receives the message via SSE and the program exits.
- Unlike regular events, this message is durable and can be replayed (see replay examples).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-events-store.basic | Channel |

## Related Examples

- [events-store/ReplayFromFirst](../ReplayFromFirst/) — replay all stored events
- [events-store/ReplayAtSequence](../ReplayAtSequence/) — replay from sequence N
- [events-store/ReconnectResume](../ReconnectResume/) — resume after disconnect
