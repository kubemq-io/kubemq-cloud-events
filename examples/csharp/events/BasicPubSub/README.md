# C# — Events: Basic Pub/Sub

Fire-and-forget event pub/sub using `CloudNative.CloudEvents` SDK and `HttpClient` with SSE streaming.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/events/BasicPubSub
dotnet run
```

## Expected Output

```
Published: status=Accepted is_error=false
Received event:
  type:    com.kubemq.examples.events.sent
  source:  urn:kubemq-ce-csharp-example
  subject: csharp-ce-events.basic-pubsub
  data:    {"message":"Hello from C# CloudEvents example!"}
```

## What's Happening

- `JsonEventFormatter.EncodeStructuredModeMessage` produces the structured CE JSON body.
- `HttpClient.SendAsync` with `ResponseHeadersRead` opens the SSE stream without buffering.
- `StreamReader.ReadLineAsync` parses SSE fields line by line.
- `TaskCompletionSource<JsonElement>` transfers the received event to the main task.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.sent | Classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-events.basic-pubsub | Channel |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/ConsumerGroup](../ConsumerGroup/) — load-balanced subscriber groups
- [events/ContentModes](../ContentModes/) — structured vs binary mode
- [events-store/Basic](../../events-store/Basic/) — persistent events with replay
