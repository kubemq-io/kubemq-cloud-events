# C# — Events: Consumer Group

Demonstrates load-balanced subscriber groups (`?group=`). Two subscribers join the same group; when two events are published, each subscriber receives exactly one — the server distributes messages across the group using round-robin.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/events/ConsumerGroup
dotnet run
```

## Expected Output

```
Published event seq=1 (status=Accepted)
Published event seq=2 (status=Accepted)
[worker-1] type=com.kubemq.examples.events.grouped data={"seq":1}
[worker-2] type=com.kubemq.examples.events.grouped data={"seq":2}
```

## What's Happening

- Two async tasks subscribe via `HttpClient.SendAsync` with `ResponseHeadersRead` to the SSE endpoint with `?group=workers`.
- Two events are published using `JsonEventFormatter.EncodeStructuredModeMessage` for structured mode.
- KubeMQ delivers each event to exactly one subscriber in the group (round-robin).
- `Task.WhenAll` collects results from both subscribers.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.grouped | Event classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-events.consumer-group | Channel |

## Related Examples

- [events/BasicPubSub](../BasicPubSub/) — fan-out (no group)
- [events/ContentModes](../ContentModes/) — structured vs binary mode
