# C# — Events-Store: Replay From First

Publishes 3 events to an events-store channel, then subscribes with `events_store_type=2` (StartFromFirst) to replay all stored events from the beginning.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/events-store/ReplayFromFirst
dotnet run
```

## Expected Output

```
Publishing 3 events to events-store...
  Published seq=1
  Published seq=2
  Published seq=3
Replayed 3 events from first:
  seq=1 id=...
  seq=2 id=...
  seq=3 id=...
```

## What's Happening

- Three events are published to `POST /ce/send/event-store` using `JsonEventFormatter` — the server persists each one with an incrementing sequence number.
- An async task subscribes with `events_store_type=2` (StartFromFirst) — KubeMQ replays all stored events from the beginning.
- The subscriber reads 3 events via SSE, prints each sequence, and exits.
- This demonstrates durability: events survive and can be replayed even after the publisher disconnects.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-events-store.replay-from-first | Channel |

## Related Examples

- [events-store/Basic](../Basic/) — StartNewOnly subscription
- [events-store/ReplayAtSequence](../ReplayAtSequence/) — replay from sequence N
- [events-store/ReconnectResume](../ReconnectResume/) — resume after disconnect
