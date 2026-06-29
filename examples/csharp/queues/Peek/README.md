# C# — Queues: Peek

Sends 3 messages to a queue, peeks twice without consuming (`is_peek=true`), then performs a normal receive to confirm messages were not removed by peeking.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/queues/Peek
dotnet run
```

## Expected Output

```
Sending 3 messages to queue...
  Sent message 1: status=Accepted
  Sent message 2: status=Accepted
  Sent message 3: status=Accepted
[peek #1] messages_received=3
[peek #2] messages_received=3
[consume] messages_received=3
```

## What's Happening

- Three CloudEvents are sent to `POST /ce/queue/send` as structured JSON using `JsonEventFormatter`.
- Two peek requests are made via `POST /ce/queue/receive?...&is_peek=true` — messages are inspected but remain in the queue.
- A final normal receive (without `is_peek`) consumes the messages, removing them from the queue.
- This demonstrates that peek is non-destructive: the same 3 messages appear in both peeks and the final consume.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.peek | Classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-queues.peek | Channel |

## Related Examples

- [queues/BasicSendReceive](../BasicSendReceive/) — simple send and receive
- [queues/AckAll](../AckAll/) — drain queue atomically
