# C# — Queues: Ack All

Sends 5 messages to a queue, then drains it atomically with `POST /ce/queue/ack_all`. Verifies the queue is empty afterwards using peek.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/queues/AckAll
dotnet run
```

## Expected Output

```
Sending 5 messages to queue...
Peek before ack_all: 5 messages
ack_all: is_error=False message=OK
Peek after ack_all: 0 messages remaining
```

## What's Happening

- Five CloudEvents are sent to `POST /ce/queue/send` using `JsonEventFormatter`.
- A peek confirms all 5 messages are in the queue.
- `POST /ce/queue/ack_all?channel=...&client_id=...&wait_timeout=5` atomically acknowledges (removes) all messages.
- A second peek confirms the queue is now empty.
- This is useful for draining dead-letter queues or resetting test state.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.ackall | Classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-queues.ack-all | Channel |

## Related Examples

- [queues/BasicSendReceive](../BasicSendReceive/) — simple send and receive
- [queues/Peek](../Peek/) — inspect without consuming
