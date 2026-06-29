# C# — Queues: Basic Send & Receive

Sends 3 CloudEvent messages to a KubeMQ queue, then polls to receive them one at a time.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/queues/BasicSendReceive
dotnet run
```

## Expected Output

```
Sending 3 messages to queue 'csharp-ce-queues.basic':
  Sent task 1: is_error=False
  Sent task 2: is_error=False
  Sent task 3: is_error=False

Receiving 3 messages:
  Received: type=com.kubemq.examples.queues.task data={"task_id":1,"task":"process-item"}
  Received: type=com.kubemq.examples.queues.task data={"task_id":2,"task":"process-item"}
  Received: type=com.kubemq.examples.queues.task data={"task_id":3,"task":"process-item"}
```

## What's Happening

- Messages are sent via `POST /ce/queue/send` as CloudEvents (structured mode) using `HttpClient` and `JsonEventFormatter`.
- Messages are polled via `POST /ce/queue/receive?channel=...&client_id=...&max_messages=1&wait_timeout=5`.
- Queue receive is a **control operation** — the body is ignored, all params are query strings.
- Received CE messages are deserialized from JSON using `System.Text.Json`.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.task | Classification |
| source | urn:kubemq-ce-csharp-example | ClientID |
| subject | csharp-ce-queues.basic | Channel |

## Related Examples

- [queues/Peek](../Peek/) — inspect without consuming
- [queues/AckAll](../AckAll/) — drain queue atomically
