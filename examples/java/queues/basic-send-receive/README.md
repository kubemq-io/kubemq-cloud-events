# Java — Queues: Basic Send & Receive

Sends 3 CloudEvent messages to a KubeMQ queue, then polls to receive them one at a time.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/queues/basic-send-receive
mvn compile exec:java
```

## Expected Output

```
Sending 3 messages to queue 'java-ce-queues.basic':
  Sent task 1: is_error=false
  Sent task 2: is_error=false
  Sent task 3: is_error=false

Receiving 3 messages:
  Received: type=com.kubemq.examples.queues.task data={task_id=1, task=process-item}
  Received: type=com.kubemq.examples.queues.task data={task_id=2, task=process-item}
  Received: type=com.kubemq.examples.queues.task data={task_id=3, task=process-item}
```

## What's Happening

- Messages are sent via `POST /ce/queue/send` as CloudEvents (structured mode) using `HttpClient`.
- Messages are polled via `POST /ce/queue/receive?channel=...&client_id=...&max_messages=1&wait_timeout=5`.
- Queue receive is a **control operation** — the body is ignored, all params are query strings.
- Received CE messages are reconstructed from KubeMQ tags back into CE JSON objects.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.task | Classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-queues.basic | Channel |

## Related Examples

- [queues/peek](../peek/) — inspect without consuming
- [queues/ack-all](../ack-all/) — drain queue atomically
