# Java — Queues: Ack All

Sends 5 messages to a queue, then drains it atomically with `POST /ce/queue/ack_all`. Verifies the queue is empty afterwards using peek.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/queues/ack-all
mvn compile exec:java
```

## Expected Output

```
Sending 5 messages to queue...
Peek before ack_all: 5 messages
ack_all: is_error=false message=OK
Peek after ack_all: 0 messages remaining
```

## What's Happening

- Five CloudEvents are sent to `POST /ce/queue/send`.
- A peek confirms all 5 messages are in the queue.
- `POST /ce/queue/ack_all?channel=...&client_id=...&wait_timeout=5` atomically acknowledges (removes) all messages.
- A second peek confirms the queue is now empty.
- This is useful for draining dead-letter queues or resetting test state.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.ackall | Classification |
| source | kubemq-ce-java-example | ClientID |
| subject | java-ce-queues.ack-all | Channel |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — simple send and receive
- [queues/peek](../peek/) — inspect without consuming
