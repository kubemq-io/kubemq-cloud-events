# Python — Queues: Ack All

Sends 5 messages to a queue, peeks to confirm, then uses `POST /ce/queue/ack_all` to drain all messages atomically without receiving them individually.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python queues/ack_all/main.py
```

## Expected Output

```
Sent 5 messages to queue 'python-ce-queues.ack-all'.
Peek: 5 messages in queue.
ack_all: is_error=False message=OK
After ack_all: 0 messages remaining.
```

## What's Happening

- Five messages are sent to the queue via `POST /ce/queue/send`.
- A peek (`is_peek=true`) confirms 5 messages are present.
- `POST /ce/queue/ack_all?channel=...&client_id=...&wait_timeout=5` removes all messages atomically.
- A second peek confirms the queue is now empty.
- This is useful for draining dead-letter queues or resetting test state.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.ackall | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-queues.ack-all | Channel |

## Related Examples

- [queues/basic_send_receive](../basic_send_receive/) — normal consume
- [queues/peek](../peek/) — inspect without consuming
