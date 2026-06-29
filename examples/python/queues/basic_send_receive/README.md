# Python — Queues: Basic Send & Receive

Sends 3 messages to a KubeMQ queue, then polls to receive them one at a time.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python queues/basic_send_receive/main.py
```

## Expected Output

```
Sending 3 messages to queue 'python-ce-queues.basic':
  Sent task 1: status=202 is_error=False
  Sent task 2: status=202 is_error=False
  Sent task 3: status=202 is_error=False

Receiving 3 messages:
  Received: type=com.kubemq.examples.queues.task data={'task_id': 1, ...}
  Received: type=com.kubemq.examples.queues.task data={'task_id': 2, ...}
  Received: type=com.kubemq.examples.queues.task data={'task_id': 3, ...}
```

## What's Happening

- Messages are sent via `POST /ce/queue/send` as CloudEvents using `to_structured()` (structured mode).
- Messages are polled via `POST /ce/queue/receive?channel=...&client_id=...&max_messages=1&wait_timeout=5`.
- Queue receive is a **control operation** -- the body is ignored, all params are query strings passed via `requests.post(params=...)`.
- Received CE messages are reconstructed from KubeMQ tags back into CE JSON objects.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.task | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-queues.basic | Channel |

## Related Examples

- [queues/peek](../peek/) — inspect without consuming
- [queues/ack_all](../ack_all/) — drain queue atomically
