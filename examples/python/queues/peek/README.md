# Python — Queues: Peek

Sends 3 messages to a queue, then uses `is_peek=true` to inspect them without consuming. A final normal receive confirms messages are still in the queue.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python queues/peek/main.py
```

## Expected Output

```
Sent 3 messages.

[peek #1] messages_received=3
[peek #2] messages_received=3
[consume] messages_received=3
```

## What's Happening

- `POST /ce/queue/receive?is_peek=true` returns messages but does not advance the queue read pointer.
- Two consecutive peeks return the same 3 messages.
- A final receive with `is_peek=false` (default) actually consumes the messages.
- All queue operations use query-string parameters via `requests.post(params=...)`.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.peek | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-queues.peek | Channel |

## Related Examples

- [queues/basic_send_receive](../basic_send_receive/) — normal consume
- [queues/ack_all](../ack_all/) — drain atomically
