# Python — Events-Store: Basic

Publishes to an events-store channel and subscribes with `StartNewOnly` to receive only new messages. Events-store messages are persisted by KubeMQ and can be replayed later.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python events_store/basic/main.py
```

## Expected Output

```
Published to events-store: status=202
Received: type=com.kubemq.examples.eventsstore.stored data={'msg': 'hello events-store from Python!'}
```

## What's Happening

- A background thread subscribes to `GET /ce/subscribe/events-store?events_store_type=1` (`StartNewOnly`).
- The main thread sends a CloudEvent via `POST /ce/send/event-store` using `to_structured()` — the server persists the message.
- The subscriber receives the message via SSE and the program exits.
- Unlike regular events, this message is durable and can be replayed (see replay examples).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-events-store.basic | Channel |

## Related Examples

- [events_store/replay_from_first](../replay_from_first/) — replay all stored events
- [events_store/replay_at_sequence](../replay_at_sequence/) — replay from sequence N
