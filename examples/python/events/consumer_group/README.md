# Python — Events: Consumer Group

Demonstrates load-balanced subscriber groups (`?group=`). Two subscribers join the same group; when two events are published, each subscriber receives exactly one.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python events/consumer_group/main.py
```

Override server URL:
```bash
KUBEMQ_CE_URL=http://my-server:9090 python events/consumer_group/main.py
```

## Expected Output

```
Published event seq=1 (status=202)
Published event seq=2 (status=202)
[worker-1] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
[worker-2] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
```

## What's Happening

- Two background threads subscribe with `?group=workers` — the server treats them as a load-balancing pool.
- Two events are published to the same channel using `to_structured()` for structured-mode CE serialization.
- KubeMQ delivers each event to exactly one subscriber in the group (round-robin).
- Without `?group=`, both subscribers would receive every event (fan-out).
- A `threading.Event` stop flag allows graceful shutdown of SSE reader threads.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.grouped | Event classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-events.consumer-group | Channel |

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — fan-out (no group)
- [events/content_modes](../content_modes/) — structured vs binary mode
