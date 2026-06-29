# Python — Events-Store: Reconnect & Resume

Demonstrates SSE reconnection with `Last-Event-ID` to resume from the last received sequence. Publishes 6 events, reads the first 3, disconnects, then reconnects to receive the remaining 3.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python events_store/reconnect_resume/main.py
```

## Expected Output

```
Published 6 events.

First connection (reading first 3 events):
  [1] id=1 data={'n': 1}
  [2] id=2 data={'n': 2}
  [3] id=3 data={'n': 3}
Disconnected. Last-Event-ID: 3

Reconnecting with Last-Event-ID=3:
  [1] id=4 data={'n': 4}
  [2] id=5 data={'n': 5}
  [3] id=6 data={'n': 6}

Reconnect-resume complete.
```

## What's Happening

- Six events are published to an events-store channel.
- The first SSE connection uses `?events_store_type=2` (StartFromFirst) and reads 3 events, tracking the SSE `id:` field.
- The connection is closed after 3 events, simulating a disconnect.
- The second connection sends `Last-Event-ID: 3` in the HTTP headers (omitting `events_store_type`), telling the server to resume from the next sequence.
- The server streams only events 4-6, proving that `Last-Event-ID` enables gapless reconnection.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.reconnect | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-events-store.reconnect-resume | Channel |

## Related Examples

- [events_store/replay_from_first](../replay_from_first/) — replay all from beginning
- [events_store/basic](../basic/) — basic events-store pub/sub
