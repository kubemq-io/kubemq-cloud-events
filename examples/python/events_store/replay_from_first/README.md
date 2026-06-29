# Python — Events-Store: Replay From First

Publishes 5 events then subscribes with `StartFromFirst` (`events_store_type=2`) to replay all stored events -- even those published before the subscriber connected.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python events_store/replay_from_first/main.py
```

## Expected Output

```
Published event 1/5
...
Published event 5/5

Subscribing with StartFromFirst — expecting 5 events:
  [1/5] data={'seq': 1}
  ...
  [5/5] data={'seq': 5}

All stored events replayed successfully.
```

## What's Happening

- Five events are published to `POST /ce/send/event-store` using `to_structured()` before any subscriber connects.
- A subscriber then connects with `?events_store_type=2` (StartFromFirst).
- The server immediately starts streaming all stored messages from the beginning of the channel's history.
- The SSE stream is read line-by-line via `requests.get(stream=True)` and `iter_lines()`.
- This is the key difference from regular events: messages are durable and replayable.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.replay | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-events-store.replay-from-first | Channel |

## Related Examples

- [events_store/replay_at_sequence](../replay_at_sequence/) — start from specific sequence
- [events_store/reconnect_resume](../reconnect_resume/) — resume after disconnect
