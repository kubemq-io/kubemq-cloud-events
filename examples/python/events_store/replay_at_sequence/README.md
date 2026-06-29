# Python — Events-Store: Replay At Sequence

Publishes 10 events then subscribes with `StartAtSequence` (`events_store_type=4`, `events_store_value=5`) to receive only events with sequence >= 5.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python events_store/replay_at_sequence/main.py
```

## Expected Output

```
Published event 1/10
...
Published event 10/10

Subscribing with StartAtSequence=5:
  [seq=5] data={'seq': 5}
  [seq=6] data={'seq': 6}
  ...
  [seq=10] data={'seq': 10}

Received 6 events starting from sequence 5.
```

## What's Happening

- Ten events are published to `POST /ce/send/event-store` using `to_structured()`.
- The subscriber connects with `?events_store_type=4&events_store_value=5` to replay from sequence 5.
- SSE `id:` fields carry the server-assigned sequence number for each message.
- Only events at or after sequence 5 are delivered (6 out of 10).
- The SSE stream is parsed line-by-line tracking `id:`, `event:`, and `data:` fields.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.seqreplay | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-events-store.replay-at-sequence | Channel |

## Related Examples

- [events_store/replay_from_first](../replay_from_first/) — replay from beginning
- [events_store/reconnect_resume](../reconnect_resume/) — resume after disconnect
