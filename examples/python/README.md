# KubeMQ CloudEvents — Python Examples

All Python examples use the official [CloudEvents Python SDK](https://github.com/cloudevents/sdk-python).

## Prerequisites

- Python 3.10+
- KubeMQ server running with CE enabled on `http://localhost:9090`

## Setup

```bash
cd examples/python
pip install -r requirements.txt
# or with uv:
uv pip install -r requirements.txt
```

## Run Any Example

```bash
export KUBEMQ_CE_URL=http://localhost:9090
python events/basic_pubsub/main.py
```

## Examples

| Pattern | Directory |
|---------|-----------|
| Basic Pub/Sub | [events/basic_pubsub/](events/basic_pubsub/) |
| Consumer Group | [events/consumer_group/](events/consumer_group/) |
| Content Modes | [events/content_modes/](events/content_modes/) |
| Events-Store Basic | [events_store/basic/](events_store/basic/) |
| Replay From First | [events_store/replay_from_first/](events_store/replay_from_first/) |
| Replay At Sequence | [events_store/replay_at_sequence/](events_store/replay_at_sequence/) |
| Reconnect & Resume | [events_store/reconnect_resume/](events_store/reconnect_resume/) |
| Queue Send & Receive | [queues/basic_send_receive/](queues/basic_send_receive/) |
| Queue Peek | [queues/peek/](queues/peek/) |
| Queue Ack All | [queues/ack_all/](queues/ack_all/) |
| Command Round-Trip | [commands/round_trip/](commands/round_trip/) |
| Query Round-Trip | [queries/round_trip/](queries/round_trip/) |
| CESQL Routing | [routing/cesql_routing/](routing/cesql_routing/) |
