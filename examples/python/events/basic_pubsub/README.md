# Python — Events: Basic Pub/Sub

Demonstrates fire-and-forget event pub/sub using the KubeMQ CloudEvents HTTP connector.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python events/basic_pubsub/main.py
```

## Expected Output

```
Published: status=202 is_error=False
Received event:
  type:    com.kubemq.examples.events.sent
  source:  kubemq-ce-python-example
  subject: python-ce-events.basic-pubsub
  data:    {'message': 'Hello from Python CloudEvents example!'}
```

## What's Happening

- A background thread opens `GET /ce/subscribe/events` with `requests` streaming.
- The main thread builds a `CloudEvent` using the `cloudevents` SDK and serializes it with `to_structured()`.
- The event is POSTed with `Content-Type: application/cloudevents+json`.
- The SSE thread parses `event:` / `data:` lines from the streamed response.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.sent | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-events.basic-pubsub | Channel |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/consumer_group](../consumer_group/) — load-balanced subscriber groups
- [events/content_modes](../content_modes/) — structured vs binary mode
- [events_store/basic](../../events_store/basic/) — persistent events with replay
