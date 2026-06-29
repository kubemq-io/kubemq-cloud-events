# Python — Events: Content Modes

Demonstrates both CloudEvents HTTP Protocol Binding content modes accepted by the KubeMQ CE connector: **structured** (CE attributes + data in one JSON body) and **binary** (CE attributes as `ce-*` HTTP headers, raw data body).

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python events/content_modes/main.py
```

## Expected Output

```
Sending in structured mode (Content-Type: application/cloudevents+json):
[structured] status=202 is_error=False

Sending in binary mode (ce-* HTTP headers):
[binary]     status=202 is_error=False

Both content modes accepted by KubeMQ CE connector.
```

## What's Happening

- **Structured mode**: `to_structured(event)` returns headers with `Content-Type: application/cloudevents+json` and a JSON body with all CE attributes and data combined.
- **Binary mode**: `to_binary(event)` returns headers with `ce-*` prefixed attributes and a body containing only the raw data with its natural content-type.
- The KubeMQ server auto-detects the content mode from the `Content-Type` header (structured) or the presence of `ce-specversion` header (binary).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.content-mode | Classification |
| source | kubemq-ce-python-example | ClientID |
| subject | python-ce-events.content-modes | Channel |

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — single mode send
- [docs/guides/content-modes.md](../../../../docs/guides/content-modes.md) — full guide
