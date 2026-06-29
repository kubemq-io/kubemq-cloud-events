# Python — Queries: Round-Trip

Demonstrates an RPC query round-trip. The sender asks for inventory data; the responder subscribes via SSE, looks up the SKU, and returns the result. The sender blocks until the response arrives.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python queries/round_trip/main.py
```

## Expected Output

```
[sender] sending query for sku=WIDGET-100...
[responder] query sku=WIDGET-100 qty=42 request_id=<uuid>
[responder] response sent.
[sender] query response: status=200 is_error=False
[sender] response data: {..., 'data': {'sku': 'WIDGET-100', 'quantity': 42}}
```

## What's Happening

1. A background thread subscribes to `GET /ce/subscribe/queries?channel=python-ce-queries.round-trip`.
2. The main thread sends a query via `POST /ce/send/query` -- this call **blocks** until a response is received.
3. The responder extracts `_kubemq_request_id` and `_kubemq_reply_channel` from the SSE CE JSON.
4. The responder looks up the requested SKU in a simulated inventory dict and sends a data response via `POST /ce/send/response?request_id={id}`.
5. The sender receives the response with the inventory quantity.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (query) | com.kubemq.examples.queries.inventory-check | Classification |
| source (query) | kubemq-ce-python-sender | ClientID |
| subject (query) | python-ce-queries.round-trip | Channel |
| type (response) | com.kubemq.examples.queries.inventory-result | Response classification |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [commands/round_trip](../../commands/round_trip/) — same pattern without data response
