# Go — Queries: Round-Trip

Demonstrates an RPC query round-trip. The sender asks for inventory data; the responder subscribes via SSE, looks up the SKU, and returns the result. The sender blocks until the response arrives.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./queries/round-trip/main.go
```

## Expected Output

```
[sender] sending query for sku=WIDGET-100...
[responder] query received: sku=WIDGET-100 request_id=<uuid>
[responder] response sent.
[sender] query response: status=200 is_error=false
[sender] response data: {...,"data":{"quantity":42,"sku":"WIDGET-100"}}
```

## What's Happening

- Same correlation pattern as commands, but the response carries data (not just an ack).
- `POST /ce/send/query` returns HTTP 200 (not 202) with the responder's CE JSON in `data`.
- The responder constructs a full CloudEvent response with a data payload.
- `_kubemq_request_id` and `_kubemq_reply_channel` are extracted from the received CE JSON.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (query) | com.kubemq.examples.queries.inventory-check | Classification |
| type (response) | com.kubemq.examples.queries.inventory-result | Response type |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [commands/round-trip](../../commands/round-trip/) — same pattern, ack only
