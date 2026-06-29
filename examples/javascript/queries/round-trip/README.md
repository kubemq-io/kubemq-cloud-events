# JavaScript/TypeScript — Queries: Round-Trip

Demonstrates an RPC query round-trip. The sender asks for inventory data; the responder subscribes via SSE using `EventSource`, looks up the SKU, and returns the result. The sender blocks until the response arrives.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx queries/round-trip/index.ts
```

## Expected Output

```
[sender] sending query for sku=WIDGET-100...
[responder] query sku=WIDGET-100 qty=42 request_id=<uuid>
[responder] response sent.
[sender] query response: status=200 is_error=false
[sender] response data: {...,"data":{"quantity":42,"sku":"WIDGET-100"}}
```

## What's Happening

1. An `EventSource` subscribes to `GET /ce/subscribe/queries?channel=js-ce-queries.round-trip`.
2. The main flow sends a query via `POST /ce/send/query` using `fetch()` -- this call **blocks** until a response is received.
3. The responder extracts `_kubemq_request_id` and `_kubemq_reply_channel` from the SSE CE JSON.
4. The responder looks up the requested SKU in a simulated inventory object and sends a data response via `POST /ce/send/response?request_id={id}`.
5. The sender receives the response with the inventory quantity.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (query) | com.kubemq.examples.queries.inventory-check | Classification |
| source (query) | kubemq-ce-js-sender | ClientID |
| subject (query) | js-ce-queries.round-trip | Channel |
| type (response) | com.kubemq.examples.queries.inventory-result | Response classification |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [commands/round-trip](../../commands/round-trip/) — same pattern without data response
