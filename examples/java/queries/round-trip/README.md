# Java — Queries: Round-Trip

Demonstrates an RPC query round-trip. The sender asks for inventory data; the responder subscribes via SSE, looks up the SKU, and returns the result as a CloudEvent response. The sender blocks until the response arrives.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/queries/round-trip
mvn compile exec:java
```

## Expected Output

```
[responder] query sku=WIDGET-100 qty=42
[responder] response sent.
[sender] sending query for sku=WIDGET-100...
[sender] query response: status=200 is_error=false
[sender] response data: {sku=WIDGET-100, quantity=42}
```

## What's Happening

1. A virtual thread subscribes to `GET /ce/subscribe/queries?channel=java-ce-queries.round-trip` via SSE.
2. The main thread sends a query via `POST /ce/send/query` — this call **blocks** until a response is received or the timeout expires.
3. The subscriber receives the query via SSE; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The subscriber looks up inventory data, builds a response CloudEvent, and POSTs it to `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response to the pending query call and unblocks the sender with the data payload.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (query) | com.kubemq.examples.queries.inventory-check | Classification |
| source (query) | kubemq-ce-java-sender | ClientID |
| subject (query) | java-ce-queries.round-trip | Channel |
| type (response) | com.kubemq.examples.queries.inventory-result | Response classification |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [commands/round-trip](../../commands/round-trip/) — same pattern without data response
