# Ruby — Queries: Round-Trip

Demonstrates an RPC query round-trip. A sender posts a query; a responder subscribes via SSE, looks up inventory data, and sends back a data response.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby queries/round_trip/main.rb
```

## Expected Output

```
[responder] query sku=WIDGET-100 qty=42 request_id=<uuid>
[responder] response sent.
[sender] sending query for sku=WIDGET-100...
[sender] query response: status=200 is_error=false
[sender] response data: {"sku":"WIDGET-100","quantity":42}
```

## What's Happening

1. A `Thread.new` subscribes to `GET /ce/subscribe/queries?channel=ruby-ce-queries.round-trip` via SSE.
2. The main thread sends a query via `POST /ce/send/query` — this call **blocks** until a response arrives or timeout expires.
3. The subscriber extracts `_kubemq_request_id` and `_kubemq_reply_channel` from the CE JSON.
4. The subscriber performs a lookup in the `inventory` hash and sends a data response via `POST /ce/send/response?request_id={id}`.
5. KubeMQ correlates the response and returns the data to the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (query) | com.kubemq.examples.queries.inventory-check | Classification |
| source (query) | urn:kubemq-ce-ruby-sender | ClientID |
| subject (query) | ruby-ce-queries.round-trip | Channel |
| type (response) | com.kubemq.examples.queries.inventory-result | Response classification |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [commands/round_trip](../../commands/round_trip/) — same pattern, execution ack only (no data)
