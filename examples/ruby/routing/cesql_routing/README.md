# Ruby — Routing: CESQL Routing

Demonstrates KubeMQ server-side CESQL routing. Events published to a source channel are automatically routed to destination channels based on CE attribute expressions.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled AND CESQL routing configured (see Go routing example for config)

## How to Run

```bash
cd examples/ruby
bundle install
ruby routing/cesql_routing/main.rb
```

## Expected Output (with routing configured)

```
CESQL Routing Example — Ruby
Requires KubeMQ with CESQL routing configured.

Published type=com.kubemq.examples.routing.order (status=202)
Published type=com.kubemq.examples.routing.alert (status=202)
Published type=com.kubemq.examples.routing.info (status=202)
  [order-archive] type=com.kubemq.examples.routing.order
  [alert-stream] type=com.kubemq.examples.routing.alert
  [all-events] type=com.kubemq.examples.routing.order
  [all-events] type=com.kubemq.examples.routing.alert
  [all-events] type=com.kubemq.examples.routing.info

CESQL routing demonstration complete.
```

## What's Happening

- Three subscriber threads listen on destination channels (`order-archive`, `alert-stream`, `all-events`).
- Three events with different `type` values are published to the `routing-source` channel.
- CESQL routing rules on the server match event types and forward to destination channels.
- Events matching `type = 'com.kubemq.examples.routing.order'` route to `order-archive`.
- Events matching `type LIKE 'com.kubemq.examples.routing.%'` match ALL three events and route to `all-events`.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.routing.* | CESQL expression match target |
| source | urn:kubemq-ce-ruby-cesql | ClientID |
| subject | routing-source | Source channel |

## Related Examples

- [docs/guides/cesql-routing.md](../../../../docs/guides/cesql-routing.md) — full CESQL guide
