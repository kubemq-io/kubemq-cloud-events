# Go — Routing: CESQL Routing

Demonstrates KubeMQ server-side CESQL routing. Events published to a source channel are automatically routed to destination channels based on CE attribute expressions.

## Prerequisites

- Go 1.21+
- KubeMQ server with CE enabled AND routing configured (see below)

## Server Configuration Required

Add routing rules to KubeMQ TOML config:

```toml
[Routing]
  Enable = true
  Data = '[{"key":"type = '\''com.kubemq.examples.routing.order'\''","keyType":"cesql","routes":"events:order-archive"},{"key":"type = '\''com.kubemq.examples.routing.alert'\''","keyType":"cesql","routes":"events:alert-stream"},{"key":"type LIKE '\''com.kubemq.examples.routing.%'\''","keyType":"cesql","routes":"events:all-events"}]'
```

## How to Run

```bash
go run ./routing/cesql-routing/main.go
```

## Expected Output (with routing configured)

```
Published type=com.kubemq.examples.routing.order to channel=routing-source (status=202)
Published type=com.kubemq.examples.routing.alert to channel=routing-source (status=202)
Published type=com.kubemq.examples.routing.info to channel=routing-source (status=202)
  [order-archive] type=com.kubemq.examples.routing.order
  [alert-stream] type=com.kubemq.examples.routing.alert
  [all-events] type=com.kubemq.examples.routing.order
  [all-events] type=com.kubemq.examples.routing.alert
  [all-events] type=com.kubemq.examples.routing.info
```

## What's Happening

- CESQL routing is evaluated server-side against CloudEvent attributes from `ce_*` tags.
- Events matching `type = 'com.kubemq.examples.routing.order'` are routed to `order-archive`.
- Events matching `type LIKE 'com.kubemq.examples.routing.%'` match ALL three events → `all-events`.
- CESQL and regex routing rules coexist in the same routing table.
- Template substitution (`{ce_type}`) can dynamically name destination channels.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.routing.* | CESQL expression match target |
| source | kubemq-ce-go-cesql | ClientID |
| subject | routing-source | Source channel |

## Related Examples

- [docs/guides/cesql-routing.md](../../../../docs/guides/cesql-routing.md) — full CESQL guide
