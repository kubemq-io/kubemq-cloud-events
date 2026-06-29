# C# — Routing: CESQL Routing

Demonstrates KubeMQ server-side CESQL routing. Events published to a source channel are automatically routed to destination channels based on CE attribute expressions.

## Prerequisites

- .NET 8+
- KubeMQ server with CE enabled AND routing configured (see below)

## Server Configuration Required

Add routing rules to KubeMQ TOML config:

```toml
[Routing]
  Enable = true
  Data = '[{"key":"type = ''com.kubemq.examples.routing.order''","keyType":"cesql","routes":"events:order-archive"},{"key":"type = ''com.kubemq.examples.routing.alert''","keyType":"cesql","routes":"events:alert-stream"},{"key":"type LIKE ''com.kubemq.examples.%''","keyType":"cesql","routes":"events:all-events"}]'
```

## How to Run

```bash
cd examples/csharp/routing/CesqlRouting
dotnet run
```

## Expected Output (with routing configured)

```
CESQL Routing Example — C#
Requires KubeMQ with CESQL routing configured.

Published type=com.kubemq.examples.routing.order (status=Accepted)
Published type=com.kubemq.examples.routing.alert (status=Accepted)
Published type=com.kubemq.examples.routing.info (status=Accepted)
  [order-archive] type=com.kubemq.examples.routing.order
  [alert-stream] type=com.kubemq.examples.routing.alert
  [all-events] type=com.kubemq.examples.routing.order
  [all-events] type=com.kubemq.examples.routing.alert
  [all-events] type=com.kubemq.examples.routing.info

CESQL routing demonstration complete.
```

## What's Happening

- CESQL routing is evaluated server-side against CloudEvent attributes from `ce_*` tags.
- Events matching `type = 'com.kubemq.examples.routing.order'` are routed to `order-archive`.
- Events matching `type = 'com.kubemq.examples.routing.alert'` are routed to `alert-stream`.
- Events matching `type LIKE 'com.kubemq.examples.%'` match ALL three events and are routed to `all-events`.
- Three subscribers on the destination channels collect the routed events via `ConcurrentQueue`.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.routing.* | CESQL expression match target |
| source | urn:kubemq-ce-csharp-cesql | ClientID |
| subject | routing-source | Source channel |

## Related Examples

- [docs/guides/cesql-routing.md](../../../../docs/guides/cesql-routing.md) — full CESQL guide
