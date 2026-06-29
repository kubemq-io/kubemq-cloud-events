# CESQL Routing

## What is CESQL?

CESQL (CloudEvents SQL) is an expression language for filtering events based on their attributes. KubeMQ integrates CESQL into its routing table to route messages based on CE attributes rather than (or in addition to) channel name patterns.

## Configuration

CESQL rules are configured in the KubeMQ routing table with `keyType: "cesql"`:

```json
[
  {
    "key": "type = 'com.example.order.created'",
    "keyType": "cesql",
    "routes": "events_store:order-archive;queues:order-processing"
  },
  {
    "key": "source = 'audit-service'",
    "keyType": "cesql",
    "routes": "events_store:{ce_source}-log"
  },
  {
    "key": "type LIKE 'com.example.%' AND source = 'critical-service'",
    "keyType": "cesql",
    "routes": "events:alerts"
  }
]
```

In TOML:
```toml
[Routing]
  Enable = true
  FilePath = "/etc/kubemq/routes.json"
  # Or inline:
  # Data = '[{"key":"type = '\''com.example.order.created'\''","keyType":"cesql","routes":"queues:order-processing"}]'
```

## Supported Operators

| Category | Operators / Functions |
|----------|----------------------|
| Comparison | `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=` |
| Logical | `AND`, `OR`, `NOT` |
| String match | `LIKE` (with `%` wildcard) |
| Existence | `EXISTS` |
| Set | `IN` |
| String functions | `CONCAT`, `LENGTH`, `LOWER`, `UPPER`, `TRIM`, `LEFT`, `RIGHT`, `SUBSTRING` |
| Type checks | `IS_BOOL`, `IS_INT` |
| Math | `+`, `-`, `*`, `/`, `%` |

## Evaluation Scope

CESQL rules evaluate against messages that have `ce_*` tags:
- CE connector messages automatically have `ce_*` tags
- gRPC/REST messages with `ce_specversion` tag are also evaluated
- Messages without `ce_*` tags skip CESQL rules

## Template Substitution

Route destinations support CE attribute placeholders:

| Placeholder | Substituted With |
|-------------|-----------------|
| `{ce_type}` | CE `type` attribute |
| `{ce_source}` | CE `source` attribute |
| `{ce_subject}` | CE `subject` attribute |
| `{ce_id}` | CE `id` attribute |
| `{ce_*}` | Any CE attribute or extension |

Example — route to channel named after event type:
```json
{
  "key": "type LIKE 'com.example.%'",
  "keyType": "cesql",
  "routes": "queues:{ce_type}"
}
```

## Error Behavior

CESQL evaluation uses **fail-open** semantics:
- Expression evaluation failures → rule skipped, warning logged, other rules continue
- Invalid expressions at load time → entry skipped with warning
- Panic protection in CESQL parser

## Coexistence with Regex Rules

CESQL and regex rules coexist in the same routing table. Each message is evaluated against all rules in order.

## Examples

- [examples/go/routing/cesql-routing/](../../examples/go/routing/cesql-routing/) — Go example with config
- [examples/python/routing/cesql_routing/](../../examples/python/routing/cesql_routing/)

## See Also

- [CloudEvents SQL spec](https://github.com/cloudevents/spec/blob/main/cesql/spec.md)
- KubeMQ routing documentation (see `08-routing.md` in server docs)
