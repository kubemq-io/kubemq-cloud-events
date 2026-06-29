# Configuration

## CeConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Enable` | bool | `false` | Enable the CloudEvents connector |
| `TimeoutSeconds` | int | `60` | Timeout for synchronous POST endpoints (seconds) |
| `SubBuffSize` | int | `100` | Buffer size for SSE subscription channels |
| `MaxSSEIdleSeconds` | int | `300` | Max idle time before SSE connection is closed (seconds) |
| `MaxSSEConnections` | int | `0` | Max concurrent SSE connections (0 = unlimited) |

## TOML Example

```toml
[Connectors.CE]
  Enable = true
  TimeoutSeconds = 60
  SubBuffSize = 100
  MaxSSEIdleSeconds = 300
  MaxSSEConnections = 0
```

## Environment Variables

| Variable | Maps To |
|----------|---------|
| `KUBEMQ_CONNECTORS_CE_ENABLE` | `Connectors.CE.Enable` |
| `KUBEMQ_CONNECTORS_CE_TIMEOUTSECONDS` | `Connectors.CE.TimeoutSeconds` |
| `KUBEMQ_CONNECTORS_CE_SUBBUFFSIZE` | `Connectors.CE.SubBuffSize` |
| `KUBEMQ_CONNECTORS_CE_MAXSSEIDLESECONDS` | `Connectors.CE.MaxSSEIdleSeconds` |
| `KUBEMQ_CONNECTORS_CE_MAXSSECONNECTIONS` | `Connectors.CE.MaxSSEConnections` |

## Validation Rules

When `Enable = true`:
- `TimeoutSeconds` must be > 0
- `SubBuffSize` must be > 0 and ≤ 10000
- `MaxSSEIdleSeconds` must be > 0
- `MaxSSEConnections` must be ≥ 0

## Timeout Behavior

`TimeoutSeconds` applies to all synchronous POST endpoints (`/ce/send/*`, `/ce/queue/*`). If a request exceeds the timeout, the server returns HTTP 503 (Service Unavailable). SSE subscription GET endpoints are long-lived and not subject to this timeout.
