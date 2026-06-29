# KubeMQ CloudEvents — Documentation

Documentation for the KubeMQ CloudEvents HTTP connector.

## Contents

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Connector architecture, CE→KubeMQ mapping, channel resolution |
| [getting-started.md](getting-started.md) | Docker quick-start, first event in 5 minutes |
| [configuration.md](configuration.md) | Configuration fields, TOML example, env vars |
| **Patterns** | |
| [patterns/events.md](patterns/events.md) | Fire-and-forget pub/sub |
| [patterns/events-store.md](patterns/events-store.md) | Persistent pub/sub, replay |
| [patterns/queues.md](patterns/queues.md) | Point-to-point queues |
| [patterns/commands.md](patterns/commands.md) | RPC commands |
| [patterns/queries.md](patterns/queries.md) | RPC queries |
| **Guides** | |
| [guides/content-modes.md](guides/content-modes.md) | Structured vs binary mode |
| [guides/channel-resolution.md](guides/channel-resolution.md) | Channel name resolution |
| [guides/sse-behavior.md](guides/sse-behavior.md) | SSE protocol, reconnection |
| [guides/cesql-routing.md](guides/cesql-routing.md) | CESQL routing configuration |
| [guides/authentication.md](guides/authentication.md) | JWT/OIDC authentication |
| **Reference** | |
| [reference/endpoints.md](reference/endpoints.md) | Full endpoint reference |
| [reference/ce-to-kubemq-mapping.md](reference/ce-to-kubemq-mapping.md) | Attribute mapping |
| [reference/error-codes.md](reference/error-codes.md) | HTTP status codes, errors |

## Examples

Working code examples in 7 languages are in [../examples/](../examples/README.md).

## Prerequisites

All examples and documentation assume:
- KubeMQ server running with CE connector enabled
- `KUBEMQ_CONNECTORS_CE_ENABLE=true`
- Default port: `9090`
