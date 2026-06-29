# Architecture

## Overview

The KubeMQ CloudEvents (CE) connector provides a native HTTP interface for all KubeMQ messaging patterns using the [CloudEvents specification](https://cloudevents.io). It runs on the shared HTTP server (default port 9090), alongside the REST, MCP, and A2A connectors.

## Shared HTTP Server

```
Client (CloudEvents HTTP)
         │
         ▼
┌──────────────────────────┐
│  Shared HTTP Server :9090│
│  ┌────────────────────┐  │
│  │ CORS Middleware     │  │
│  │ Auth Middleware     │  │
│  │ Traffic Gate        │  │
│  └────────┬───────────┘  │
│           ▼              │
│  ┌────┬────┬────┬────┐   │
│  │REST│MCP │A2A │ CE │   │
│  └────┴────┴────┴──┬─┘   │
└─────────────────────┼────┘
                      ▼
              Array Service
                      │
              ┌───────┼───────┐
              ▼       ▼       ▼
          Events   Queues    RPC
```

All connectors share the same Echo framework instance, middleware chain, and broker state.

## CE Connector Components

- **handlers.go** — HTTP route registration and request dispatch
- **mapper.go** — CloudEvent ↔ KubeMQ message conversion
- **sse.go** — SSE subscription management, frame serialization

## CE Attribute → KubeMQ Tag Mapping

When a CloudEvent is received, its attributes are stored as KubeMQ message tags with the `ce_` prefix:

| CloudEvent Attribute | KubeMQ Tag Key | Required | Notes |
|---------------------|----------------|----------|-------|
| specversion | ce_specversion | Yes | Always "1.0" |
| type | ce_type | Yes | Application-defined event type |
| source | ce_source | Yes | Also used as `ClientID` |
| id | ce_id | Yes (auto-generated) | Also `EventID`/`RequestID`/`MessageID` |
| subject | ce_subject | No | Primary channel resolution source |
| time | ce_time | No (auto-generated) | RFC3339Nano format |
| datacontenttype | ce_datacontenttype | No | e.g., "application/json" |
| dataschema | ce_dataschema | No | URI of the data schema |
| *(any extension)* | ce_{name} | No | Extension attributes |

## Channel Resolution Flow

```
Incoming CloudEvent
        │
        ▼
  Has subject?
   YES ──────→ use subject as channel name
   NO
        │
        ▼
  Has ?channel= param?
   YES ──────→ use channel param
   NO
        │
        ▼
  HTTP 400 Bad Request
```

## ClientID Resolution Flow

```
Incoming request
        │
        ▼
  Auth enabled AND valid token?
   YES ──────→ use auth claims ClientID (overrides all)
   NO
        │
        ▼
  CE source attribute
        │
        ▼
  Used as KubeMQ ClientID
```

## Auto-Generation

Missing optional attributes are auto-generated before processing:
- **`id`**: New UUID if empty
- **`time`**: Current UTC time if zero

## data vs data_base64

On outbound delivery (SSE or queue receive), the connector checks whether the message body is valid JSON:
- **Valid JSON** → `data` attribute (inline JSON object)
- **Non-JSON** (binary, plain text) → `data_base64` attribute (base64-encoded)

## Mixed CE and Non-CE Messages

SSE subscriptions deliver both CE and non-CE messages. The `event:` field distinguishes them:
- `event: cloudevent` → CE message (reconstructed from `ce_*` tags)
- `event: message` → plain KubeMQ message (no `ce_*` tags)

## Source Code

`connectors/ce/` in the KubeMQ server repository (connector.go, handlers.go, mapper.go, sse.go).
