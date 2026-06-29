# Endpoint Reference

## Base URL

`http://localhost:9090` (configurable via `KUBEMQ_CONNECTORS_CE_ENABLE` and port config)

## Endpoint Table

| Method | Path | Purpose | Request Body | Success Status |
|--------|------|---------|--------------|----------------|
| POST | `/ce/send/event` | Fire-and-forget event | CloudEvent (structured or binary) | 202 Accepted |
| POST | `/ce/send/event-store` | Persistent event | CloudEvent (structured or binary) | 202 Accepted |
| POST | `/ce/send/command` | RPC command (blocks) | CloudEvent (structured or binary) | 202 Accepted |
| POST | `/ce/send/query` | RPC query (blocks, data response) | CloudEvent (structured or binary) | 200 OK |
| POST | `/ce/send/response` | Response to command/query | CloudEvent + `?request_id=` | 202 Accepted |
| POST | `/ce/queue/send` | Queue message send | CloudEvent (structured or binary) | 202 Accepted |
| POST | `/ce/queue/receive` | Queue message receive (control) | None (body ignored) | 200 OK |
| POST | `/ce/queue/ack_all` | Queue drain (control) | None (body ignored) | 200 OK |
| GET | `/ce/subscribe/events` | SSE events subscription | — | 200 text/event-stream |
| GET | `/ce/subscribe/events-store` | SSE events-store subscription | — | 200 text/event-stream |
| GET | `/ce/subscribe/commands` | SSE commands subscription | — | 200 text/event-stream |
| GET | `/ce/subscribe/queries` | SSE queries subscription | — | 200 text/event-stream |

## POST Endpoint Timeout

All synchronous POST endpoints enforce `TimeoutSeconds` (default 60s). Exceeding returns HTTP 503.

## SSE Endpoint Notes

- No body — subscription is established via query parameters
- Response is `Content-Type: text/event-stream` (long-lived)
- Not subject to `TimeoutSeconds`
- Subject to `MaxSSEIdleSeconds` (default 300s idle timeout)
- HTTP 429 when `MaxSSEConnections` exceeded

## Common Query Parameters (SSE)

| Parameter | Applies To | Required | Description |
|-----------|-----------|----------|-------------|
| `client_id` | All SSE | Yes | Client identifier |
| `channel` | All SSE | Yes | Channel to subscribe |
| `group` | Events | No | Load-balancing group |
| `events_store_type` | Events-Store | No | Replay start position (1–6) |
| `events_store_value` | Events-Store | No | Value for type 4, 5, 6 |

## Queue Receive / Ack All Parameters

| Parameter | Endpoint | Required | Description |
|-----------|---------|----------|-------------|
| `channel` | Both | Yes | Queue channel |
| `client_id` | Both | Yes | Client identifier |
| `max_messages` | receive | No (default 1) | Max messages (1–1000) |
| `wait_timeout` | Both | No (default 5) | Timeout in seconds |
| `is_peek` | receive | No (default false) | Peek without consuming |
