# Queries Pattern

## Description

Queries implement **RPC with data response**. Like commands, but the responder returns a data payload (not just an ack). `POST /ce/send/query` returns HTTP 200 with the responder's CloudEvent in the `data` field.

## Sending a Query: POST /ce/send/query

```bash
curl -X POST http://localhost:9090/ce/send/query \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.inventory.check",
    "source": "web-frontend",
    "subject": "inventory-queries",
    "data": {"sku": "WIDGET-100"}
  }'
```

**Response (HTTP 200):**
```json
{
  "is_error": false,
  "message": "OK",
  "data": {
    "specversion": "1.0",
    "type": "com.example.inventory.result",
    "source": "inventory-service",
    "data": {"sku": "WIDGET-100", "quantity": 42}
  }
}
```

## Subscribing to Queries: GET /ce/subscribe/queries

Same as commands — subscribe and extract `_kubemq_request_id` and `_kubemq_reply_channel`.

## Sending a Response

Same as command response:

```bash
curl -X POST "http://localhost:9090/ce/send/response?request_id={REQUEST_ID}" \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.inventory.result",
    "source": "inventory-service",
    "subject": "{REPLY_CHANNEL}",
    "data": {"sku": "WIDGET-100", "quantity": 42}
  }'
```

## Difference from Commands

| Aspect | Commands | Queries |
|--------|----------|---------|
| Endpoint | `/ce/send/command` | `/ce/send/query` |
| Response code | HTTP 202 | HTTP 200 |
| Response payload | Execution ack | Full data CE |
| Use case | "Execute this action" | "Get me this data" |

## Examples

| Language | Link |
|----------|------|
| Go | [examples/go/queries/round-trip/](../../examples/go/queries/round-trip/) |
