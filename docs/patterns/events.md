# Events Pattern

## Description

Events are **fire-and-forget** pub/sub messages. Publishers send without waiting for delivery confirmation. All subscribers connected to a channel receive every message (fan-out). Messages are **not persisted** — subscribers must be connected at the time of delivery.

## Publishing: POST /ce/send/event

```bash
curl -X POST http://localhost:9090/ce/send/event \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.order.created",
    "source": "order-service",
    "subject": "orders",
    "datacontenttype": "application/json",
    "data": {"order_id": "12345", "amount": 99.99}
  }'
```

**Response:** HTTP 202 Accepted
```json
{"is_error": false, "message": "OK", "data": {...}}
```

## Subscribing: GET /ce/subscribe/events

```bash
curl -N "http://localhost:9090/ce/subscribe/events?client_id=my-client&channel=orders"
```

**Query Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `client_id` | Yes | Client identifier |
| `channel` | Yes | Channel to subscribe |
| `group` | No | Load-balancing group name |

**SSE Response:**
```
event: cloudevent
data: {"specversion":"1.0","type":"com.example.order.created","source":"order-service",...}

```

## Load-Balancing Groups

Adding `?group=workers` to multiple subscribers creates a pool — each event is delivered to exactly one subscriber in the group (round-robin):

```bash
# Terminal 1
curl -N "http://localhost:9090/ce/subscribe/events?client_id=w1&channel=orders&group=workers"

# Terminal 2
curl -N "http://localhost:9090/ce/subscribe/events?client_id=w2&channel=orders&group=workers"
```

Without `?group=`, both subscribers receive every event (fan-out).

## Examples

| Language | Link |
|----------|------|
| Go | [examples/go/events/](../../examples/go/events/) |
| Python | [examples/python/events/](../../examples/python/events/) |
| JavaScript | [examples/javascript/events/](../../examples/javascript/events/) |
| Java | [examples/java/events/](../../examples/java/events/) |
| C# | [examples/csharp/events/](../../examples/csharp/events/) |
| Ruby | [examples/ruby/events/](../../examples/ruby/events/) |
| Rust | [examples/rust/events/](../../examples/rust/events/) |
