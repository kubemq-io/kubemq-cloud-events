# Queues Pattern

## Description

Queues provide **point-to-point, durable** messaging. Messages are stored until consumed. Each message is delivered to exactly one consumer (competing consumers). Supports peek-without-consume and atomic drain.

## Sending: POST /ce/queue/send

```bash
curl -X POST http://localhost:9090/ce/queue/send \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.job.submit",
    "source": "job-scheduler",
    "subject": "work-queue",
    "data": {"job_type": "report", "priority": "high"}
  }'
```

## Receiving: POST /ce/queue/receive

Queue receive is a **control operation** — the request body is ignored; all parameters are query strings:

```bash
curl -X POST "http://localhost:9090/ce/queue/receive?channel=work-queue&client_id=worker-1&max_messages=10&wait_timeout=10"
```

**Query Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `channel` | required | Queue channel name |
| `client_id` | required | Client identifier |
| `max_messages` | 1 | Max messages to receive (1–1000) |
| `wait_timeout` | 5 | Wait timeout in seconds |
| `is_peek` | false | Peek without consuming |

**Response:**
```json
{
  "is_error": false,
  "message": "OK",
  "data": {
    "messages_received": 1,
    "messages": [
      {"specversion": "1.0", "type": "com.example.job.submit", ...}
    ]
  }
}
```

## Peek Mode

Setting `is_peek=true` returns messages without advancing the queue read pointer:

```bash
curl -X POST "http://localhost:9090/ce/queue/receive?channel=work-queue&client_id=inspector&is_peek=true&max_messages=100&wait_timeout=3"
```

## Ack All: POST /ce/queue/ack_all

Atomically drain all pending messages:

```bash
curl -X POST "http://localhost:9090/ce/queue/ack_all?channel=work-queue&client_id=worker-1&wait_timeout=10"
```

## Examples

| Language | Link |
|----------|------|
| Go | [examples/go/queues/](../../examples/go/queues/) |
| Python | [examples/python/queues/](../../examples/python/queues/) |
