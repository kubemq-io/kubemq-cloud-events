# Content Modes

The KubeMQ CE connector supports both CloudEvents HTTP Protocol Binding content modes.

## Structured Mode

All CE attributes and event data are encoded in a single JSON object. The request uses `Content-Type: application/cloudevents+json`:

```bash
curl -X POST http://localhost:9090/ce/send/event \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.order.created",
    "source": "order-service",
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "subject": "orders",
    "time": "2026-03-29T10:30:00Z",
    "datacontenttype": "application/json",
    "data": {"order_id": "12345", "amount": 99.99}
  }'
```

**Go snippet:**
```go
body, _ := json.Marshal(event) // event is cloudevents.Event
req.Header.Set("Content-Type", "application/cloudevents+json")
```

**Python snippet:**
```python
from cloudevents.v1.conversion import to_structured
headers, body = to_structured(event)
requests.post(url, data=body, headers=dict(headers))
```

## Binary Mode

CE attributes are carried in `ce-*` HTTP headers. The body contains only the raw event data:

```bash
curl -X POST http://localhost:9090/ce/send/event \
  -H "Content-Type: application/json" \
  -H "ce-specversion: 1.0" \
  -H "ce-type: com.example.order.created" \
  -H "ce-source: order-service" \
  -H "ce-id: 550e8400-e29b-41d4-a716-446655440000" \
  -H "ce-subject: orders" \
  -H "ce-time: 2026-03-29T10:30:00Z" \
  -d '{"order_id": "12345", "amount": 99.99}'
```

**Python snippet:**
```python
from cloudevents.v1.conversion import to_binary
headers, body = to_binary(event)
requests.post(url, data=body, headers=dict(headers))
```

**JavaScript snippet:**
```typescript
import { HTTP } from 'cloudevents';
const binary = HTTP.binary(event);
fetch(url, { method: 'POST', headers: binary.headers, body: binary.body });
```

## Auto-Detection

The server uses `cehttp.NewEventFromHTTPRequest` for automatic detection:
- `Content-Type: application/cloudevents+json` → structured mode
- Any other content type with `ce-specversion` header → binary mode
- Neither → HTTP 415 Unsupported Media Type

## When to Use Each Mode

| Structured | Binary |
|-----------|--------|
| Simpler code — one JSON body | Preserves native content-type of data |
| Easier to debug (human-readable) | Better for binary/non-JSON data |
| Single serialization step | HTTP header overhead per attribute |
| Default recommendation | When data is already in native format |

## See Also

- [examples/go/events/content-modes/](../../examples/go/events/content-modes/)
- [CloudEvents HTTP Protocol Binding spec](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/http-protocol-binding.md)
