# Channel Resolution

## Priority Order

The KubeMQ channel is resolved in priority order:

1. **CE `subject` attribute** — if the CloudEvent has a `subject` field, it is used directly as the channel name
2. **`?channel=` query parameter** — fallback if `subject` is absent

If neither is provided, the server returns HTTP 400 Bad Request.

## Using subject (Recommended)

```bash
curl -X POST http://localhost:9090/ce/send/event \
  -H "Content-Type: application/cloudevents+json" \
  -d '{"specversion":"1.0","type":"...","source":"...","subject":"my-channel","data":{}}'
```

The `subject` attribute is part of the standard CloudEvents spec and carries semantic meaning (what the event is about). Using it for channel resolution makes the event self-describing.

## Using ?channel= Query Parameter

```bash
curl -X POST "http://localhost:9090/ce/send/event?channel=my-channel" \
  -H "Content-Type: application/cloudevents+json" \
  -d '{"specversion":"1.0","type":"...","source":"...","data":{}}'
```

Use when:
- The CloudEvent schema requires `subject` for a different purpose
- Integrating with external CE producers that don't set `subject`

## HTTP 400 Error

When neither `subject` nor `?channel=` is provided:

```json
{"is_error": true, "message": "channel is required", "data": null}
```

## Best Practices

- **Prefer `subject`** for semantic clarity — it makes the CE self-describing
- Use `?channel=` only for compatibility with external CE sources
- Avoid setting `subject` to different values than the intended channel — downstream CESQL routing reads CE attributes
