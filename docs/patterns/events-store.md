# Events-Store Pattern

## Description

Events-store provides **persistent, replayable** pub/sub. Messages are stored durably. Subscribers can replay historical messages using different start positions. Unlike regular events, messages can be delivered to subscribers that connect after the message was published.

## Publishing: POST /ce/send/event-store

```bash
curl -X POST http://localhost:9090/ce/send/event-store \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.audit.entry",
    "source": "audit-service",
    "subject": "audit-log",
    "data": {"action": "user.login", "user": "admin"}
  }'
```

## Subscribing: GET /ce/subscribe/events-store

```bash
# StartNewOnly (default) — only new messages
curl -N "http://localhost:9090/ce/subscribe/events-store?client_id=c1&channel=audit-log&events_store_type=1"

# StartFromFirst — replay all stored messages
curl -N "http://localhost:9090/ce/subscribe/events-store?client_id=c1&channel=audit-log&events_store_type=2"

# StartAtSequence=5 — replay from sequence 5
curl -N "http://localhost:9090/ce/subscribe/events-store?client_id=c1&channel=audit-log&events_store_type=4&events_store_value=5"
```

## Replay Types

| Value | Name | Description |
|-------|------|-------------|
| 1 | StartNewOnly | Only new messages (default) |
| 2 | StartFromFirst | Replay from first stored message |
| 3 | StartFromLast | Start from the last stored message |
| 4 | StartAtSequence | Start at sequence N (`events_store_value=N`) |
| 5 | StartAtTime | Start at Unix timestamp (`events_store_value=timestamp`) |
| 6 | StartAtTimeDelta | Start N seconds back (`events_store_value=N`) |

## SSE Sequence Numbers

The server includes `id: {sequence}` on each SSE frame for events-store subscriptions:

```
id: 42
event: cloudevent
data: {"specversion":"1.0",...}

```

## Last-Event-ID Reconnection

When reconnecting after a disconnect, pass `Last-Event-ID` to resume from the next sequence. **Important:** omit `events_store_type` so the header takes precedence:

```bash
curl -N -H "Last-Event-ID: 42" \
  "http://localhost:9090/ce/subscribe/events-store?client_id=c1&channel=audit-log"
```

## Examples

| Language | Link |
|----------|------|
| Go | [examples/go/events-store/](../../examples/go/events-store/) |
| Python | [examples/python/events_store/](../../examples/python/events_store/) |
| JavaScript | [examples/javascript/events-store/](../../examples/javascript/events-store/) |
