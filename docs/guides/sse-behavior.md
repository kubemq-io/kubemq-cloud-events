# SSE Behavior

## Protocol Overview

Server-Sent Events (SSE) is a standard HTTP mechanism for server-to-client streaming. The server responds with `Content-Type: text/event-stream` and sends a continuous stream of text frames. Each frame consists of optional `id:`, `event:`, and `data:` fields separated by a blank line.

```
id: 42
event: cloudevent
data: {"specversion":"1.0","type":"com.example.order","source":"svc",...}

```

## Event Types

| `event:` value | Description |
|----------------|-------------|
| `cloudevent` | CE message (has `ce_*` tags); `data:` is a CE JSON object |
| `message` | Non-CE message; `data:` is plain KubeMQ JSON |
| `error` | Error condition; `data:` is `{"is_error":true,"message":"..."}` |
| *(absent)* | Keepalive comment (`: keepalive`) — not a named event |

## Keepalive

The server sends a keepalive comment every 30 seconds:

```
: keepalive

```

Standard `EventSource` clients ignore comment lines. Manual SSE parsers should check for lines starting with `:` and skip them.

## Idle Timeout

If no messages arrive for `MaxSSEIdleSeconds` (default 300s), the server sends:

```
event: error
data: {"is_error":true,"message":"stream idle timeout"}

```

The connection is then closed. Clients should handle this gracefully and reconnect if needed.

## Connection Limits

When `MaxSSEConnections > 0`, new connections exceeding the limit receive HTTP 429 Too Many Requests. Setting `MaxSSEConnections = 0` (default) disables the limit.

## Last-Event-ID Reconnection (Events-Store Only)

Events-store subscriptions emit `id: {sequence}` on each frame. On reconnect, pass `Last-Event-ID` to resume:

```bash
curl -N -H "Last-Event-ID: 42" \
  "http://localhost:9090/ce/subscribe/events-store?client_id=c1&channel=audit-log"
```

**Important:** Omit `events_store_type` when using `Last-Event-ID` — if both are present, `events_store_type` takes precedence.

Regular events subscriptions do not emit `id:` fields (non-persistent events cannot be replayed).

## Mixed CE and Non-CE Messages

Both CE and non-CE messages can coexist on the same channel. Clients should handle both `event: cloudevent` and `event: message` frames.

## Client Implementation Tips

**Using EventSource API (JavaScript):**
```javascript
const es = new EventSource(url);
es.addEventListener('cloudevent', (e) => {
  const ce = JSON.parse(e.data);
  console.log(ce.type, ce.data);
});
es.addEventListener('error', (e) => {
  if (e.data) console.error(JSON.parse(e.data).message);
});
```

**Manual SSE parsing (all languages):**
```
Read response body line by line:
- Empty line → dispatch current event, reset state
- Line starting with ":" → keepalive, skip
- "event: X" → set event type = X
- "data: Y" → set data = Y
- "id: Z" → set lastEventId = Z
On dispatch: if event type == "cloudevent" → parse JSON; "error" → log and exit
```

## See Also

- [patterns/events-store.md](../patterns/events-store.md) — replay and reconnection
