# Go — Events-Store: Reconnect & Resume

Demonstrates SSE reconnection using the `Last-Event-ID` header. After receiving the first 3 of 6 events, the connection is intentionally closed. On reconnect, `Last-Event-ID` tells the server to resume from where it left off.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./events-store/reconnect-resume/main.go
```

## Expected Output

```
Published 6 events to events-store.

First connection (StartFromFirst, reading first 3 events):
  [1] id=1 data=map[n:1]
  [2] id=2 data=map[n:2]
  [3] id=3 data=map[n:3]
Disconnected. Last-Event-ID captured: 3

Reconnecting with Last-Event-ID: 3
Receiving remaining 3 events:
  [1] id=4 data=map[n:4]
  [2] id=5 data=map[n:5]
  [3] id=6 data=map[n:6]

Reconnect-resume demonstration complete.
```

## What's Happening

- Each SSE frame from events-store includes `id: {sequence}`.
- After receiving 3 events, the body is closed (simulating a disconnect).
- The second connection sends `Last-Event-ID: 3` — the server resumes from sequence 4.
- **Important:** When using `Last-Event-ID` for reconnection, omit `events_store_type` from the URL so the header takes precedence.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.reconnect | Classification |
| subject | go-ce-events-store.reconnect-resume | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — explicit StartFromFirst
- [docs/guides/sse-behavior.md](../../../../docs/guides/sse-behavior.md) — SSE reconnection guide
