# Go — Events-Store: Basic

Publishes to an events-store channel and subscribes with `StartNewOnly` to receive only new messages. Events-store messages are persisted by KubeMQ and can be replayed later.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./events-store/basic/main.go
```

## Expected Output

```
Published to events-store: hello events-store!
Received from events-store:
  type:    com.kubemq.examples.eventsstore.stored
  data:    map[msg:hello events-store!]
```

## What's Happening

- A goroutine subscribes to `GET /ce/subscribe/events-store?events_store_type=1` (`StartNewOnly`).
- The publisher sends to `POST /ce/send/event-store` — the server persists the message.
- The subscriber receives the message via SSE and the program exits.
- Unlike regular events, this message is durable and can be replayed (see replay examples).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | kubemq-ce-go-example | ClientID |
| subject | go-ce-events-store.basic | Channel |

## Related Examples

- [events-store/replay-from-first](../replay-from-first/) — replay all stored events
- [events-store/replay-at-sequence](../replay-at-sequence/) — replay from sequence N
