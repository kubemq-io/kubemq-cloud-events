# Go — Events: Basic Pub/Sub

Demonstrates fire-and-forget event publish/subscribe using the KubeMQ CloudEvents HTTP connector. A subscriber opens an SSE stream; a publisher sends one CloudEvent; the subscriber prints it and the program exits.

## Prerequisites

- Go 1.21+
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/go
go mod download
go run ./events/basic-pubsub/main.go
```

Override server URL:
```bash
KUBEMQ_CE_URL=http://my-server:9090 go run ./events/basic-pubsub/main.go
```

## Expected Output

```
Published: status=202 is_error=false
Received event:
  type:    com.kubemq.examples.events.sent
  source:  kubemq-ce-go-example
  subject: go-ce-events.basic-pubsub
  data:    map[message:Hello from Go CloudEvents example!]
```

## What's Happening

- A goroutine opens `GET /ce/subscribe/events?client_id=...&channel=go-ce-events.basic-pubsub` — this is a long-lived SSE HTTP connection.
- The main goroutine waits 500ms for the SSE connection to establish, then sends a CloudEvent via `POST /ce/send/event` with `Content-Type: application/cloudevents+json` (structured mode).
- The server routes the event to all subscribers on the channel.
- The SSE goroutine reads lines from the response body, parses `event:` and `data:` SSE fields, and sends the JSON payload to a channel.
- When the main goroutine receives the event, it unmarshals the CE JSON and prints the attributes.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.sent | Event classification (reverse-DNS) |
| source | kubemq-ce-go-example | ClientID for KubeMQ |
| subject | go-ce-events.basic-pubsub | KubeMQ channel name |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/consumer-group](../consumer-group/) — load-balanced subscriber groups
- [events/content-modes](../content-modes/) — structured vs binary mode
- [events-store/basic](../../events-store/basic/) — persistent events with replay
