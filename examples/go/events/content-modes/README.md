# Go — Events: Content Modes

Demonstrates both CloudEvents HTTP Protocol Binding content modes accepted by the KubeMQ CE connector: **structured** (CE attributes + data in one JSON body) and **binary** (CE attributes as `ce-*` HTTP headers, raw data body).

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./events/content-modes/main.go
```

## Expected Output

```
Sending in structured mode (Content-Type: application/cloudevents+json):
[structured] status=202 is_error=false message=OK

Sending in binary mode (ce-* HTTP headers):
[binary]     status=202 is_error=false message=OK

Both content modes accepted by KubeMQ CE connector.
```

## What's Happening

- **Structured mode**: the entire CloudEvent (attributes + data) is serialized as a single JSON object with `Content-Type: application/cloudevents+json`. The CE SDK's `json.Marshal(event)` produces this.
- **Binary mode**: CE attributes (`specversion`, `type`, `source`, `id`, `subject`, `time`) are placed in `ce-*` HTTP request headers. The body contains only the raw data with its natural content-type. The server auto-detects binary mode when `ce-specversion` header is present.
- The KubeMQ server uses the CloudEvents SDK's `cehttp.NewEventFromHTTPRequest` for automatic detection.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.content-mode | Classification |
| source | kubemq-ce-go-example | ClientID |
| subject | go-ce-events.content-modes | Channel |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single mode send
- [docs/guides/content-modes.md](../../../../docs/guides/content-modes.md) — full guide
