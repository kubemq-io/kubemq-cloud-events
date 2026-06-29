# JavaScript/TypeScript — Events: Content Modes

Demonstrates both CloudEvents HTTP Protocol Binding content modes accepted by the KubeMQ CE connector: **structured** (CE attributes + data in one JSON body) and **binary** (CE attributes as `ce-*` HTTP headers, raw data body).

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx events/content-modes/index.ts
```

## Expected Output

```
Sending in structured mode:
[structured] status=202 is_error=false

Sending in binary mode:
[binary]     status=202 is_error=false

Both content modes accepted.
```

## What's Happening

- **Structured mode**: `HTTP.structured(event)` serializes the entire CloudEvent as a single JSON object with `Content-Type: application/cloudevents+json`.
- **Binary mode**: `HTTP.binary(event)` places CE attributes in `ce-*` HTTP headers and puts only the raw data in the body.
- Both modes are sent via `fetch()` to the same `POST /ce/send/event` endpoint.
- The KubeMQ server auto-detects the content mode and accepts both interchangeably.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.content-mode | Classification |
| source | kubemq-ce-js-example | ClientID |
| subject | js-ce-events.content-modes | Channel |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single mode send
- [docs/guides/content-modes.md](../../../../docs/guides/content-modes.md) — full guide
