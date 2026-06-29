# JavaScript/TypeScript — Commands: Round-Trip

Demonstrates a full RPC command round-trip with KubeMQ CloudEvents. A sender posts a command; a responder subscribes via SSE using `EventSource`, processes the command, and sends back an execution acknowledgement.

## Prerequisites

- Node.js 18+, `npm install` in `examples/javascript/`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/javascript
npm install
npx tsx commands/round-trip/index.ts
```

## Expected Output

```
[sender] sending command...
[responder] command received: type=com.kubemq.examples.commands.reboot request_id=<uuid>
[responder] response sent: is_error=false
[sender] command ack: status=202 is_error=false
```

## What's Happening

1. An `EventSource` subscribes to `GET /ce/subscribe/commands?channel=js-ce-commands.round-trip`.
2. The main flow sends a command via `POST /ce/send/command` using `fetch()` -- this call **blocks** until a response is received or the timeout expires.
3. The `EventSource` receives the command via the `cloudevent` SSE event; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The responder constructs a response CloudEvent and sends it via `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response to the pending command call and unblocks the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (command) | com.kubemq.examples.commands.reboot | Classification |
| source (command) | kubemq-ce-js-sender | ClientID |
| subject (command) | js-ce-commands.round-trip | Channel |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [queries/round-trip](../../queries/round-trip/) — same pattern with data response
