# Go — Commands: Round-Trip

Demonstrates a full RPC command round-trip with KubeMQ CloudEvents. A sender posts a command; a responder subscribes via SSE, processes the command, and sends back an execution acknowledgement.

## Prerequisites

- Go 1.21+, KubeMQ server with CE enabled

## How to Run

```bash
go run ./commands/round-trip/main.go
```

## Expected Output

```
[sender] sending command...
[responder] command received: type=com.kubemq.examples.commands.reboot request_id=<uuid>
[responder] response sent: is_error=false
[sender] command ack received: status=202 is_error=false data={...}
```

## What's Happening

1. A goroutine subscribes to `GET /ce/subscribe/commands?channel=go-ce-commands.round-trip`.
2. The main goroutine sends a command via `POST /ce/send/command` — this call **blocks** until a response is received or the timeout expires.
3. The subscriber receives the command via SSE; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The subscriber sends a response via `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response to the pending command call and unblocks the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (command) | com.kubemq.examples.commands.reboot | Classification |
| source (command) | kubemq-ce-go-sender | ClientID |
| subject (command) | go-ce-commands.round-trip | Channel |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [queries/round-trip](../../queries/round-trip/) — same pattern with data response
