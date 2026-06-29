# C# — Commands: Round-Trip

Demonstrates a full RPC command round-trip with KubeMQ CloudEvents. A sender posts a command; a responder subscribes via SSE using `async/await`, processes the command, and sends back an execution acknowledgement via `TaskCompletionSource`.

## Prerequisites

- .NET 8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/csharp/commands/RoundTrip
dotnet run
```

## Expected Output

```
[responder] command received: type=com.kubemq.examples.commands.reboot request_id=...
[responder] response sent: is_error=false
[sender] sending command...
[sender] command ack: status=Accepted is_error=false
```

## What's Happening

1. An async task subscribes to `GET /ce/subscribe/commands?channel=csharp-ce-commands.round-trip` via SSE using `HttpClient.SendAsync` with `ResponseHeadersRead`.
2. The main task sends a command via `POST /ce/send/command` — this call **blocks** until a response is received or the timeout expires.
3. The subscriber receives the command via SSE; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The subscriber sends a response via `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response to the pending command call and unblocks the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (command) | com.kubemq.examples.commands.reboot | Classification |
| source (command) | urn:kubemq-ce-csharp-sender | ClientID |
| subject (command) | csharp-ce-commands.round-trip | Channel |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [queries/RoundTrip](../../queries/RoundTrip/) — same pattern with data response
