# Java — Commands: Round-Trip

Demonstrates a full RPC command round-trip with KubeMQ CloudEvents. A sender posts a command; a responder subscribes via SSE on a virtual thread, processes the command, and sends back an execution acknowledgement.

## Prerequisites

- Java 21+, Maven 3.8+, KubeMQ server with CE enabled

## How to Run

```bash
cd examples/java/commands/round-trip
mvn compile exec:java
```

## Expected Output

```
[responder] command received: type=com.kubemq.examples.commands.reboot request_id=...
[responder] response sent: is_error=false
[sender] sending command...
[sender] command ack: status=202 is_error=false
```

## What's Happening

1. A virtual thread subscribes to `GET /ce/subscribe/commands?channel=java-ce-commands.round-trip` via SSE using `HttpURLConnection`.
2. The main thread sends a command via `POST /ce/send/command` — this call **blocks** until a response is received or the timeout expires.
3. The subscriber receives the command via SSE; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The subscriber sends a response via `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response to the pending command call and unblocks the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (command) | com.kubemq.examples.commands.reboot | Classification |
| source (command) | kubemq-ce-java-sender | ClientID |
| subject (command) | java-ce-commands.round-trip | Channel |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [queries/round-trip](../../queries/round-trip/) — same pattern with data response
