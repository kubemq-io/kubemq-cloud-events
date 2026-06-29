# Rust — Commands: Round-Trip

Demonstrates a full RPC command round-trip. A sender posts a command; a responder subscribes via SSE using Tokio async tasks and `oneshot` channels, processes the command, and sends back an execution acknowledgement.

## Prerequisites

- Rust 1.75+ (stable), KubeMQ server with CE enabled

## How to Run

```bash
cd examples/rust
cargo run -p round-trip-commands
```

## Expected Output

```
[responder] command received: type="com.kubemq.examples.commands.reboot" request_id=<uuid>
[responder] response sent: is_error=false
[sender] sending command...
[sender] command ack: status=202 is_error=false
```

## What's Happening

1. A Tokio task subscribes to `GET /ce/subscribe/commands?channel=rust-ce-commands.round-trip` via `reqwest` streaming.
2. The main task sends a command via `POST /ce/send/command` — this call **blocks** until a response is received or the timeout expires.
3. The subscriber receives the command via SSE; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The subscriber sends a response via `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response and unblocks the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (command) | com.kubemq.examples.commands.reboot | Classification |
| source (command) | urn:kubemq-ce-rust-sender | ClientID |
| subject (command) | rust-ce-commands.round-trip | Channel |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [queries/round-trip](../../queries/round-trip/) — same pattern with data response
