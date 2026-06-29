# Ruby — Commands: Round-Trip

Demonstrates a full RPC command round-trip. A sender posts a command; a responder subscribes via SSE, processes the command, and sends back an execution acknowledgement.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby commands/round_trip/main.rb
```

## Expected Output

```
[responder] command received: type=com.kubemq.examples.commands.reboot request_id=<uuid>
[responder] response sent: is_error=false
[sender] sending command...
[sender] command ack: status=202 is_error=false
```

## What's Happening

1. A `Thread.new` subscribes to `GET /ce/subscribe/commands?channel=ruby-ce-commands.round-trip` via SSE.
2. The main thread sends a command via `POST /ce/send/command` — this call **blocks** until a response is received or the timeout expires.
3. The subscriber receives the command via SSE; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The subscriber sends a response via `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response and unblocks the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (command) | com.kubemq.examples.commands.reboot | Classification |
| source (command) | urn:kubemq-ce-ruby-sender | ClientID |
| subject (command) | ruby-ce-commands.round-trip | Channel |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [queries/round_trip](../../queries/round_trip/) — same pattern with data response
