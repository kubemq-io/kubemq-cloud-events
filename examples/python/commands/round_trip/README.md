# Python — Commands: Round-Trip

Demonstrates a full RPC command round-trip with KubeMQ CloudEvents. A sender posts a command; a responder subscribes via SSE, processes the command, and sends back an execution acknowledgement.

## Prerequisites

- Python 3.10+, `pip install -r requirements.txt`
- KubeMQ server running with CE enabled: `KUBEMQ_CONNECTORS_CE_ENABLE=true`

## How to Run

```bash
cd examples/python
pip install -r requirements.txt
python commands/round_trip/main.py
```

## Expected Output

```
[sender] sending command...
[responder] command received: type=com.kubemq.examples.commands.reboot request_id=<uuid>
[responder] response sent: is_error=False
[sender] command ack: status=202 is_error=False
[sender] response data: {...}
```

## What's Happening

1. A background thread subscribes to `GET /ce/subscribe/commands?channel=python-ce-commands.round-trip`.
2. The main thread sends a command via `POST /ce/send/command` -- this call **blocks** until a response is received or the timeout expires.
3. The subscriber receives the command via SSE; the CE JSON contains `_kubemq_request_id` and `_kubemq_reply_channel` fields.
4. The subscriber constructs a response CloudEvent and sends it via `POST /ce/send/response?request_id={_kubemq_request_id}` with `subject = _kubemq_reply_channel`.
5. KubeMQ correlates the response to the pending command call and unblocks the sender.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type (command) | com.kubemq.examples.commands.reboot | Classification |
| source (command) | kubemq-ce-python-sender | ClientID |
| subject (command) | python-ce-commands.round-trip | Channel |
| subject (response) | _kubemq_reply_channel value | Response routing |

## Related Examples

- [queries/round_trip](../../queries/round_trip/) — same pattern with data response
