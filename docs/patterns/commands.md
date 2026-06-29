# Commands Pattern

## Description

Commands implement **RPC with execution acknowledgement**. The sender blocks until a responder processes the command and sends back a confirmation. If no responder replies within `TimeoutSeconds`, the server returns an error.

## Sending a Command: POST /ce/send/command

The call **blocks** until a response is received or the timeout expires:

```bash
curl -X POST http://localhost:9090/ce/send/command \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.device.reboot",
    "source": "control-plane",
    "subject": "device-commands",
    "data": {"device_id": "sensor-42"}
  }'
```

## Subscribing to Commands: GET /ce/subscribe/commands

```bash
curl -N "http://localhost:9090/ce/subscribe/commands?client_id=device-handler&channel=device-commands"
```

When a command arrives, the SSE CE JSON includes correlation fields:

```json
{
  "specversion": "1.0",
  "type": "com.example.device.reboot",
  "source": "control-plane",
  "_kubemq_request_id": "abc-123-def-456",
  "_kubemq_reply_channel": "device-commands",
  "data": {"device_id": "sensor-42"}
}
```

## Sending a Response: POST /ce/send/response

```bash
curl -X POST "http://localhost:9090/ce/send/response?request_id=abc-123-def-456" \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.device.reboot.result",
    "source": "device-agent",
    "subject": "device-commands",
    "data": {"status": "rebooting"}
  }'
```

**Key rules:**
- `request_id` query parameter: value from `_kubemq_request_id`
- `subject`: must equal `_kubemq_reply_channel`

## Timeout

The server enforces `TimeoutSeconds` (default 60s). Exceeding the timeout returns HTTP 503.

## Examples

| Language | Link |
|----------|------|
| Go | [examples/go/commands/round-trip/](../../examples/go/commands/round-trip/) |
| Python | [examples/python/commands/round_trip/](../../examples/python/commands/round_trip/) |
