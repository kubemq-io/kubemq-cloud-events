# Error Codes

## Response Envelope

All responses (success and error) use the same envelope format:

```json
{
  "is_error": false,
  "message": "OK",
  "data": { ... }
}
```

Error responses:
```json
{
  "is_error": true,
  "message": "descriptive error message",
  "data": null
}
```

## HTTP Status Code Table

| Status Code | Meaning | When |
|-------------|---------|------|
| 200 OK | Success | Query response, queue receive, queue ack_all |
| 202 Accepted | Success | Event send, event-store send, command, queue send, response send |
| 400 Bad Request | Invalid input | Invalid CloudEvent, missing required params, validation failure, reserved channel name, subscription error |
| 415 Unsupported Media Type | Unknown content type | Content-Type not recognized as CloudEvent |
| 429 Too Many Requests | Rate limited | SSE connection limit (`MaxSSEConnections`) exceeded |
| 500 Internal Server Error | Server error | Backend messaging error, SSE setup failure |
| 503 Service Unavailable | Timeout | Synchronous request exceeded `TimeoutSeconds` |

## Common Error Messages

| Message | Cause | Solution |
|---------|-------|---------|
| `channel is required` | No `subject` attr and no `?channel=` param | Set CE `subject` attribute |
| `stream idle timeout` | No messages for `MaxSSEIdleSeconds` | Reconnect; consider lowering `MaxSSEIdleSeconds` |
| `invalid CloudEvent` | Malformed CE JSON or missing required attributes | Check `specversion`, `type`, `source` are present |
| `context deadline exceeded` | Request exceeded `TimeoutSeconds` | Increase timeout or ensure responder is active |
| `connection limit exceeded` | `MaxSSEConnections` reached | Increase limit or reduce concurrent SSE connections |
| `reserved channel name` | Channel name conflicts with internal KubeMQ channel | Use a different channel name |
