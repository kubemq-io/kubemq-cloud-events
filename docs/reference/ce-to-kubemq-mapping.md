# CE Attribute → KubeMQ Mapping

## Attribute Mapping Table

| CloudEvent Attribute | KubeMQ Tag Key | Required | Notes |
|---------------------|----------------|----------|-------|
| `specversion` | `ce_specversion` | Yes | Always "1.0" |
| `type` | `ce_type` | Yes | Application-defined event type |
| `source` | `ce_source` | Yes | Also used as `ClientID` |
| `id` | `ce_id` | Yes (auto-gen) | Also `EventID`/`RequestID`/`MessageID` |
| `subject` | `ce_subject` | No | Primary channel resolution source |
| `time` | `ce_time` | No (auto-gen) | RFC3339Nano format |
| `datacontenttype` | `ce_datacontenttype` | No | e.g., "application/json" |
| `dataschema` | `ce_dataschema` | No | URI of the data schema |
| *(any extension)* | `ce_{name}` | No | Extension attributes |

## Auto-Generation

The server auto-generates missing attributes before processing:
- `id`: UUID v4 if empty
- `time`: Current UTC time (RFC3339Nano) if zero

## Round-Trip Fidelity

Messages sent via the CE connector and received by a CE subscriber retain all original CE attributes. The mapper reconstructs the CloudEvent from `ce_*` tags on outbound delivery.

## Extension Attribute Handling

CloudEvent extension attributes follow the same `ce_` prefix convention:

```json
{"myextension": "value"}
```
→ stored as `ce_myextension` tag in KubeMQ
→ reconstructed as `{"myextension": "value"}` on delivery

## data vs data_base64

On outbound delivery:
- Body is valid JSON → `data` attribute (inline)
- Body is binary/non-JSON → `data_base64` attribute (base64-encoded string)

## CESQL Routing Scope

CESQL expressions reference CloudEvent attribute names **without** the `ce_` prefix:
```sql
type = 'com.example.order.created'   -- NOT ce_type
source = 'my-service'                -- NOT ce_source
```
