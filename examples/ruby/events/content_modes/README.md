# Ruby — Events: Content Modes

Demonstrates both CloudEvents HTTP content modes: **structured** (CE attributes + data in one JSON body via `CloudEvents::HttpBinding`) and **binary** (CE attributes as `ce-*` HTTP headers, raw data body).

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby events/content_modes/main.rb
```

## Expected Output

```
Sending in structured mode:
[structured] status=202 is_error=false

Sending in binary mode:
[binary]     status=202 is_error=false

Both content modes accepted.
```

## What's Happening

- **Structured mode**: `encode_event(event, structured_format: "json")` serializes the entire CloudEvent as a single JSON body with `Content-Type: application/cloudevents+json`.
- **Binary mode**: CE attributes are placed manually in `ce-*` HTTP headers (`ce-specversion`, `ce-type`, `ce-source`, `ce-id`, `ce-subject`, `ce-time`). The body contains only the raw data with `Content-Type: application/json`.
- The KubeMQ server auto-detects both modes via `ce-specversion` header presence.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.content-mode | Classification |
| source | urn:kubemq-ce-ruby-example | ClientID |
| subject | ruby-ce-events.content-modes | Channel |

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — single mode send
- [docs/guides/content-modes.md](../../../../docs/guides/content-modes.md) — full guide
