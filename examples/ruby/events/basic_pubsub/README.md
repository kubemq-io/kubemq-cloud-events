# Ruby — Events: Basic Pub/Sub

Fire-and-forget event pub/sub using the `cloud_events` gem and `Net::HTTP` streaming for SSE.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby events/basic_pubsub/main.rb
```

## Expected Output

```
Published: status=202 is_error=false
Received event:
  type:    com.kubemq.examples.events.sent
  source:  urn:kubemq-ce-ruby-example
  subject: ruby-ce-events.basic-pubsub
  data:    {"message"=>"Hello from Ruby CloudEvents example!"}
```

## What's Happening

- `Net::HTTP` streaming with `resp.read_body` block reads SSE chunks.
- `CloudEvents::HttpBinding.default.encode_event` with `structured_format:` produces structured mode JSON.
- A `Queue` transfers the received SSE data to the main thread.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.sent | Event classification (reverse-DNS) |
| source | urn:kubemq-ce-ruby-example | ClientID for KubeMQ |
| subject | ruby-ce-events.basic-pubsub | KubeMQ channel name |
| datacontenttype | application/json | Data format |

## Related Examples

- [events/consumer_group](../consumer_group/) — load-balanced subscriber groups
- [events/content_modes](../content_modes/) — structured vs binary mode
- [events_store/basic](../../events_store/basic/) — persistent events with replay
