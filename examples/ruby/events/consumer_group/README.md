# Ruby — Events: Consumer Group

Demonstrates load-balanced subscriber groups (`?group=`). Two subscribers join the same group; when two events are published, each subscriber receives exactly one.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby events/consumer_group/main.rb
```

## Expected Output

```
Published event seq=1 (status=202)
Published event seq=2 (status=202)
[ruby-worker-1] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
[ruby-worker-2] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
```

## What's Happening

- Two `Thread.new` blocks each open `GET /ce/subscribe/events?group=workers` — the server treats them as a load-balancing pool.
- Two events are published via `POST /ce/send/event` using `CloudEvents::HttpBinding.default.encode_event`.
- KubeMQ delivers each event to exactly one subscriber in the group (round-robin).
- Without `?group=`, both subscribers would receive every event (fan-out).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.events.grouped | Event classification |
| source | urn:kubemq-ce-ruby-example | ClientID |
| subject | ruby-ce-events.consumer-group | Channel |

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — fan-out (no group)
- [events/content_modes](../content_modes/) — structured vs binary mode
