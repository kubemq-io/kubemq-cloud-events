# Ruby — Events-Store: Basic

Publishes to an events-store channel and subscribes with `StartNewOnly` (`events_store_type=1`) to receive only new messages. Events-store messages are persisted by KubeMQ and can be replayed later.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby events_store/basic/main.rb
```

## Expected Output

```
Published to events-store: status=202
Received: type=com.kubemq.examples.eventsstore.stored data={"msg"=>"hello events-store from Ruby!"}
```

## What's Happening

- A `Thread.new` subscribes to `GET /ce/subscribe/events-store?events_store_type=1` (`StartNewOnly`).
- The publisher sends to `POST /ce/send/event-store` — the server persists the message.
- The subscriber receives the message via SSE and the program exits.
- Unlike regular events, this message is durable and can be replayed (see replay examples).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-ruby-example | ClientID |
| subject | ruby-ce-events-store.basic | Channel |

## Related Examples

- [events_store/replay_from_first](../replay_from_first/) — replay all stored events
- [events_store/replay_at_sequence](../replay_at_sequence/) — replay from sequence N
- [events_store/reconnect_resume](../reconnect_resume/) — Last-Event-ID reconnect
