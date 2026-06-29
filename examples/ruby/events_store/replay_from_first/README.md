# Ruby — Events-Store: Replay From First

Publishes 3 events then subscribes with `StartFromFirst` (`events_store_type=2`) to replay all stored events — even those published before the subscriber connected.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby events_store/replay_from_first/main.rb
```

## Expected Output

```
Publishing 3 events to events-store...
  Published seq=1
  Published seq=2
  Published seq=3
Replayed 3 events from first:
  seq=1 id=...
  seq=2 id=...
  seq=3 id=...
```

## What's Happening

- Three events are published to `POST /ce/send/event-store` before any subscriber connects.
- A subscriber then connects with `?events_store_type=2` (StartFromFirst).
- The server immediately starts streaming all stored messages from the beginning of the channel's history.
- This is the key difference from regular events: messages are durable and replayable.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.eventsstore.stored | Classification |
| source | urn:kubemq-ce-ruby-example | ClientID |
| subject | ruby-ce-events-store.replay-from-first | Channel |

## Related Examples

- [events_store/basic](../basic/) — StartNewOnly subscription
- [events_store/replay_at_sequence](../replay_at_sequence/) — replay from sequence N
