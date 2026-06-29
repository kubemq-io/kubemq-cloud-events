# Ruby — Queues: Peek

Sends 3 messages to a queue, then uses `is_peek=true` to inspect them without consuming. Peeks twice to prove messages remain, then consumes normally.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby queues/peek/main.rb
```

## Expected Output

```
Sent 3 messages.

[peek #1] messages_received=3
[peek #2] messages_received=3
[consume] messages_received=3
```

## What's Happening

- Three messages are sent to `POST /ce/queue/send` as CloudEvents.
- `POST /ce/queue/receive?is_peek=true` inspects messages without advancing the queue read pointer.
- Peeking twice returns the same 3 messages both times.
- A final `POST /ce/queue/receive?is_peek=false` consumes the messages (removing them from the queue).

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.peek | Classification |
| source | urn:kubemq-ce-ruby-example | ClientID |
| subject | ruby-ce-queues.peek | Channel |

## Related Examples

- [queues/basic_send_receive](../basic_send_receive/) — basic send & receive
- [queues/ack_all](../ack_all/) — drain queue atomically
