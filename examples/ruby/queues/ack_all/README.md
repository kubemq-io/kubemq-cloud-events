# Ruby — Queues: Ack All

Sends 5 messages, peeks to verify the count, then calls `POST /ce/queue/ack_all` to atomically drain all pending messages. A final peek confirms the queue is empty.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby queues/ack_all/main.rb
```

## Expected Output

```
Sent 5 messages.
Peek: 5 messages in queue.
ack_all: is_error=false message=OK
After ack_all: 0 messages remaining.
```

## What's Happening

- Five messages are sent to the queue via `POST /ce/queue/send`.
- A peek (`is_peek=true`) confirms 5 messages are waiting.
- `POST /ce/queue/ack_all?channel=...&client_id=...&wait_timeout=5` atomically discards all pending messages.
- A second peek confirms 0 messages remain.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.ackall | Classification |
| source | urn:kubemq-ce-ruby-example | ClientID |
| subject | ruby-ce-queues.ack-all | Channel |

## Related Examples

- [queues/basic_send_receive](../basic_send_receive/) — basic send & receive
- [queues/peek](../peek/) — inspect without consuming
