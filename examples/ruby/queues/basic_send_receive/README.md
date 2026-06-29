# Ruby — Queues: Basic Send & Receive

Sends 3 messages to a KubeMQ queue via `POST /ce/queue/send`, then polls to receive them one at a time via `POST /ce/queue/receive`.

## Prerequisites

- Ruby 3.1+, `bundle install` in `examples/ruby/`
- KubeMQ server with CE enabled

## How to Run

```bash
cd examples/ruby
bundle install
ruby queues/basic_send_receive/main.rb
```

## Expected Output

```
Sending 3 messages to queue 'ruby-ce-queues.basic':
  Sent task 1: status=202 is_error=false
  Sent task 2: status=202 is_error=false
  Sent task 3: status=202 is_error=false

Receiving 3 messages:
  Received: type=com.kubemq.examples.queues.task data={"task_id"=>1, "task"=>"process-item"}
  Received: type=com.kubemq.examples.queues.task data={"task_id"=>2, "task"=>"process-item"}
  Received: type=com.kubemq.examples.queues.task data={"task_id"=>3, "task"=>"process-item"}
```

## What's Happening

- Messages are sent via `POST /ce/queue/send` as CloudEvents (structured mode via `encode_event`).
- Messages are polled via `POST /ce/queue/receive?channel=...&client_id=...&max_messages=1&wait_timeout=5`.
- Queue receive is a **control operation** — the body is ignored, all params are query strings.
- Received CE messages are reconstructed from KubeMQ tags back into CE JSON objects.

## CE Attributes Used

| Attribute | Value | Purpose |
|-----------|-------|---------|
| type | com.kubemq.examples.queues.task | Classification |
| source | urn:kubemq-ce-ruby-example | ClientID |
| subject | ruby-ce-queues.basic | Channel |

## Related Examples

- [queues/peek](../peek/) — inspect without consuming
- [queues/ack_all](../ack_all/) — drain queue atomically
