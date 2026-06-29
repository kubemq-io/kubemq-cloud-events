# KubeMQ CloudEvents — Go Examples

All Go examples use the official [CloudEvents Go SDK](https://github.com/cloudevents/sdk-go).

## Prerequisites

- Go 1.21+
- KubeMQ server running with CE enabled on `http://localhost:9090`

## Setup

```bash
cd examples/go
go mod download  # downloads all dependencies and generates go.sum
```

## Run Any Example

```bash
# Set server URL (optional)
export KUBEMQ_CE_URL=http://localhost:9090

# Run an example
go run ./events/basic-pubsub/main.go
```

## Examples

| Pattern | Directory |
|---------|-----------|
| Basic Pub/Sub | [events/basic-pubsub/](events/basic-pubsub/) |
| Consumer Group | [events/consumer-group/](events/consumer-group/) |
| Content Modes | [events/content-modes/](events/content-modes/) |
| Events-Store Basic | [events-store/basic/](events-store/basic/) |
| Replay From First | [events-store/replay-from-first/](events-store/replay-from-first/) |
| Replay At Sequence | [events-store/replay-at-sequence/](events-store/replay-at-sequence/) |
| Reconnect & Resume | [events-store/reconnect-resume/](events-store/reconnect-resume/) |
| Queue Send & Receive | [queues/basic-send-receive/](queues/basic-send-receive/) |
| Queue Peek | [queues/peek/](queues/peek/) |
| Queue Ack All | [queues/ack-all/](queues/ack-all/) |
| Command Round-Trip | [commands/round-trip/](commands/round-trip/) |
| Query Round-Trip | [queries/round-trip/](queries/round-trip/) |
| CESQL Routing | [routing/cesql-routing/](routing/cesql-routing/) |
