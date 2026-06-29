# Getting Started

Get a CloudEvent flowing through KubeMQ in 5 minutes.

## Prerequisites

- Docker
- `curl`

## Step 1: Start KubeMQ with CloudEvents Enabled

```bash
docker run -d \
  --name kubemq-ce \
  -p 9090:9090 \
  -p 50000:50000 \
  -e KUBEMQ_CONNECTORS_CE_ENABLE=true \
  kubemq/kubemq
```

Verify the CE connector is running:

```bash
curl -s http://localhost:9090/health | jq .
```

## Step 2: Subscribe to Events

Open a terminal and subscribe:

```bash
curl -N "http://localhost:9090/ce/subscribe/events?client_id=demo-sub&channel=demo"
```

Leave this running.

## Step 3: Publish a CloudEvent

In a second terminal:

```bash
curl -X POST http://localhost:9090/ce/send/event \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "com.example.greeting",
    "source": "demo-publisher",
    "subject": "demo",
    "datacontenttype": "application/json",
    "data": {"message": "Hello, CloudEvents!"}
  }'
```

The first terminal will display:

```
event: cloudevent
data: {"specversion":"1.0","type":"com.example.greeting","source":"demo-publisher","id":"...","subject":"demo","time":"...","data":{"message":"Hello, CloudEvents!"}}

```

## Step 4: Explore Language Examples

Pick your language and run the basic pub/sub example:

| Language | Command |
|----------|---------|
| Go | `go run ./examples/go/events/basic-pubsub/main.go` |
| Python | `python examples/python/events/basic_pubsub/main.py` |
| JavaScript | `npx tsx examples/javascript/events/basic-pubsub/index.ts` |
| Java | `cd examples/java/events/basic-pubsub && mvn compile exec:java` |
| C# | `cd examples/csharp/events/BasicPubSub && dotnet run` |
| Ruby | `ruby examples/ruby/events/basic_pubsub/main.rb` |
| Rust | `cd examples/rust && cargo run -p basic-pubsub` |

## Environment Variable

All examples respect `KUBEMQ_CE_URL` to override the server address:

```bash
export KUBEMQ_CE_URL=http://my-kubemq-server:9090
```

## Next Steps

- [Architecture](architecture.md) — understand how CE maps to KubeMQ
- [Patterns](patterns/events.md) — deep dive into each messaging pattern
- [Configuration](configuration.md) — tune the CE connector
