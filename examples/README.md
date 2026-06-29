# KubeMQ CloudEvents — Examples

Working code examples for the [KubeMQ CloudEvents HTTP connector](../docs/README.md) in 7 languages.

## Quick Start

1. Start KubeMQ with CloudEvents enabled:
   ```bash
   docker run -d -p 9090:9090 \
     -e KUBEMQ_CONNECTORS_CE_ENABLE=true \
     kubemq/kubemq
   ```

2. Pick your language:

| Language | Folder | Prerequisites |
|----------|--------|---------------|
| Go | [go/](go/) | Go 1.21+ |
| Python | [python/](python/) | Python 3.10+, pip |
| JavaScript/TypeScript | [javascript/](javascript/) | Node.js 18+, npm |
| Java | [java/](java/) | Java 21+, Maven 3.8+ |
| C# | [csharp/](csharp/) | .NET 8+ |
| Ruby | [ruby/](ruby/) | Ruby 3.1+, Bundler |
| Rust | [rust/](rust/) | Rust 1.75+ (stable), Cargo |

3. Set the server URL (optional, defaults to `http://localhost:9090`):
   ```bash
   export KUBEMQ_CE_URL=http://localhost:9090
   ```

## Example Patterns

| Pattern | Description |
|---------|-------------|
| [events/basic-pubsub](go/events/basic-pubsub/) | Fire-and-forget pub/sub |
| [events/consumer-group](go/events/consumer-group/) | Load-balanced subscriber groups |
| [events/content-modes](go/events/content-modes/) | Structured vs binary mode |
| [events-store/basic](go/events-store/basic/) | Persistent events, StartNewOnly |
| [events-store/replay-from-first](go/events-store/replay-from-first/) | Replay all stored events |
| [events-store/replay-at-sequence](go/events-store/replay-at-sequence/) | Replay from sequence N |
| [events-store/reconnect-resume](go/events-store/reconnect-resume/) | Resume after disconnect |
| [queues/basic-send-receive](go/queues/basic-send-receive/) | Point-to-point queues |
| [queues/peek](go/queues/peek/) | Inspect without consuming |
| [queues/ack-all](go/queues/ack-all/) | Drain queue atomically |
| [commands/round-trip](go/commands/round-trip/) | RPC command with ack |
| [queries/round-trip](go/queries/round-trip/) | RPC query with data response |
| [routing/cesql-routing](go/routing/cesql-routing/) | Server-side CESQL filtering |

Each pattern folder contains the same example in all 7 languages. See the language-specific README for run instructions.

## Documentation

Full documentation is in [../docs/](../docs/README.md).
