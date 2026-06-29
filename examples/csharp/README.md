# KubeMQ CloudEvents — C# Examples

All C# examples use the official [CloudEvents .NET SDK](https://github.com/cloudevents/sdk-csharp) (`CloudNative.CloudEvents`).

## Prerequisites

- .NET 8+
- KubeMQ server running with CE enabled on `http://localhost:9090`

## Run Any Example

```bash
export KUBEMQ_CE_URL=http://localhost:9090
cd examples/csharp/events/BasicPubSub
dotnet run
```
