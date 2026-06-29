# KubeMQ CloudEvents — Java Examples

All Java examples use the official [CloudEvents Java SDK](https://github.com/cloudevents/sdk-java) with Maven.

## Prerequisites

- Java 21+
- Maven 3.8+
- KubeMQ server running with CE enabled on `http://localhost:9090`

## Run Any Example

```bash
export KUBEMQ_CE_URL=http://localhost:9090
cd examples/java/events/basic-pubsub
mvn compile exec:java
```
