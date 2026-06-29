# KubeMQ CloudEvents — Ruby Examples

All Ruby examples use the official [CloudEvents Ruby SDK](https://github.com/cloudevents/sdk-ruby) (`cloud_events` gem).

## Prerequisites

- Ruby 3.1+
- Bundler
- KubeMQ server running with CE enabled on `http://localhost:9090`

## Setup

```bash
cd examples/ruby
bundle install
```

## Run Any Example

```bash
export KUBEMQ_CE_URL=http://localhost:9090
ruby events/basic_pubsub/main.rb
```
