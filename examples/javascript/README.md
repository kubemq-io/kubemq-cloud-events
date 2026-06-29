# KubeMQ CloudEvents — JavaScript/TypeScript Examples

All examples use the official [CloudEvents JavaScript SDK](https://github.com/cloudevents/sdk-javascript) and are written in TypeScript.

## Prerequisites

- Node.js 18+
- npm
- KubeMQ server running with CE enabled on `http://localhost:9090`

## Setup

```bash
cd examples/javascript
npm install
```

## Run Any Example

```bash
export KUBEMQ_CE_URL=http://localhost:9090
npx tsx events/basic-pubsub/index.ts
```
