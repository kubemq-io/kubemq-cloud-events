/**
 * Example: queues/basic-send-receive — send 3 queue messages, receive one at a time.
 * Run: npx tsx queues/basic-send-receive/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

interface CEResponse {
  is_error: boolean;
  message: string;
  data: {
    messages_received: number;
    messages: Array<Record<string, unknown>>;
  };
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-queues.basic';
  const clientId = 'kubemq-ce-js-worker';
  const numMessages = 3;

  console.log(`Sending ${numMessages} messages to queue '${channel}':`);
  for (let i = 1; i <= numMessages; i++) {
    const event = new CloudEvent({
      type: 'com.kubemq.examples.queues.task',
      source: 'kubemq-ce-js-example',
      subject: channel,
      datacontenttype: 'application/json',
      data: { task_id: i, task: 'process-item' },
    });
    const msg = HTTP.structured(event);
    const resp = await fetch(`${base}/ce/queue/send`, {
      method: 'POST',
      headers: msg.headers as Record<string, string>,
      body: msg.body as string,
    });
    const result = await resp.json() as { is_error: boolean };
    console.log(`  Sent task ${i}: status=${resp.status} is_error=${result.is_error}`);
  }

  console.log(`\nReceiving ${numMessages} messages:`);
  for (let i = 0; i < numMessages; i++) {
    const url = `${base}/ce/queue/receive?channel=${encodeURIComponent(channel)}&client_id=${clientId}&max_messages=1&wait_timeout=5`;
    const resp = await fetch(url, { method: 'POST' });
    const result = await resp.json() as CEResponse;
    if (result.is_error) {
      console.log(`  Error: ${result.message}`);
      continue;
    }
    const msgs = result.data?.messages ?? [];
    for (const m of msgs) {
      console.log(`  Received: type=${m.type} data=${JSON.stringify(m.data)}`);
    }
    if (msgs.length === 0) console.log('  No messages');
  }
}

main().catch(console.error);
