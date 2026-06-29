/**
 * Example: queues/ack-all — drain queue with /ce/queue/ack_all.
 * Run: npx tsx queues/ack-all/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-queues.ack-all';
  const clientId = 'kubemq-ce-js-example';

  for (let i = 1; i <= 5; i++) {
    const event = new CloudEvent({
      type: 'com.kubemq.examples.queues.ackall',
      source: clientId,
      subject: channel,
      datacontenttype: 'application/json',
      data: { n: i },
    });
    const msg = HTTP.structured(event);
    await fetch(`${base}/ce/queue/send`, {
      method: 'POST',
      headers: msg.headers as Record<string, string>,
      body: msg.body as string,
    });
  }
  console.log(`Sent 5 messages to queue '${channel}'.`);

  const peekUrl = `${base}/ce/queue/receive?channel=${encodeURIComponent(channel)}&client_id=${clientId}&max_messages=100&wait_timeout=3&is_peek=true`;
  const peek = await fetch(peekUrl, { method: 'POST' });
  const peekResult = await peek.json() as { data: { messages_received: number } };
  console.log(`Peek: ${peekResult.data?.messages_received} messages in queue.`);

  const ackUrl = `${base}/ce/queue/ack_all?channel=${encodeURIComponent(channel)}&client_id=${clientId}&wait_timeout=5`;
  const ack = await fetch(ackUrl, { method: 'POST' });
  const ackResult = await ack.json() as { is_error: boolean; message: string };
  console.log(`ack_all: is_error=${ackResult.is_error} message=${ackResult.message}`);

  const peek2 = await fetch(peekUrl, { method: 'POST' });
  const peek2Result = await peek2.json() as { data: { messages_received: number } };
  console.log(`After ack_all: ${peek2Result.data?.messages_received} messages remaining.`);
}

main().catch(console.error);
