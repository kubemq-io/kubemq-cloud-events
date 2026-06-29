/**
 * Example: queues/peek — inspect without consuming (is_peek=true).
 * Run: npx tsx queues/peek/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

async function receiveOrPeek(
  base: string, channel: string, clientId: string, isPeak: boolean, label: string,
): Promise<number> {
  const url = `${base}/ce/queue/receive?channel=${encodeURIComponent(channel)}&client_id=${clientId}&max_messages=10&wait_timeout=3&is_peek=${isPeak}`;
  const resp = await fetch(url, { method: 'POST' });
  const result = await resp.json() as { is_error: boolean; data: { messages_received: number } };
  const n = result.data?.messages_received ?? 0;
  console.log(`[${label}] messages_received=${n}`);
  return n;
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-queues.peek';

  for (let i = 1; i <= 3; i++) {
    const event = new CloudEvent({
      type: 'com.kubemq.examples.queues.peek',
      source: 'kubemq-ce-js-example',
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
  console.log('Sent 3 messages.\n');

  await receiveOrPeek(base, channel, 'js-peek-client', true, 'peek #1');
  await receiveOrPeek(base, channel, 'js-peek-client', true, 'peek #2');
  await receiveOrPeek(base, channel, 'js-peek-client', false, 'consume');
}

main().catch(console.error);
