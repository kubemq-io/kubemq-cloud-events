/**
 * Example: events/consumer-group
 * Two subscribers in the same group; each receives exactly one of two events.
 * Run: npx tsx events/consumer-group/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

function subscribeGroup(
  base: string,
  channel: string,
  group: string,
  clientId: string,
): Promise<Record<string, unknown>> {
  return new Promise((resolve) => {
    const url = `${base}/ce/subscribe/events?client_id=${clientId}&channel=${encodeURIComponent(channel)}&group=${group}`;
    const es = new EventSource(url);
    es.addEventListener('cloudevent', (evt: MessageEvent) => {
      es.close();
      resolve(JSON.parse(evt.data) as Record<string, unknown>);
    });
    es.addEventListener('error', (evt: MessageEvent) => {
      if (evt.data) {
        const err = JSON.parse(evt.data) as { message: string };
        console.error('[subscriber] SSE error:', err.message);
        es.close();
      }
    });
  });
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-events.consumer-group';
  const group = 'workers';

  const p1 = subscribeGroup(base, channel, group, 'js-worker-1');
  const p2 = subscribeGroup(base, channel, group, 'js-worker-2');

  await new Promise((r) => setTimeout(r, 600));

  for (const seq of [1, 2]) {
    const event = new CloudEvent({
      type: 'com.kubemq.examples.events.grouped',
      source: 'kubemq-ce-js-example',
      subject: channel,
      datacontenttype: 'application/json',
      data: { seq },
    });
    const msg = HTTP.structured(event);
    const resp = await fetch(`${base}/ce/send/event`, {
      method: 'POST',
      headers: msg.headers as Record<string, string>,
      body: msg.body as string,
    });
    console.log(`Published event seq=${seq} (status=${resp.status})`);
  }

  const [r1, r2] = await Promise.all([p1, p2]);
  console.log(`[worker-1] received: type=${r1.type} data=${JSON.stringify(r1.data)}`);
  console.log(`[worker-2] received: type=${r2.type} data=${JSON.stringify(r2.data)}`);
}

main().catch(console.error);
