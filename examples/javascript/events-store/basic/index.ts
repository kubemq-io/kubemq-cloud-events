/**
 * Example: events-store/basic — persist and subscribe with StartNewOnly.
 * Run: npx tsx events-store/basic/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-events-store.basic';

  const eventPromise = new Promise<Record<string, unknown>>((resolve) => {
    const url = `${base}/ce/subscribe/events-store?client_id=js-es-sub&channel=${encodeURIComponent(channel)}&events_store_type=1`;
    const es = new EventSource(url);
    es.addEventListener('cloudevent', (evt: MessageEvent) => {
      es.close();
      resolve(JSON.parse(evt.data) as Record<string, unknown>);
    });
    es.addEventListener('error', (err) => {
      console.error('SSE error:', err);
      es.close();
    });
  });

  await new Promise((r) => setTimeout(r, 500));

  const event = new CloudEvent({
    type: 'com.kubemq.examples.eventsstore.stored',
    source: 'kubemq-ce-js-example',
    subject: channel,
    datacontenttype: 'application/json',
    data: { msg: 'hello events-store from JS!' },
  });
  const msg = HTTP.structured(event);
  const resp = await fetch(`${base}/ce/send/event-store`, {
    method: 'POST',
    headers: msg.headers as Record<string, string>,
    body: msg.body as string,
  });
  console.log(`Published to events-store: status=${resp.status}`);

  const received = await eventPromise;
  console.log(`Received: type=${received.type} data=${JSON.stringify(received.data)}`);
}

main().catch(console.error);
