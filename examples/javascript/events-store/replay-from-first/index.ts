/**
 * Example: events-store/replay-from-first — StartFromFirst (type=2).
 * Run: npx tsx events-store/replay-from-first/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

async function publishEvents(base: string, channel: string, count: number): Promise<void> {
  for (let i = 1; i <= count; i++) {
    const event = new CloudEvent({
      type: 'com.kubemq.examples.eventsstore.replay',
      source: 'kubemq-ce-js-example',
      subject: channel,
      datacontenttype: 'application/json',
      data: { seq: i },
    });
    const msg = HTTP.structured(event);
    await fetch(`${base}/ce/send/event-store`, {
      method: 'POST',
      headers: msg.headers as Record<string, string>,
      body: msg.body as string,
    });
    console.log(`Published event ${i}/${count}`);
  }
}

function replayFromFirst(base: string, channel: string, expected: number): Promise<void> {
  return new Promise((resolve) => {
    const url = `${base}/ce/subscribe/events-store?client_id=js-replay-sub&channel=${encodeURIComponent(channel)}&events_store_type=2`;
    const es = new EventSource(url);
    let count = 0;
    console.log(`\nSubscribing StartFromFirst — expecting ${expected} events:`);
    es.addEventListener('cloudevent', (evt: MessageEvent) => {
      const ce = JSON.parse(evt.data) as Record<string, unknown>;
      count++;
      console.log(`  [${count}/${expected}] data=${JSON.stringify(ce.data)}`);
      if (count === expected) {
        es.close();
        resolve();
      }
    });
    es.addEventListener('error', (err) => {
      console.error('SSE error:', err);
      es.close();
    });
  });
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-events-store.replay-from-first';
  const numEvents = 5;

  await publishEvents(base, channel, numEvents);
  await new Promise((r) => setTimeout(r, 200));
  await replayFromFirst(base, channel, numEvents);
  console.log('\nAll stored events replayed.');
}

main().catch(console.error);
