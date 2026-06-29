/**
 * Example: events-store/replay-at-sequence — StartAtSequence (type=4).
 * Run: npx tsx events-store/replay-at-sequence/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-events-store.replay-at-sequence';
  const totalEvents = 10;
  const startSeq = 5;

  for (let i = 1; i <= totalEvents; i++) {
    const event = new CloudEvent({
      type: 'com.kubemq.examples.eventsstore.seqreplay',
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
    console.log(`Published event ${i}/${totalEvents}`);
  }

  await new Promise((r) => setTimeout(r, 200));

  const expected = totalEvents - startSeq + 1;
  console.log(`\nSubscribing StartAtSequence=${startSeq} — expecting ${expected} events:`);

  await new Promise<void>((resolve) => {
    const url = `${base}/ce/subscribe/events-store?client_id=js-seq-sub&channel=${encodeURIComponent(channel)}&events_store_type=4&events_store_value=${startSeq}`;
    const es = new EventSource(url);
    let count = 0;
    es.addEventListener('cloudevent', (evt: MessageEvent & { lastEventId: string }) => {
      const ce = JSON.parse(evt.data) as Record<string, unknown>;
      count++;
      console.log(`  [seq=${evt.lastEventId}] data=${JSON.stringify(ce.data)}`);
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

  console.log(`\nReceived ${totalEvents - startSeq + 1} events from sequence ${startSeq}.`);
}

main().catch(console.error);
