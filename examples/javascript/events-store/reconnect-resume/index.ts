/**
 * Example: events-store/reconnect-resume — Last-Event-ID reconnection.
 * Run: npx tsx events-store/reconnect-resume/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

function readNEvents(
  base: string, channel: string, clientId: string,
  n: number, lastEventId?: string,
): Promise<string> {
  return new Promise((resolve) => {
    const params = new URLSearchParams({ client_id: clientId, channel });
    if (lastEventId) {
      // omit events_store_type so Last-Event-ID takes precedence
    } else {
      params.set('events_store_type', '2');
    }
    const headers: Record<string, string> = {};
    if (lastEventId) headers['Last-Event-ID'] = lastEventId;

    // Note: EventSource doesn't natively support custom headers.
    // We use a manual SSE approach via fetch for Last-Event-ID reconnect.
    // For initial connection, EventSource works fine.
    if (!lastEventId) {
      const es = new EventSource(`${base}/ce/subscribe/events-store?${params}`);
      let count = 0;
      let lastId = '';
      es.addEventListener('error', (err) => {
        console.error('SSE error:', err);
        es.close();
        resolve('');
      });
      es.addEventListener('cloudevent', (evt: MessageEvent & { lastEventId: string }) => {
        const ce = JSON.parse(evt.data) as Record<string, unknown>;
        count++;
        lastId = evt.lastEventId;
        console.log(`  [${count}] id=${lastId} data=${JSON.stringify(ce.data)}`);
        if (count === n) {
          es.close();
          resolve(lastId);
        }
      });
    } else {
      // Use fetch with Last-Event-ID header for reconnect.
      const url = `${base}/ce/subscribe/events-store?${params}`;
      fetch(url, { headers }).then(async (resp) => {
        const reader = resp.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let count = 0;
        let lastId = lastEventId;
        let evType = '';
        let data = '';

        while (count < n) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() ?? '';
          for (const line of lines) {
            if (line === '') {
              if (evType === 'cloudevent' && data) {
                const ce = JSON.parse(data) as Record<string, unknown>;
                count++;
                console.log(`  [${count}] id=${lastId} data=${JSON.stringify(ce.data)}`);
                if (count === n) { reader.cancel(); resolve(lastId); return; }
              }
              evType = data = '';
            } else if (line.startsWith('id:')) {
              lastId = line.slice(3).trim();
            } else if (line.startsWith('event:')) {
              evType = line.slice(6).trim();
            } else if (line.startsWith('data:')) {
              data = line.slice(5).trim();
            }
          }
        }
        resolve(lastId);
      });
    }
  });
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-events-store.reconnect-resume';
  const total = 6;
  const firstBatch = 3;

  for (let i = 1; i <= total; i++) {
    const event = new CloudEvent({
      type: 'com.kubemq.examples.eventsstore.reconnect',
      source: 'kubemq-ce-js-example',
      subject: channel,
      datacontenttype: 'application/json',
      data: { n: i },
    });
    const msg = HTTP.structured(event);
    await fetch(`${base}/ce/send/event-store`, {
      method: 'POST',
      headers: msg.headers as Record<string, string>,
      body: msg.body as string,
    });
  }
  console.log(`Published ${total} events.`);
  await new Promise((r) => setTimeout(r, 200));

  console.log(`\nFirst connection (reading first ${firstBatch} events):`);
  const lastId = await readNEvents(base, channel, 'js-reconnect-sub', firstBatch);
  console.log(`Disconnected. Last-Event-ID: ${lastId}`);

  console.log(`\nReconnecting with Last-Event-ID=${lastId}:`);
  await readNEvents(base, channel, 'js-reconnect-sub', total - firstBatch, lastId);
  console.log('\nReconnect-resume complete.');
}

main().catch(console.error);
