/**
 * Example: routing/cesql-routing — publish events for CESQL server-side routing.
 * Requires KubeMQ configured with CESQL rules. See main.go comments for config.
 * Run: npx tsx routing/cesql-routing/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

function subscribeAndCollect(
  base: string, channel: string, clientId: string, max: number, results: string[],
): void {
  const url = `${base}/ce/subscribe/events?client_id=${clientId}&channel=${encodeURIComponent(channel)}`;
  const es = new EventSource(url);
  let count = 0;
  es.addEventListener('cloudevent', (evt: MessageEvent) => {
    const ce = JSON.parse(evt.data) as Record<string, unknown>;
    results.push(`  [${channel}] type=${ce.type}`);
    count++;
    if (count >= max) es.close();
  });
  es.addEventListener('error', (err) => {
    console.error('SSE error:', err);
    es.close();
  });
}

async function main(): Promise<void> {
  const base = serverUrl();
  console.log('CESQL Routing Example — JavaScript/TypeScript');
  console.log('Requires KubeMQ with CESQL routing rules (see Go example comments).\n');

  const results: string[] = [];
  subscribeAndCollect(base, 'order-archive', 'js-order-sub', 1, results);
  subscribeAndCollect(base, 'alert-stream', 'js-alert-sub', 1, results);
  subscribeAndCollect(base, 'all-events', 'js-all-sub', 3, results);

  await new Promise((r) => setTimeout(r, 500));

  for (const evType of [
    'com.kubemq.examples.routing.order',
    'com.kubemq.examples.routing.alert',
    'com.kubemq.examples.routing.info',
  ]) {
    const event = new CloudEvent({
      type: evType,
      source: 'kubemq-ce-js-cesql',
      subject: 'routing-source',
      datacontenttype: 'application/json',
      data: { description: `Event of type ${evType}` },
    });
    const msg = HTTP.structured(event);
    const resp = await fetch(`${base}/ce/send/event`, {
      method: 'POST',
      headers: msg.headers as Record<string, string>,
      body: msg.body as string,
    });
    console.log(`Published type=${evType} (status=${resp.status})`);
  }

  await new Promise((r) => setTimeout(r, 2000));
  for (const r of results) console.log(r);
  console.log('\nCESQL routing demonstration complete.');
}

main().then(() => process.exit(0)).catch((err) => { console.error(err); process.exit(1); });
