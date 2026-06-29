/**
 * Example: queries/round-trip — RPC query with data response.
 * Run: npx tsx queries/round-trip/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

const INVENTORY: Record<string, number> = { 'WIDGET-100': 42, 'GADGET-200': 7 };

function startQueryResponder(base: string, channel: string): Promise<void> {
  return new Promise((resolve) => {
    const url = `${base}/ce/subscribe/queries?client_id=js-query-responder&channel=${encodeURIComponent(channel)}`;
    const es = new EventSource(url);
    es.addEventListener('cloudevent', async (evt: MessageEvent) => {
      es.close();
      const raw = JSON.parse(evt.data) as Record<string, unknown>;
      const requestId = raw._kubemq_request_id as string;
      const replyChannel = raw._kubemq_reply_channel as string;
      const queryData = raw.data as Record<string, string>;
      const sku = queryData.sku;
      const qty = INVENTORY[sku] ?? 0;
      console.log(`[responder] query sku=${sku} qty=${qty} request_id=${requestId}`);

      const response = new CloudEvent({
        type: 'com.kubemq.examples.queries.inventory-result',
        source: 'js-query-responder',
        subject: replyChannel,
        datacontenttype: 'application/json',
        data: { sku, quantity: qty },
      });
      const msg = HTTP.structured(response);
      await fetch(`${base}/ce/send/response?request_id=${requestId}`, {
        method: 'POST',
        headers: msg.headers as Record<string, string>,
        body: msg.body as string,
      });
      console.log('[responder] response sent.');
      resolve();
    });
    es.addEventListener('error', (evt: MessageEvent) => {
      if (evt.data) {
        const err = JSON.parse(evt.data) as { message: string };
        console.error('[responder] SSE error:', err.message);
        es.close();
      }
    });
  });
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-queries.round-trip';

  const responderDone = startQueryResponder(base, channel);
  await new Promise((r) => setTimeout(r, 500));

  const event = new CloudEvent({
    type: 'com.kubemq.examples.queries.inventory-check',
    source: 'kubemq-ce-js-sender',
    subject: channel,
    datacontenttype: 'application/json',
    data: { sku: 'WIDGET-100' },
  });
  const msg = HTTP.structured(event);
  console.log('[sender] sending query for sku=WIDGET-100...');
  const resp = await fetch(`${base}/ce/send/query`, {
    method: 'POST',
    headers: msg.headers as Record<string, string>,
    body: msg.body as string,
  });
  const result = await resp.json() as { is_error: boolean; data: unknown };
  console.log(`[sender] query response: status=${resp.status} is_error=${result.is_error}`);
  console.log(`[sender] response data: ${JSON.stringify(result.data)}`);

  await responderDone;
}

main().catch(console.error);
