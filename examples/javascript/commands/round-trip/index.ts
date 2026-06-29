/**
 * Example: commands/round-trip — RPC command with execution ack.
 * Run: npx tsx commands/round-trip/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

function startResponder(base: string, channel: string): Promise<void> {
  return new Promise((resolve) => {
    const url = `${base}/ce/subscribe/commands?client_id=js-cmd-responder&channel=${encodeURIComponent(channel)}`;
    const es = new EventSource(url);
    es.addEventListener('cloudevent', async (evt: MessageEvent) => {
      es.close();
      const raw = JSON.parse(evt.data) as Record<string, unknown>;
      const requestId = raw._kubemq_request_id as string;
      const replyChannel = raw._kubemq_reply_channel as string;
      console.log(`[responder] command received: type=${raw.type} request_id=${requestId}`);

      const response = new CloudEvent({
        type: 'com.kubemq.examples.commands.response',
        source: 'js-cmd-responder',
        subject: replyChannel,
        datacontenttype: 'application/json',
        data: { executed: true, status: 'command processed' },
      });
      const msg = HTTP.structured(response);
      const r = await fetch(`${base}/ce/send/response?request_id=${requestId}`, {
        method: 'POST',
        headers: msg.headers as Record<string, string>,
        body: msg.body as string,
      });
      const result = await r.json() as { is_error: boolean };
      console.log(`[responder] response sent: is_error=${result.is_error}`);
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
  const channel = 'js-ce-commands.round-trip';

  const responderDone = startResponder(base, channel);
  await new Promise((r) => setTimeout(r, 500));

  const event = new CloudEvent({
    type: 'com.kubemq.examples.commands.reboot',
    source: 'kubemq-ce-js-sender',
    subject: channel,
    datacontenttype: 'application/json',
    data: { device_id: 'sensor-42', action: 'reboot' },
  });
  const msg = HTTP.structured(event);
  console.log('[sender] sending command...');
  const resp = await fetch(`${base}/ce/send/command`, {
    method: 'POST',
    headers: msg.headers as Record<string, string>,
    body: msg.body as string,
  });
  const result = await resp.json() as { is_error: boolean; data: unknown };
  console.log(`[sender] command ack: status=${resp.status} is_error=${result.is_error}`);

  await responderDone;
}

main().catch(console.error);
