/**
 * Example: events/basic-pubsub
 *
 * Demonstrates fire-and-forget event pub/sub via the KubeMQ
 * CloudEvents HTTP connector. A subscriber opens an SSE stream;
 * a publisher sends one CloudEvent; the subscriber prints it.
 *
 * Run: npx tsx events/basic-pubsub/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';
import EventSource from 'eventsource';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

async function waitForEvent(
  base: string,
  channel: string,
  clientId: string,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const sseUrl = `${base}/ce/subscribe/events?client_id=${clientId}&channel=${encodeURIComponent(channel)}`;
    const es = new EventSource(sseUrl);

    const timer = setTimeout(() => {
      es.close();
      reject(new Error('Timed out waiting for event'));
    }, 10_000);

    es.addEventListener('cloudevent', (evt: MessageEvent) => {
      clearTimeout(timer);
      es.close();
      resolve(JSON.parse(evt.data) as Record<string, unknown>);
    });

    (es as EventSource & { onerror: (e: Event) => void }).onerror = (e) => {
      // EventSource reconnects automatically; log but don't reject.
      console.error('[subscriber] SSE error:', e);
    };
  });
}

async function main(): Promise<void> {
  const base = serverUrl();
  const channel = 'js-ce-events.basic-pubsub';
  const clientId = 'kubemq-ce-js-example';

  // Start waiting for event (opens SSE stream).
  const eventPromise = waitForEvent(base, channel, `${clientId}-sub`);

  // Allow SSE connection to establish.
  await new Promise((r) => setTimeout(r, 500));

  // Build and publish CloudEvent (structured mode).
  const event = new CloudEvent({
    type: 'com.kubemq.examples.events.sent',
    source: clientId,
    subject: channel,
    datacontenttype: 'application/json',
    data: { message: 'Hello from JavaScript/TypeScript CloudEvents example!' },
  });

  const message = HTTP.structured(event);
  const resp = await fetch(`${base}/ce/send/event`, {
    method: 'POST',
    headers: message.headers as Record<string, string>,
    body: message.body as string,
  });
  const result = await resp.json() as { is_error: boolean };
  console.log(`Published: status=${resp.status} is_error=${result.is_error}`);

  // Wait for subscriber.
  const received = await eventPromise;
  console.log('Received event:');
  console.log(`  type:    ${received.type}`);
  console.log(`  source:  ${received.source}`);
  console.log(`  subject: ${received.subject}`);
  console.log(`  data:    ${JSON.stringify(received.data)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
// Published: status=202 is_error=false
// Received event:
//   type:    com.kubemq.examples.events.sent
//   source:  kubemq-ce-js-example
//   subject: js-ce-events.basic-pubsub
//   data:    {"message":"Hello from JavaScript/TypeScript CloudEvents example!"}
