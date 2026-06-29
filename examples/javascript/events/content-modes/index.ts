/**
 * Example: events/content-modes
 * Sends the same event in structured and binary mode.
 * Run: npx tsx events/content-modes/index.ts
 */
import { CloudEvent, HTTP } from 'cloudevents';

function serverUrl(): string {
  return process.env.KUBEMQ_CE_URL ?? 'http://localhost:9090';
}

async function main(): Promise<void> {
  const base = serverUrl();

  const event = new CloudEvent({
    type: 'com.kubemq.examples.events.content-mode',
    source: 'kubemq-ce-js-example',
    subject: 'js-ce-events.content-modes',
    datacontenttype: 'application/json',
    data: { message: 'hello content modes' },
  });

  // Structured mode
  console.log('Sending in structured mode:');
  const structured = HTTP.structured(event);
  const r1 = await fetch(`${base}/ce/send/event`, {
    method: 'POST',
    headers: structured.headers as Record<string, string>,
    body: structured.body as string,
  });
  const res1 = await r1.json() as { is_error: boolean };
  console.log(`[structured] status=${r1.status} is_error=${res1.is_error}`);

  // Binary mode
  console.log('\nSending in binary mode:');
  const binary = HTTP.binary(event);
  const r2 = await fetch(`${base}/ce/send/event`, {
    method: 'POST',
    headers: binary.headers as Record<string, string>,
    body: binary.body as string,
  });
  const res2 = await r2.json() as { is_error: boolean };
  console.log(`[binary]     status=${r2.status} is_error=${res2.is_error}`);

  console.log('\nBoth content modes accepted.');
}

main().catch(console.error);

// Expected output:
// Sending in structured mode:
// [structured] status=202 is_error=false
//
// Sending in binary mode:
// [binary]     status=202 is_error=false
//
// Both content modes accepted.
