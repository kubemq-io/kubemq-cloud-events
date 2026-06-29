"""
Example: events/basic_pubsub

Demonstrates basic fire-and-forget event pub/sub via the KubeMQ
CloudEvents HTTP connector.

A subscriber opens an SSE stream; a publisher sends one CloudEvent;
the subscriber prints it and the program exits.

Run: python events/basic_pubsub/main.py
"""

from __future__ import annotations

import json
import os
import threading
import time
from uuid import uuid4

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def subscribe(base: str, channel: str, client_id: str, received: list[str]) -> None:
    """Open SSE stream and collect one cloudevent."""
    sse_url = (
        f"{base}/ce/subscribe/events"
        f"?client_id={client_id}&channel={channel}"
    )
    with requests.get(sse_url, stream=True, timeout=None,
                      headers={"Accept": "text/event-stream",
                               "Cache-Control": "no-cache"}) as resp:
        event_type = ""
        data = ""
        for raw_line in resp.iter_lines(decode_unicode=True):
            line: str = raw_line
            if line == "":
                if event_type == "cloudevent" and data:
                    received.append(data)
                    return
                if event_type == "error":
                    print(f"[subscriber] SSE error: {data}")
                    return
                event_type = ""
                data = ""
                continue
            if line.startswith(":"):
                continue  # keepalive
            if line.startswith("event:"):
                event_type = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data = line[len("data:"):].strip()


def main() -> None:
    base = server_url()
    channel = "python-ce-events.basic-pubsub"
    client_id = "kubemq-ce-python-example"

    received: list[str] = []

    # Start subscriber in background thread.
    t = threading.Thread(
        target=subscribe,
        args=(base, channel, client_id + "-sub", received),
        daemon=True,
    )
    t.start()

    # Allow SSE connection to establish.
    time.sleep(0.5)

    # Build and send CloudEvent (structured mode).
    event = CloudEvent(
        attributes={
            "type": "com.kubemq.examples.events.sent",
            "source": client_id,
            "subject": channel,
            "datacontenttype": "application/json",
        },
        data={"message": "Hello from Python CloudEvents example!"},
    )

    headers, body = to_structured(event)
    resp = requests.post(
        f"{base}/ce/send/event",
        data=body,
        headers=dict(headers),
        timeout=10,
    )
    result = resp.json()
    print(f"Published: status={resp.status_code} is_error={result.get('is_error')}")

    # Wait for subscriber.
    deadline = time.time() + 10
    while not received and time.time() < deadline:
        time.sleep(0.1)

    if not received:
        raise TimeoutError("Timed out waiting for event")

    ce = json.loads(received[0])
    print("Received event:")
    print(f"  type:    {ce.get('type')}")
    print(f"  source:  {ce.get('source')}")
    print(f"  subject: {ce.get('subject')}")
    print(f"  data:    {ce.get('data')}")


if __name__ == "__main__":
    main()

# Expected output:
# Published: status=202 is_error=False
# Received event:
#   type:    com.kubemq.examples.events.sent
#   source:  kubemq-ce-python-example
#   subject: python-ce-events.basic-pubsub
#   data:    {'message': 'Hello from Python CloudEvents example!'}
