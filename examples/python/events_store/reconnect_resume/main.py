"""Example: events_store/reconnect_resume — SSE reconnect with Last-Event-ID."""

from __future__ import annotations

import json
import os
import time

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def read_n_events(base: str, channel: str, client_id: str,
                  n: int, last_event_id: str = "") -> str:
    """Open SSE, read n events, return last SSE id."""
    if last_event_id:
        sse_url = (f"{base}/ce/subscribe/events-store"
                   f"?client_id={client_id}&channel={channel}")
        extra_headers = {"Last-Event-ID": last_event_id}
    else:
        sse_url = (f"{base}/ce/subscribe/events-store"
                   f"?client_id={client_id}&channel={channel}&events_store_type=2")
        extra_headers = {}

    headers = {"Accept": "text/event-stream", **extra_headers}
    last_id = ""
    count = 0

    with requests.get(sse_url, stream=True, timeout=None, headers=headers) as resp:
        ev_type = data = sse_id = ""
        for line in resp.iter_lines(decode_unicode=True):
            if line == "":
                if ev_type == "cloudevent" and data:
                    ce = json.loads(data)
                    count += 1
                    last_id = sse_id
                    print(f"  [{count}] id={sse_id} data={ce.get('data')}")
                    if count == n:
                        return last_id
                ev_type = data = sse_id = ""
                continue
            if line.startswith(":"):
                continue
            if line.startswith("id:"):
                sse_id = line[3:].strip()
            elif line.startswith("event:"):
                ev_type = line[6:].strip()
            elif line.startswith("data:"):
                data = line[5:].strip()
    return last_id


def main() -> None:
    base = server_url()
    channel = "python-ce-events-store.reconnect-resume"
    total = 6
    first_batch = 3

    for i in range(1, total + 1):
        event = CloudEvent(
            attributes={
                "type": "com.kubemq.examples.eventsstore.reconnect",
                "source": "kubemq-ce-python-example",
                "subject": channel,
                "datacontenttype": "application/json",
            },
            data={"n": i},
        )
        headers, body = to_structured(event)
        requests.post(f"{base}/ce/send/event-store", data=body,
                      headers=dict(headers), timeout=10)
    print(f"Published {total} events.")
    time.sleep(0.2)

    print(f"\nFirst connection (reading first {first_batch} events):")
    last_id = read_n_events(base, channel, "python-reconnect-sub", first_batch)
    print(f"Disconnected. Last-Event-ID: {last_id}")

    print(f"\nReconnecting with Last-Event-ID={last_id}:")
    read_n_events(base, channel, "python-reconnect-sub", total - first_batch, last_id)
    print("\nReconnect-resume complete.")


if __name__ == "__main__":
    main()
