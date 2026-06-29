"""Example: events_store/basic — publish to events-store, subscribe with StartNewOnly."""

from __future__ import annotations

import json
import os
import threading
import time

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def subscribe_events_store(base: str, channel: str, client_id: str,
                            store_type: int, received: list[str]) -> None:
    sse_url = (f"{base}/ce/subscribe/events-store"
               f"?client_id={client_id}&channel={channel}"
               f"&events_store_type={store_type}")
    with requests.get(sse_url, stream=True, timeout=None,
                      headers={"Accept": "text/event-stream"}) as resp:
        ev_type = data = ""
        for line in resp.iter_lines(decode_unicode=True):
            if line == "":
                if ev_type == "cloudevent" and data:
                    received.append(data)
                    return
                ev_type = data = ""
                continue
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                ev_type = line[6:].strip()
            elif line.startswith("data:"):
                data = line[5:].strip()


def main() -> None:
    base = server_url()
    channel = "python-ce-events-store.basic"

    received: list[str] = []
    t = threading.Thread(
        target=subscribe_events_store,
        args=(base, channel, "python-es-sub", 1, received),  # type=1=StartNewOnly
        daemon=True,
    )
    t.start()
    time.sleep(0.5)

    event = CloudEvent(
        attributes={
            "type": "com.kubemq.examples.eventsstore.stored",
            "source": "kubemq-ce-python-example",
            "subject": channel,
            "datacontenttype": "application/json",
        },
        data={"msg": "hello events-store from Python!"},
    )
    headers, body = to_structured(event)
    resp = requests.post(f"{base}/ce/send/event-store", data=body,
                          headers=dict(headers), timeout=10)
    print(f"Published to events-store: status={resp.status_code}")

    deadline = time.time() + 10
    while not received and time.time() < deadline:
        time.sleep(0.1)

    if not received:
        raise TimeoutError("Timed out")

    ce = json.loads(received[0])
    print(f"Received: type={ce.get('type')} data={ce.get('data')}")


if __name__ == "__main__":
    main()

# Expected output:
# Published to events-store: status=202
# Received: type=com.kubemq.examples.eventsstore.stored data={'msg': 'hello events-store from Python!'}
