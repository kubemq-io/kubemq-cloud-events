"""Example: events/consumer_group — load-balanced subscriber groups."""

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


def subscribe_group(base: str, channel: str, group: str, client_id: str,
                    results: list[str], stop: threading.Event) -> None:
    sse_url = (f"{base}/ce/subscribe/events"
               f"?client_id={client_id}&channel={channel}&group={group}")
    with requests.get(sse_url, stream=True, timeout=None,
                      headers={"Accept": "text/event-stream"}) as resp:
        ev_type = ""
        data = ""
        for line in resp.iter_lines(decode_unicode=True):
            if stop.is_set():
                return
            if line == "":
                if ev_type == "cloudevent" and data:
                    results.append(f"[{client_id}] received: {data[:80]}")
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
    channel = "python-ce-events.consumer-group"
    group = "workers"

    results: list[str] = []
    stop = threading.Event()
    threads = []
    for i in range(1, 3):
        t = threading.Thread(
            target=subscribe_group,
            args=(base, channel, group, f"worker-{i}", results, stop),
            daemon=True,
        )
        t.start()
        threads.append(t)

    time.sleep(0.6)

    for seq in range(1, 3):
        event = CloudEvent(
            attributes={
                "type": "com.kubemq.examples.events.grouped",
                "source": "kubemq-ce-python-example",
                "subject": channel,
                "datacontenttype": "application/json",
            },
            data={"seq": seq},
        )
        headers, body = to_structured(event)
        resp = requests.post(f"{base}/ce/send/event", data=body,
                              headers=dict(headers), timeout=10)
        print(f"Published event seq={seq} (status={resp.status_code})")

    deadline = time.time() + 10
    while len(results) < 2 and time.time() < deadline:
        time.sleep(0.1)

    stop.set()
    for r in results:
        print(r)


if __name__ == "__main__":
    main()

# Expected output:
# Published event seq=1 (status=202)
# Published event seq=2 (status=202)
# [worker-1] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
# [worker-2] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
