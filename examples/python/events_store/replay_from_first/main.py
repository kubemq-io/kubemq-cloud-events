"""Example: events_store/replay_from_first — StartFromFirst (type=2) replay."""

from __future__ import annotations

import json
import os
import time

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def publish_events(base: str, channel: str, count: int) -> None:
    for i in range(1, count + 1):
        event = CloudEvent(
            attributes={
                "type": "com.kubemq.examples.eventsstore.replay",
                "source": "kubemq-ce-python-example",
                "subject": channel,
                "datacontenttype": "application/json",
            },
            data={"seq": i},
        )
        headers, body = to_structured(event)
        requests.post(f"{base}/ce/send/event-store", data=body,
                      headers=dict(headers), timeout=10)
        print(f"Published event {i}/{count}")


def replay_from_first(base: str, channel: str, expected: int) -> None:
    sse_url = (f"{base}/ce/subscribe/events-store"
               f"?client_id=python-replay-sub&channel={channel}&events_store_type=2")
    print(f"\nSubscribing with StartFromFirst — expecting {expected} events:")
    count = 0
    with requests.get(sse_url, stream=True, timeout=None,
                      headers={"Accept": "text/event-stream"}) as resp:
        ev_type = data = ""
        for line in resp.iter_lines(decode_unicode=True):
            if line == "":
                if ev_type == "cloudevent" and data:
                    ce = json.loads(data)
                    count += 1
                    print(f"  [{count}/{expected}] data={ce.get('data')}")
                    if count == expected:
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
    channel = "python-ce-events-store.replay-from-first"
    const_events = 5

    publish_events(base, channel, const_events)
    time.sleep(0.2)
    replay_from_first(base, channel, const_events)
    print("\nAll stored events replayed successfully.")


if __name__ == "__main__":
    main()

# Expected output:
# Published event 1/5 ... Published event 5/5
#
# Subscribing with StartFromFirst — expecting 5 events:
#   [1/5] data={'seq': 1}
#   ...
#   [5/5] data={'seq': 5}
#
# All stored events replayed successfully.
