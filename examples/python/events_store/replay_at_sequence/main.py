"""Example: events_store/replay_at_sequence — StartAtSequence (type=4) replay."""

from __future__ import annotations

import json
import os
import time

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def main() -> None:
    base = server_url()
    channel = "python-ce-events-store.replay-at-sequence"
    total_events = 10
    start_seq = 5

    # Publish 10 events.
    for i in range(1, total_events + 1):
        event = CloudEvent(
            attributes={
                "type": "com.kubemq.examples.eventsstore.seqreplay",
                "source": "kubemq-ce-python-example",
                "subject": channel,
                "datacontenttype": "application/json",
            },
            data={"seq": i},
        )
        headers, body = to_structured(event)
        requests.post(f"{base}/ce/send/event-store", data=body,
                      headers=dict(headers), timeout=10)
        print(f"Published event {i}/{total_events}")

    time.sleep(0.2)

    # Subscribe starting at sequence 5.
    sse_url = (f"{base}/ce/subscribe/events-store"
               f"?client_id=python-seq-sub&channel={channel}"
               f"&events_store_type=4&events_store_value={start_seq}")

    print(f"\nSubscribing with StartAtSequence={start_seq}:")
    expected = total_events - start_seq + 1
    count = 0

    with requests.get(sse_url, stream=True, timeout=None,
                      headers={"Accept": "text/event-stream"}) as resp:
        ev_type = data = sse_id = ""
        for line in resp.iter_lines(decode_unicode=True):
            if line == "":
                if ev_type == "cloudevent" and data:
                    ce = json.loads(data)
                    count += 1
                    print(f"  [seq={sse_id}] data={ce.get('data')}")
                    if count == expected:
                        break
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

    print(f"\nReceived {count} events starting from sequence {start_seq}.")


if __name__ == "__main__":
    main()
