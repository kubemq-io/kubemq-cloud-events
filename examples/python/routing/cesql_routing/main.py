"""Example: routing/cesql_routing — publish events for server-side CESQL routing.

CESQL routing is SERVER-SIDE configuration. See the comments below for
required KubeMQ configuration before running this example.
"""

from __future__ import annotations

import json
import os
import threading
import time

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent

# Required KubeMQ routing config (TOML):
# [Routing]
#   Enable = true
#   Data = '[
#     {"key":"type = '\''com.kubemq.examples.routing.order'\''","keyType":"cesql","routes":"events:order-archive"},
#     {"key":"type = '\''com.kubemq.examples.routing.alert'\''","keyType":"cesql","routes":"events:alert-stream"},
#     {"key":"type LIKE '\''com.kubemq.examples.routing.%'\''","keyType":"cesql","routes":"events:all-events"}
#   ]'


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def subscribe_and_print(base: str, channel: str, client_id: str,
                         results: list[str], max_msgs: int) -> None:
    sse_url = (f"{base}/ce/subscribe/events"
               f"?client_id={client_id}&channel={channel}")
    with requests.get(sse_url, stream=True, timeout=None,
                      headers={"Accept": "text/event-stream"}) as resp:
        ev_type = data = ""
        count = 0
        for line in resp.iter_lines(decode_unicode=True):
            if line == "":
                if ev_type == "cloudevent" and data:
                    ce = json.loads(data)
                    results.append(f"  [{channel}] type={ce.get('type')}")
                    count += 1
                    if count >= max_msgs:
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

    print("CESQL Routing Example — Python")
    print("Requires KubeMQ with CESQL routing configured (see source comments).\n")

    results: list[str] = []
    # Subscribe to routed channels.
    for ch, cid, n in [("order-archive", "py-order-sub", 1),
                        ("alert-stream", "py-alert-sub", 1),
                        ("all-events", "py-all-sub", 3)]:
        t = threading.Thread(
            target=subscribe_and_print,
            args=(base, ch, cid, results, n),
            daemon=True,
        )
        t.start()

    time.sleep(0.5)

    for ev_type in [
        "com.kubemq.examples.routing.order",
        "com.kubemq.examples.routing.alert",
        "com.kubemq.examples.routing.info",
    ]:
        event = CloudEvent(
            attributes={
                "type": ev_type,
                "source": "kubemq-ce-python-cesql",
                "subject": "routing-source",
                "datacontenttype": "application/json",
            },
            data={"description": f"Event of type {ev_type}"},
        )
        headers, body = to_structured(event)
        resp = requests.post(f"{base}/ce/send/event", data=body,
                              headers=dict(headers), timeout=10)
        print(f"Published type={ev_type} (status={resp.status_code})")

    time.sleep(2)
    for r in results:
        print(r)
    print("\nCESQL routing demonstration complete.")


if __name__ == "__main__":
    main()
