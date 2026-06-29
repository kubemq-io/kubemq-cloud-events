"""Example: queries/round_trip — RPC query with data response."""

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


def responder(base: str, channel: str, ready: threading.Event) -> None:
    sse_url = (f"{base}/ce/subscribe/queries"
               f"?client_id=python-query-responder&channel={channel}")
    inventory = {"WIDGET-100": 42, "GADGET-200": 7}

    with requests.get(sse_url, stream=True, timeout=None,
                      headers={"Accept": "text/event-stream"}) as resp:
        ready.set()
        ev_type = data = ""
        for line in resp.iter_lines(decode_unicode=True):
            if line == "":
                if ev_type == "cloudevent" and data:
                    raw = json.loads(data)
                    request_id = raw.get("_kubemq_request_id", "")
                    reply_channel = raw.get("_kubemq_reply_channel", "")
                    query_data = raw.get("data", {})
                    sku = query_data.get("sku", "")
                    qty = inventory.get(sku, 0)
                    print(f"[responder] query sku={sku} qty={qty} request_id={request_id}")

                    response_event = CloudEvent(
                        attributes={
                            "type": "com.kubemq.examples.queries.inventory-result",
                            "source": "python-query-responder",
                            "subject": reply_channel,
                            "datacontenttype": "application/json",
                        },
                        data={"sku": sku, "quantity": qty},
                    )
                    headers, body = to_structured(response_event)
                    requests.post(
                        f"{base}/ce/send/response",
                        params={"request_id": request_id},
                        data=body, headers=dict(headers), timeout=10,
                    )
                    print("[responder] response sent.")
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
    channel = "python-ce-queries.round-trip"

    ready = threading.Event()
    t = threading.Thread(target=responder, args=(base, channel, ready), daemon=True)
    t.start()

    ready.wait(timeout=5)
    time.sleep(0.1)

    event = CloudEvent(
        attributes={
            "type": "com.kubemq.examples.queries.inventory-check",
            "source": "kubemq-ce-python-sender",
            "subject": channel,
            "datacontenttype": "application/json",
        },
        data={"sku": "WIDGET-100"},
    )
    headers, body = to_structured(event)
    print("[sender] sending query for sku=WIDGET-100...")
    resp = requests.post(f"{base}/ce/send/query", data=body,
                          headers=dict(headers), timeout=30)
    result = resp.json()
    print(f"[sender] query response: status={resp.status_code} is_error={result.get('is_error')}")
    print(f"[sender] response data: {result.get('data')}")
    t.join(timeout=5)


if __name__ == "__main__":
    main()
