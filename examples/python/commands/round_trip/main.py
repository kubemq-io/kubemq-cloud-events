"""Example: commands/round_trip — RPC command with execution ack."""

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
    sse_url = (f"{base}/ce/subscribe/commands"
               f"?client_id=python-cmd-responder&channel={channel}")
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
                    print(f"[responder] command received: type={raw.get('type')} "
                          f"request_id={request_id}")
                    # Send response.
                    response_event = CloudEvent(
                        attributes={
                            "type": "com.kubemq.examples.commands.response",
                            "source": "python-cmd-responder",
                            "subject": reply_channel,
                            "datacontenttype": "application/json",
                        },
                        data={"executed": True, "status": "command processed"},
                    )
                    headers, body = to_structured(response_event)
                    r = requests.post(
                        f"{base}/ce/send/response",
                        params={"request_id": request_id},
                        data=body, headers=dict(headers), timeout=10,
                    )
                    print(f"[responder] response sent: is_error={r.json().get('is_error')}")
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
    channel = "python-ce-commands.round-trip"

    ready = threading.Event()
    t = threading.Thread(target=responder, args=(base, channel, ready), daemon=True)
    t.start()

    ready.wait(timeout=5)
    time.sleep(0.1)

    event = CloudEvent(
        attributes={
            "type": "com.kubemq.examples.commands.reboot",
            "source": "kubemq-ce-python-sender",
            "subject": channel,
            "datacontenttype": "application/json",
        },
        data={"device_id": "sensor-42", "action": "reboot"},
    )
    headers, body = to_structured(event)
    print("[sender] sending command...")
    resp = requests.post(f"{base}/ce/send/command", data=body,
                          headers=dict(headers), timeout=30)
    result = resp.json()
    print(f"[sender] command ack: status={resp.status_code} is_error={result.get('is_error')}")
    print(f"[sender] response data: {result.get('data')}")
    t.join(timeout=5)


if __name__ == "__main__":
    main()
