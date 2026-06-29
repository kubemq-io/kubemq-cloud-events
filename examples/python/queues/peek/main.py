"""Example: queues/peek — is_peek=true inspection without consuming."""

from __future__ import annotations

import os

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def receive_or_peek(base: str, channel: str, client_id: str, is_peek: bool, label: str) -> int:
    resp = requests.post(
        f"{base}/ce/queue/receive",
        params={
            "channel": channel,
            "client_id": client_id,
            "max_messages": 10,
            "wait_timeout": 3,
            "is_peek": "true" if is_peek else "false",
        },
        timeout=10,
    )
    result = resp.json()
    n = result.get("data", {}).get("messages_received", 0)
    print(f"[{label}] messages_received={n}")
    return n


def main() -> None:
    base = server_url()
    channel = "python-ce-queues.peek"

    for i in range(1, 4):
        event = CloudEvent(
            attributes={
                "type": "com.kubemq.examples.queues.peek",
                "source": "kubemq-ce-python-example",
                "subject": channel,
                "datacontenttype": "application/json",
            },
            data={"n": i},
        )
        headers, body = to_structured(event)
        requests.post(f"{base}/ce/queue/send", data=body,
                      headers=dict(headers), timeout=10)
    print("Sent 3 messages.\n")

    receive_or_peek(base, channel, "python-peek-client", True, "peek #1")
    receive_or_peek(base, channel, "python-peek-client", True, "peek #2")
    receive_or_peek(base, channel, "python-peek-client", False, "consume")


if __name__ == "__main__":
    main()
