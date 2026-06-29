"""Example: queues/ack_all — drain queue with POST /ce/queue/ack_all."""

from __future__ import annotations

import os

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def main() -> None:
    base = server_url()
    channel = "python-ce-queues.ack-all"
    client_id = "kubemq-ce-python-example"

    for i in range(1, 6):
        event = CloudEvent(
            attributes={
                "type": "com.kubemq.examples.queues.ackall",
                "source": client_id,
                "subject": channel,
                "datacontenttype": "application/json",
            },
            data={"n": i},
        )
        headers, body = to_structured(event)
        requests.post(f"{base}/ce/queue/send", data=body,
                      headers=dict(headers), timeout=10)
    print(f"Sent 5 messages to queue '{channel}'.")

    # Peek to confirm.
    peek_resp = requests.post(
        f"{base}/ce/queue/receive",
        params={"channel": channel, "client_id": client_id,
                "max_messages": 100, "wait_timeout": 3, "is_peek": "true"},
        timeout=10,
    )
    n = peek_resp.json().get("data", {}).get("messages_received", 0)
    print(f"Peek: {n} messages in queue.")

    # Ack all.
    ack_resp = requests.post(
        f"{base}/ce/queue/ack_all",
        params={"channel": channel, "client_id": client_id, "wait_timeout": 5},
        timeout=10,
    )
    result = ack_resp.json()
    print(f"ack_all: is_error={result.get('is_error')} message={result.get('message')}")

    # Confirm empty.
    peek_resp2 = requests.post(
        f"{base}/ce/queue/receive",
        params={"channel": channel, "client_id": client_id,
                "max_messages": 100, "wait_timeout": 3, "is_peek": "true"},
        timeout=10,
    )
    n2 = peek_resp2.json().get("data", {}).get("messages_received", 0)
    print(f"After ack_all: {n2} messages remaining.")


if __name__ == "__main__":
    main()
