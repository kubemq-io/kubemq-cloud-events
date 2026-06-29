"""Example: queues/basic_send_receive — send 3 messages, receive one at a time."""

from __future__ import annotations

import json
import os

import requests
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def main() -> None:
    base = server_url()
    channel = "python-ce-queues.basic"
    client_id = "kubemq-ce-python-worker"
    num_messages = 3

    print(f"Sending {num_messages} messages to queue '{channel}':")
    for i in range(1, num_messages + 1):
        event = CloudEvent(
            attributes={
                "type": "com.kubemq.examples.queues.task",
                "source": "kubemq-ce-python-example",
                "subject": channel,
                "datacontenttype": "application/json",
            },
            data={"task_id": i, "task": "process-item"},
        )
        headers, body = to_structured(event)
        resp = requests.post(f"{base}/ce/queue/send", data=body,
                              headers=dict(headers), timeout=10)
        result = resp.json()
        print(f"  Sent task {i}: status={resp.status_code} is_error={result.get('is_error')}")

    print(f"\nReceiving {num_messages} messages:")
    for _ in range(num_messages):
        resp = requests.post(
            f"{base}/ce/queue/receive",
            params={
                "channel": channel,
                "client_id": client_id,
                "max_messages": 1,
                "wait_timeout": 5,
            },
            timeout=10,
        )
        result = resp.json()
        if result.get("is_error"):
            print(f"  Error: {result.get('message')}")
            continue
        data = result.get("data", {})
        messages = data.get("messages", [])
        if not messages:
            print("  No messages (queue empty)")
            continue
        for msg in messages:
            print(f"  Received: type={msg.get('type')} data={msg.get('data')}")


if __name__ == "__main__":
    main()
