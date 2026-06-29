"""Example: events/content_modes — structured vs binary CloudEvents mode."""

from __future__ import annotations

import json
import os
import time

import requests
from cloudevents.v1.conversion import to_binary, to_structured
from cloudevents.v1.http import CloudEvent


def server_url() -> str:
    return os.environ.get("KUBEMQ_CE_URL", "http://localhost:9090")


def main() -> None:
    base = server_url()

    event = CloudEvent(
        attributes={
            "type": "com.kubemq.examples.events.content-mode",
            "source": "kubemq-ce-python-example",
            "subject": "python-ce-events.content-modes",
            "datacontenttype": "application/json",
        },
        data={"message": "hello content modes"},
    )

    # --- Structured mode ---
    print("Sending in structured mode (Content-Type: application/cloudevents+json):")
    headers, body = to_structured(event)
    resp = requests.post(f"{base}/ce/send/event", data=body,
                          headers=dict(headers), timeout=10)
    result = resp.json()
    print(f"[structured] status={resp.status_code} is_error={result.get('is_error')}")

    # --- Binary mode ---
    print("\nSending in binary mode (ce-* HTTP headers):")
    headers, body = to_binary(event)
    resp = requests.post(f"{base}/ce/send/event", data=body,
                          headers=dict(headers), timeout=10)
    result = resp.json()
    print(f"[binary]     status={resp.status_code} is_error={result.get('is_error')}")

    print("\nBoth content modes accepted by KubeMQ CE connector.")


if __name__ == "__main__":
    main()

# Expected output:
# Sending in structured mode (Content-Type: application/cloudevents+json):
# [structured] status=202 is_error=False
#
# Sending in binary mode (ce-* HTTP headers):
# [binary]     status=202 is_error=False
#
# Both content modes accepted by KubeMQ CE connector.
