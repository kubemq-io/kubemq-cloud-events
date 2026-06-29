//! Example: events/basic-pubsub
//!
//! Demonstrates basic fire-and-forget event pub/sub via the KubeMQ
//! CloudEvents HTTP connector.
//!
//! Run: cargo run -p basic-pubsub

use bytes::Bytes;
use cloudevents::{EventBuilder, EventBuilderV10};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::env;
use tokio::sync::oneshot;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

/// Parse SSE lines from a stream and return the data of the first cloudevent.
async fn wait_for_cloudevent(
    mut stream: impl futures_util::Stream<Item = reqwest::Result<Bytes>> + Unpin,
    tx: oneshot::Sender<String>,
) {
    let mut event_type = String::new();
    let mut data = String::new();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => { eprintln!("SSE read error: {}", e); break; }
        };
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer = buffer[pos + 1..].to_string();

            if line.is_empty() {
                if event_type == "cloudevent" && !data.is_empty() {
                    let _ = tx.send(data.clone());
                    return;
                }
                if event_type == "error" {
                    eprintln!("SSE error: {}", data);
                    return;
                }
                event_type.clear();
                data.clear();
            } else if line.starts_with(':') {
                // keepalive comment — ignore
            } else if let Some(v) = line.strip_prefix("event:") {
                event_type = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("data:") {
                data = v.trim().to_string();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let channel = "rust-ce-events.basic-pubsub";
    let client_id = "kubemq-ce-rust-example";

    let client = Client::new();

    // Start SSE subscriber.
    let (tx, rx) = oneshot::channel::<String>();
    let sub_url = format!(
        "{}/ce/subscribe/events?client_id={}-sub&channel={}",
        base, client_id, channel
    );
    let sub_client = client.clone();
    tokio::spawn(async move {
        let stream = sub_client
            .get(&sub_url)
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .send()
            .await
            .expect("SSE connect failed")
            .bytes_stream();
        wait_for_cloudevent(stream, tx).await;
    });

    // Allow SSE to establish.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Build CloudEvent (structured mode using cloudevents-sdk).
    let event = EventBuilderV10::new()
        .id(Uuid::new_v4().to_string())
        .ty("com.kubemq.examples.events.sent")
        .source(format!("urn:{}", client_id))
        .subject(channel)
        .data(
            "application/json",
            json!({"message": "Hello from Rust CloudEvents example!"}),
        )
        .build()?;

    // Serialize to structured mode JSON.
    let body = serde_json::to_string(&event)?;
    let resp = client
        .post(format!("{}/ce/send/event", base))
        .header("Content-Type", "application/cloudevents+json")
        .body(body)
        .send()
        .await?;

    let result: Value = resp.json().await?;
    println!(
        "Published: status=202 is_error={}",
        result["is_error"]
    );

    // Wait for received event.
    let data = tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        rx,
    )
    .await
    .expect("Timed out waiting for event")
    .expect("Channel closed");

    let ce: Value = serde_json::from_str(&data)?;
    println!("Received event:");
    println!("  type:    {}", ce["type"]);
    println!("  source:  {}", ce["source"]);
    println!("  subject: {}", ce["subject"]);
    println!("  data:    {}", ce["data"]);

    Ok(())
}

// Expected output:
// Published: status=202 is_error=false
// Received event:
//   type:    "com.kubemq.examples.events.sent"
//   source:  "urn:kubemq-ce-rust-example"
//   subject: "rust-ce-events.basic-pubsub"
//   data:    {"message":"Hello from Rust CloudEvents example!"}
