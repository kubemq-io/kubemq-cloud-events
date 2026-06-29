//! Example: events-store/basic
//!
//! Subscribes with StartNewOnly (events_store_type=1), publishes one event, receives it.
//!
//! Run: cargo run -p events-store-basic

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

async fn sse_first_cloudevent(
    mut stream: impl futures_util::Stream<Item = reqwest::Result<Bytes>> + Unpin,
    tx: oneshot::Sender<String>,
) {
    let mut ev_type = String::new();
    let mut data = String::new();
    let mut buffer = String::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap_or_default();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer = buffer[pos + 1..].to_string();
            if line.is_empty() {
                if ev_type == "cloudevent" && !data.is_empty() {
                    let _ = tx.send(data.clone());
                    return;
                }
                if ev_type == "error" { eprintln!("SSE error: {}", data); return; }
                ev_type.clear(); data.clear();
            } else if line.starts_with(':') {
            } else if let Some(v) = line.strip_prefix("event:") { ev_type = v.trim().to_string(); }
            else if let Some(v) = line.strip_prefix("data:") { data = v.trim().to_string(); }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let channel = "rust-ce-events-store.basic";
    let client = Client::new();

    let (tx, rx) = oneshot::channel::<String>();
    // events_store_type=1 = StartNewOnly
    let sub_url = format!(
        "{}/ce/subscribe/events-store?client_id=rust-es-sub&channel={}&events_store_type=1",
        base, channel
    );
    let sub_client = client.clone();
    tokio::spawn(async move {
        let stream = sub_client.get(&sub_url)
            .header("Accept", "text/event-stream")
            .send().await.expect("SSE connect").bytes_stream();
        sse_first_cloudevent(stream, tx).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let event = EventBuilderV10::new()
        .id(Uuid::new_v4().to_string())
        .ty("com.kubemq.examples.eventsstore.stored")
        .source("urn:kubemq-ce-rust-example")
        .subject(channel)
        .data("application/json", json!({"msg": "hello events-store from Rust!"}))
        .build()?;
    let body = serde_json::to_string(&event)?;
    let resp = client.post(format!("{}/ce/send/event-store", base))
        .header("Content-Type", "application/cloudevents+json")
        .body(body).send().await?;
    println!("Published to events-store: status={}", resp.status());

    let data = tokio::time::timeout(tokio::time::Duration::from_secs(10), rx)
        .await.expect("Timed out").expect("Channel closed");
    let ce: Value = serde_json::from_str(&data)?;
    println!("Received: type={} data={}", ce["type"], ce["data"]);
    Ok(())
}

// Expected output:
// Published to events-store: status=202 Accepted
// Received: type="com.kubemq.examples.eventsstore.stored" data={"msg":"hello events-store from Rust!"}
