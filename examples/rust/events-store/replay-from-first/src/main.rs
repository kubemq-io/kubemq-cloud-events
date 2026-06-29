//! Example: events-store/replay-from-first
//!
//! Publishes 3 events then subscribes with events_store_type=2 (StartFromFirst).
//!
//! Run: cargo run -p events-store-replay-from-first

use bytes::Bytes;
use cloudevents::{EventBuilder, EventBuilderV10};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::env;
use tokio::sync::mpsc;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

async fn collect_sse_cloudevents(client: Client, url: String, max: usize, tx: mpsc::Sender<String>) {
    let stream = client.get(&url)
        .header("Accept", "text/event-stream")
        .send().await.expect("SSE connect").bytes_stream();
    let mut stream = Box::pin(stream);
    let mut ev_type = String::new();
    let mut data = String::new();
    let mut buffer = String::new();
    let mut count = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk: Bytes = chunk.unwrap_or_default();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer = buffer[pos + 1..].to_string();
            if line.is_empty() {
                if ev_type == "cloudevent" && !data.is_empty() {
                    let _ = tx.send(data.clone()).await;
                    count += 1;
                    if count >= max { return; }
                }
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
    let channel = "rust-ce-events-store.replay-from-first";
    let client = Client::new();

    // Publish 3 events.
    println!("Publishing 3 events to events-store...");
    for i in 1..=3u32 {
        let event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .ty("com.kubemq.examples.eventsstore.stored")
            .source("urn:kubemq-ce-rust-example")
            .subject(channel)
            .data("application/json", json!({"seq": i}))
            .build()?;
        let body = serde_json::to_string(&event)?;
        client.post(format!("{}/ce/send/event-store", base))
            .header("Content-Type", "application/cloudevents+json")
            .body(body).send().await?;
        println!("  Published seq={}", i);
    }

    // Subscribe with StartFromFirst (events_store_type=2).
    let (tx, mut rx) = mpsc::channel::<String>(10);
    let sub_url = format!(
        "{}/ce/subscribe/events-store?client_id=rust-es-replay&channel={}&events_store_type=2",
        base, channel
    );
    tokio::spawn(collect_sse_cloudevents(client.clone(), sub_url, 3, tx));

    let mut events = Vec::new();
    for _ in 0..3 {
        if let Some(data) = tokio::time::timeout(
            tokio::time::Duration::from_secs(10), rx.recv()
        ).await.ok().flatten() {
            let ce: Value = serde_json::from_str(&data)?;
            events.push(ce);
        }
    }

    println!("Replayed {} events from first:", events.len());
    for ce in &events {
        println!("  seq={} id={}", ce["data"]["seq"], ce["id"]);
    }
    Ok(())
}

// Expected output:
// Publishing 3 events to events-store...
//   Published seq=1
//   Published seq=2
//   Published seq=3
// Replayed 3 events from first:
//   seq=1 id="..."
//   seq=2 id="..."
//   seq=3 id="..."
