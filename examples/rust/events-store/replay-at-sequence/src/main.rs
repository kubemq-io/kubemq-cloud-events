//! Example: events-store/replay-at-sequence
//!
//! Publishes 5 events then subscribes with events_store_type=4 + events_store_value=3.
//!
//! Run: cargo run -p events-store-replay-at-sequence

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

async fn collect_sse_n(client: Client, url: String, max: usize, tx: mpsc::Sender<String>) {
    let stream = client.get(&url)
        .header("Accept", "text/event-stream")
        .send().await.expect("SSE connect").bytes_stream();
    let mut stream = Box::pin(stream);
    let mut ev_type = String::new(); let mut data = String::new(); let mut buffer = String::new();
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
                    count += 1; if count >= max { return; }
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
    let channel = "rust-ce-events-store.replay-at-sequence";
    let client = Client::new();
    let start_at_seq: u32 = 3;

    // Publish 5 events.
    println!("Publishing 5 events to events-store...");
    for i in 1..=5u32 {
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
    }
    println!("Published 5 events. Subscribing from sequence {}...", start_at_seq);

    // events_store_type=4 (StartAtSequence) + events_store_value=start_at_seq
    let expected = (5 - start_at_seq + 1) as usize;
    let (tx, mut rx) = mpsc::channel::<String>(10);
    let sub_url = format!(
        "{}/ce/subscribe/events-store?client_id=rust-es-seq&channel={}&events_store_type=4&events_store_value={}",
        base, channel, start_at_seq
    );
    tokio::spawn(collect_sse_n(client.clone(), sub_url, expected, tx));

    println!("Expecting {} events from sequence {}:", expected, start_at_seq);
    for _ in 0..expected {
        if let Some(data) = tokio::time::timeout(
            tokio::time::Duration::from_secs(10), rx.recv()
        ).await.ok().flatten() {
            let ce: Value = serde_json::from_str(&data)?;
            println!("  Received: seq={}", ce["data"]["seq"]);
        }
    }
    Ok(())
}

// Expected output:
// Publishing 5 events to events-store...
// Published 5 events. Subscribing from sequence 3...
// Expecting 3 events from sequence 3:
//   Received: seq=3
//   Received: seq=4
//   Received: seq=5
