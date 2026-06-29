//! Example: events-store/reconnect-resume
//!
//! Demonstrates Last-Event-ID reconnect. Receives 2 events recording SSE id field,
//! then reconnects with Last-Event-ID header (no events_store_type param).
//!
//! Run: cargo run -p events-store-reconnect-resume

use bytes::Bytes;
use cloudevents::{EventBuilder, EventBuilderV10};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::env;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

/// Subscribe and collect up to `max` events. Returns (events, last_id_seen).
async fn subscribe_and_collect(
    client: &Client,
    url: &str,
    last_event_id: Option<&str>,
    max: usize,
) -> (Vec<Value>, String) {
    let mut req = client.get(url).header("Accept", "text/event-stream");
    if let Some(id) = last_event_id {
        req = req.header("Last-Event-ID", id);
    }
    let stream = req.send().await.expect("SSE connect").bytes_stream();
    let mut stream = Box::pin(stream);
    let mut ev_type = String::new(); let mut data = String::new();
    let mut id_field = String::new(); let mut last_id = String::new();
    let mut buffer = String::new();
    let mut events = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk: Bytes = chunk.unwrap_or_default();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer = buffer[pos + 1..].to_string();
            if line.is_empty() {
                if ev_type == "cloudevent" && !data.is_empty() {
                    if !id_field.is_empty() { last_id = id_field.clone(); }
                    let ce: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
                    events.push(ce);
                    if events.len() >= max { return (events, last_id); }
                }
                ev_type.clear(); data.clear(); id_field.clear();
            } else if line.starts_with(':') {
            } else if let Some(v) = line.strip_prefix("id:") { id_field = v.trim().to_string(); }
            else if let Some(v) = line.strip_prefix("event:") { ev_type = v.trim().to_string(); }
            else if let Some(v) = line.strip_prefix("data:") { data = v.trim().to_string(); }
        }
    }
    (events, last_id)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let channel = "rust-ce-events-store.reconnect-resume";
    let client = Client::new();

    // Publish 4 events.
    println!("Publishing 4 events...");
    for i in 1..=4u32 {
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

    // First connection: StartFromFirst (events_store_type=2), receive 2 events.
    let first_url = format!(
        "{}/ce/subscribe/events-store?client_id=rust-es-reconnect&channel={}&events_store_type=2",
        base, channel
    );
    println!("First connection — receiving 2 events:");
    let (first_events, last_id) = subscribe_and_collect(&client, &first_url, None, 2).await;
    for ce in &first_events {
        println!("  Received: seq={}", ce["data"]["seq"]);
    }
    println!("  Last-Event-ID recorded: {}", last_id);

    // Reconnect using Last-Event-ID (no events_store_type param).
    let reconnect_url = format!(
        "{}/ce/subscribe/events-store?client_id=rust-es-reconnect&channel={}",
        base, channel
    );
    println!("Reconnecting with Last-Event-ID={}...", last_id);
    let (second_events, _) = subscribe_and_collect(&client, &reconnect_url, Some(&last_id), 2).await;
    for ce in &second_events {
        println!("  Resumed: seq={}", ce["data"]["seq"]);
    }
    println!("Reconnect-resume demonstration complete.");
    Ok(())
}

// Expected output:
// Publishing 4 events...
// First connection — receiving 2 events:
//   Received: seq=1
//   Received: seq=2
//   Last-Event-ID recorded: 2
// Reconnecting with Last-Event-ID=2...
//   Resumed: seq=3
//   Resumed: seq=4
// Reconnect-resume demonstration complete.
