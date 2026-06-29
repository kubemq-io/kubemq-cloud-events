//! Example: routing/cesql-routing
//!
//! Publishes events with different type values for server-side CESQL routing.
//! Requires KubeMQ configured with CESQL routing rules.
//!
//! Run: cargo run -p cesql-routing

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

async fn start_subscriber(client: Client, base: String, channel: String, client_id: String, max: usize, tx: mpsc::Sender<String>) {
    let url = format!("{}/ce/subscribe/events?client_id={}&channel={}", base, client_id, channel);
    let stream = client.get(&url)
        .header("Accept", "text/event-stream")
        .send().await.expect("SSE connect").bytes_stream();
    let mut stream = Box::pin(stream);
    let mut ev_type = String::new(); let mut data = String::new();
    let mut buffer = String::new(); let mut count = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk: Bytes = chunk.unwrap_or_default();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer = buffer[pos + 1..].to_string();
            if line.is_empty() {
                if ev_type == "cloudevent" && !data.is_empty() {
                    let ce: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
                    let _ = tx.send(format!("  [{}] type={}", channel, ce["type"])).await;
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
    let client = Client::new();
    println!("CESQL Routing Example — Rust");
    println!("Requires KubeMQ with CESQL routing configured.\n");

    let (tx, mut rx) = mpsc::channel::<String>(20);

    // Subscribe to routed destination channels.
    tokio::spawn(start_subscriber(client.clone(), base.clone(), "order-archive".to_string(), "rust-order-sub".to_string(), 1, tx.clone()));
    tokio::spawn(start_subscriber(client.clone(), base.clone(), "alert-stream".to_string(),  "rust-alert-sub".to_string(), 1, tx.clone()));
    tokio::spawn(start_subscriber(client.clone(), base.clone(), "all-events".to_string(),    "rust-all-sub".to_string(),   3, tx.clone()));
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Publish events with different type values.
    let event_types = [
        "com.kubemq.examples.routing.order",
        "com.kubemq.examples.routing.alert",
        "com.kubemq.examples.routing.info",
    ];
    for ev_type in &event_types {
        let event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .ty(*ev_type)
            .source("urn:kubemq-ce-rust-cesql")
            .subject("routing-source")
            .data("application/json", json!({"description": format!("Event of type {}", ev_type)}))
            .build()?;
        let body = serde_json::to_string(&event)?;
        let resp = client.post(format!("{}/ce/send/event", base))
            .header("Content-Type", "application/cloudevents+json")
            .body(body).send().await?;
        println!("Published type={} (status={})", ev_type, resp.status());
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    while let Ok(msg) = rx.try_recv() { println!("{}", msg); }
    println!("\nCESQL routing demonstration complete.");
    Ok(())
}

// Expected output (with CESQL routing configured):
// CESQL Routing Example — Rust
// Requires KubeMQ with CESQL routing configured.
//
// Published type=com.kubemq.examples.routing.order (status=202 Accepted)
// Published type=com.kubemq.examples.routing.alert (status=202 Accepted)
// Published type=com.kubemq.examples.routing.info (status=202 Accepted)
//   [order-archive] type="com.kubemq.examples.routing.order"
//   [alert-stream] type="com.kubemq.examples.routing.alert"
//   [all-events] type="com.kubemq.examples.routing.order"
//   ...
//
// CESQL routing demonstration complete.
