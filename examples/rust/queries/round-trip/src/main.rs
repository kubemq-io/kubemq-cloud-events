//! Example: queries/round-trip
//!
//! Responder subscribes to queries, receives a query, sends a CE response.
//! Sender publishes via POST /ce/send/query (blocks until response arrives).
//!
//! Run: cargo run -p round-trip-queries

use bytes::Bytes;
use cloudevents::{EventBuilder, EventBuilderV10};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::{collections::HashMap, env};
use tokio::sync::oneshot;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

async fn run_responder(base: String, channel: String, ready_tx: oneshot::Sender<()>) {
    let client = Client::new();
    let url = format!("{}/ce/subscribe/queries?client_id=rust-query-responder&channel={}", base, channel);
    let resp = client.get(&url)
        .header("Accept", "text/event-stream")
        .send().await.expect("SSE connect");
    // Signal ready as soon as the SSE connection is established
    let _ = ready_tx.send(());
    let mut stream = Box::pin(resp.bytes_stream());
    let mut buffer = String::new();
    let mut ev_type = String::new(); let mut data_str = String::new();
    let inventory: HashMap<&str, u32> = [("WIDGET-100", 42), ("GADGET-200", 7)].into();

    while let Some(chunk) = stream.next().await {
        let chunk: Bytes = chunk.unwrap_or_default();
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer = buffer[pos + 1..].to_string();
            if line.is_empty() {
                if ev_type == "cloudevent" && !data_str.is_empty() {
                    let raw: Value = serde_json::from_str(&data_str).unwrap();
                    let request_id = raw["_kubemq_request_id"].as_str().unwrap_or("").to_string();
                    let reply_channel = raw["_kubemq_reply_channel"].as_str().unwrap_or("").to_string();
                    let sku = raw["data"]["sku"].as_str().unwrap_or("");
                    let qty = *inventory.get(sku).unwrap_or(&0);
                    println!("[responder] query sku={} qty={} request_id={}", sku, qty, request_id);

                    let resp_event = EventBuilderV10::new()
                        .id(Uuid::new_v4().to_string())
                        .ty("com.kubemq.examples.queries.inventory-result")
                        .source("urn:rust-query-responder")
                        .subject(reply_channel.as_str())
                        .data("application/json", json!({"sku": sku, "quantity": qty}))
                        .build().unwrap();
                    let body = serde_json::to_string(&resp_event).unwrap();
                    let r = client.post(format!("{}/ce/send/response?request_id={}", base, request_id))
                        .header("Content-Type", "application/cloudevents+json")
                        .body(body).send().await.unwrap();
                    let rj: Value = r.json().await.unwrap();
                    println!("[responder] response sent: is_error={}", rj["is_error"]);
                    return;
                }
                ev_type.clear(); data_str.clear();
            } else if line.starts_with(':') {
            } else if let Some(v) = line.strip_prefix("event:") { ev_type = v.trim().to_string(); }
            else if let Some(v) = line.strip_prefix("data:") { data_str = v.trim().to_string(); }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let channel = "rust-ce-queries.round-trip".to_string();
    let client = Client::new();

    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let base_clone = base.clone();
    let channel_clone = channel.clone();
    tokio::spawn(async move { run_responder(base_clone, channel_clone, ready_tx).await });

    tokio::time::timeout(tokio::time::Duration::from_secs(5), ready_rx).await??;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let event = EventBuilderV10::new()
        .id(Uuid::new_v4().to_string())
        .ty("com.kubemq.examples.queries.inventory-check")
        .source("urn:kubemq-ce-rust-sender")
        .subject(channel.as_str())
        .data("application/json", json!({"sku": "WIDGET-100"}))
        .build()?;
    let body = serde_json::to_string(&event)?;
    println!("[sender] sending query for sku=WIDGET-100...");
    let resp = client.post(format!("{}/ce/send/query", base))
        .header("Content-Type", "application/cloudevents+json")
        .body(body).send().await?;
    let result: Value = resp.json().await?;
    println!("[sender] query response: status=200 is_error={}", result["is_error"]);
    println!("[sender] response data: {}", result["data"]);
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    Ok(())
}

// Expected output:
// [responder] query sku=WIDGET-100 qty=42 request_id=...
// [responder] response sent: is_error=false
// [sender] sending query for sku=WIDGET-100...
// [sender] query response: status=200 is_error=false
// [sender] response data: {"sku":"WIDGET-100","quantity":42}
