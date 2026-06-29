//! Example: queues/peek
//!
//! Sends 3 messages, peeks twice (is_peek=true), then consumes (is_peek=false).
//!
//! Run: cargo run -p queues-peek

use cloudevents::{EventBuilder, EventBuilderV10};
use reqwest::Client;
use serde_json::{json, Value};
use std::env;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

async fn peek_or_receive(client: &Client, base: &str, channel: &str, client_id: &str, is_peek: bool) -> i64 {
    let url = format!(
        "{}/ce/queue/receive?channel={}&client_id={}&max_messages=10&wait_timeout=3&is_peek={}",
        base, channel, client_id, is_peek
    );
    let resp = client.post(&url).send().await.expect("receive");
    let r: Value = resp.json().await.expect("parse");
    r["data"]["messages_received"].as_i64().unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let channel = "rust-ce-queues.peek";
    let client_id = "kubemq-ce-rust-worker";
    let client = Client::new();

    println!("Sending 3 messages to queue...");
    for i in 1..=3u32 {
        let event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .ty("com.kubemq.examples.queues.peek")
            .source("urn:kubemq-ce-rust-example")
            .subject(channel)
            .data("application/json", json!({"n": i}))
            .build()?;
        let body = serde_json::to_string(&event)?;
        let resp = client.post(format!("{}/ce/queue/send", base))
            .header("Content-Type", "application/cloudevents+json")
            .body(body).send().await?;
        println!("  Sent message {}: status={}", i, resp.status());
    }

    let peek1 = peek_or_receive(&client, &base, channel, client_id, true).await;
    println!("[peek #1] messages_received={}", peek1);
    let peek2 = peek_or_receive(&client, &base, channel, client_id, true).await;
    println!("[peek #2] messages_received={}", peek2);
    let consumed = peek_or_receive(&client, &base, channel, client_id, false).await;
    println!("[consume] messages_received={}", consumed);
    Ok(())
}

// Expected output:
// Sending 3 messages to queue...
//   Sent message 1: status=202 Accepted
//   Sent message 2: status=202 Accepted
//   Sent message 3: status=202 Accepted
// [peek #1] messages_received=3
// [peek #2] messages_received=3
// [consume] messages_received=3
