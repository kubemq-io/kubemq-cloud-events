//! Example: queues/ack-all
//!
//! Sends 5 messages, peeks to verify count, calls ack_all to drain atomically,
//! then peeks again to confirm empty.
//!
//! Run: cargo run -p queues-ack-all

use cloudevents::{EventBuilder, EventBuilderV10};
use reqwest::Client;
use serde_json::{json, Value};
use std::env;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

async fn peek_count(client: &Client, base: &str, channel: &str, client_id: &str) -> i64 {
    let url = format!(
        "{}/ce/queue/receive?channel={}&client_id={}&max_messages=100&wait_timeout=3&is_peek=true",
        base, channel, client_id
    );
    let resp = client.post(&url).send().await.expect("peek");
    let r: Value = resp.json().await.expect("parse");
    r["data"]["messages_received"].as_i64().unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let channel = "rust-ce-queues.ack-all";
    let client_id = "kubemq-ce-rust-example";
    let client = Client::new();

    // Send 5 messages.
    println!("Sending 5 messages to queue...");
    for i in 1..=5u32 {
        let event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .ty("com.kubemq.examples.queues.ackall")
            .source(format!("urn:{}", client_id))
            .subject(channel)
            .data("application/json", json!({"n": i}))
            .build()?;
        let body = serde_json::to_string(&event)?;
        client.post(format!("{}/ce/queue/send", base))
            .header("Content-Type", "application/cloudevents+json")
            .body(body).send().await?;
    }

    println!("Peek before ack_all: {} messages", peek_count(&client, &base, channel, client_id).await);

    // ack_all.
    let ack_url = format!(
        "{}/ce/queue/ack_all?channel={}&client_id={}&wait_timeout=5",
        base, channel, client_id
    );
    let ack_resp = client.post(&ack_url).send().await?;
    let ack_result: Value = ack_resp.json().await?;
    println!("ack_all: is_error={} message={}", ack_result["is_error"], ack_result["message"]);

    println!("Peek after ack_all: {} messages remaining", peek_count(&client, &base, channel, client_id).await);
    Ok(())
}

// Expected output:
// Sending 5 messages to queue...
// Peek before ack_all: 5 messages
// ack_all: is_error=false message="OK"
// Peek after ack_all: 0 messages remaining
