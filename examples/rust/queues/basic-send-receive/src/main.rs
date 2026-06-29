//! Example: queues/basic-send-receive — send 3, receive one at a time.

use cloudevents::{EventBuilder, EventBuilderV10};
use reqwest::Client;
use serde_json::{json, Value};
use std::env;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let client = Client::new();
    let channel = "rust-ce-queues.basic";
    let client_id = "kubemq-ce-rust-worker";

    println!("Sending 3 messages to queue '{}':", channel);
    for i in 1..=3u32 {
        let event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .ty("com.kubemq.examples.queues.task")
            .source("urn:kubemq-ce-rust-example")
            .subject(channel)
            .data("application/json", json!({"task_id": i, "task": "process-item"}))
            .build()?;
        let body = serde_json::to_string(&event)?;
        let resp = client.post(format!("{}/ce/queue/send", base))
            .header("Content-Type", "application/cloudevents+json")
            .body(body).send().await?;
        let r: Value = resp.json().await?;
        println!("  Sent task {}: is_error={}", i, r["is_error"]);
    }

    println!("\nReceiving 3 messages:");
    for _ in 0..3 {
        let url = format!(
            "{}/ce/queue/receive?channel={}&client_id={}&max_messages=1&wait_timeout=5",
            base, channel, client_id
        );
        let resp = client.post(&url).send().await?;
        let r: Value = resp.json().await?;
        if r["is_error"].as_bool().unwrap_or(false) {
            println!("  Error: {}", r["message"]);
            continue;
        }
        let msgs = r["data"]["messages"].as_array().cloned().unwrap_or_default();
        if msgs.is_empty() {
            println!("  No messages");
        }
        for m in &msgs {
            println!("  Received: type={} data={}", m["type"], m["data"]);
        }
    }
    Ok(())
}
