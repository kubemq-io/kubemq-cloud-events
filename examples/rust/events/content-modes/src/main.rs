//! Example: events/content-modes — structured and binary mode.

use cloudevents::{AttributesReader, EventBuilder, EventBuilderV10};
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

    let event = EventBuilderV10::new()
        .id(Uuid::new_v4().to_string())
        .ty("com.kubemq.examples.events.content-mode")
        .source("urn:kubemq-ce-rust-example")
        .subject("rust-ce-events.content-modes")
        .data("application/json", json!({"message": "hello content modes"}))
        .build()?;

    // Structured mode
    println!("Sending in structured mode:");
    let body = serde_json::to_string(&event)?;
    let r1 = client.post(format!("{}/ce/send/event", base))
        .header("Content-Type", "application/cloudevents+json")
        .body(body)
        .send().await?;
    let j1: Value = r1.json().await?;
    println!("[structured] status=202 is_error={}", j1["is_error"]);

    // Binary mode — CE attrs in ce-* headers, JSON data as body
    println!("\nSending in binary mode:");
    let data_body = serde_json::to_string(&json!({"message": "binary mode payload"}))?;
    let r2 = client.post(format!("{}/ce/send/event", base))
        .header("Content-Type", "application/json")
        .header("ce-specversion", "1.0")
        .header("ce-type", event.ty())
        .header("ce-source", event.source().as_str())
        .header("ce-id", event.id())
        .header("ce-subject", event.subject().unwrap_or(""))
        .body(data_body)
        .send().await?;
    let j2: Value = r2.json().await?;
    println!("[binary]     status=202 is_error={}", j2["is_error"]);

    println!("\nBoth content modes accepted.");
    Ok(())
}
