//! Example: events/consumer-group — two subscribers in same group.

use bytes::Bytes;
use cloudevents::{EventBuilder, EventBuilderV10};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::json;
use std::env;
use tokio::sync::mpsc;
use uuid::Uuid;

fn server_url() -> String {
    env::var("KUBEMQ_CE_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

async fn subscribe_group(
    client: Client,
    url: String,
    tx: mpsc::Sender<String>,
    label: String,
) {
    let stream = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("SSE connect")
        .bytes_stream();

    let mut stream = Box::pin(stream);
    let mut ev_type = String::new();
    let mut data = String::new();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk: Bytes = chunk.unwrap();
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer = buffer[pos + 1..].to_string();
            if line.is_empty() {
                if ev_type == "cloudevent" && !data.is_empty() {
                    let _ = tx.send(format!("[{}] received: {}", label, &data[..data.len().min(80)])).await;
                    return;
                }
                ev_type.clear(); data.clear();
            } else if line.starts_with(':') {
            } else if let Some(v) = line.strip_prefix("event:") {
                ev_type = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("data:") {
                data = v.trim().to_string();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = server_url();
    let channel = "rust-ce-events.consumer-group";
    let group = "workers";
    let client = Client::new();
    let (tx, mut rx) = mpsc::channel::<String>(2);

    for i in 1..=2u32 {
        let url = format!("{}/ce/subscribe/events?client_id=rust-worker-{}&channel={}&group={}",
            base, i, channel, group);
        tokio::spawn(subscribe_group(client.clone(), url, tx.clone(), format!("worker-{}", i)));
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    for seq in 1..=2u32 {
        let event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .ty("com.kubemq.examples.events.grouped")
            .source("urn:kubemq-ce-rust-example")
            .subject(channel)
            .data("application/json", json!({"seq": seq}))
            .build()?;
        let body = serde_json::to_string(&event)?;
        let resp = client.post(format!("{}/ce/send/event", base))
            .header("Content-Type", "application/cloudevents+json")
            .body(body).send().await?;
        println!("Published event seq={} (status={})", seq, resp.status());
    }

    for _ in 0..2 {
        if let Some(msg) = tokio::time::timeout(
            tokio::time::Duration::from_secs(10), rx.recv()
        ).await.ok().flatten() {
            println!("{}", msg);
        }
    }

    Ok(())
}
