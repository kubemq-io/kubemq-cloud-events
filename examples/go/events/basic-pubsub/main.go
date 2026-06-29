// Example: events/basic-pubsub
//
// Demonstrates basic fire-and-forget event pub/sub via the KubeMQ
// CloudEvents HTTP connector.
//
// A subscriber opens an SSE stream on the events channel.
// A publisher sends a single CloudEvent via POST /ce/send/event.
// The subscriber prints the received event and the program exits.
//
// Channel:  go-ce-events.basic-pubsub
// CE type:  com.kubemq.examples.events.sent
// CE source: kubemq-ce-go-example
//
// Run: go run ./events/basic-pubsub/main.go
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func serverURL() string {
	if u := os.Getenv("KUBEMQ_CE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}

func main() {
	base := serverURL()
	channel := "go-ce-events.basic-pubsub"
	clientID := "kubemq-ce-go-example"

	received := make(chan string, 1)

	// Start SSE subscriber in background goroutine.
	go func() {
		sseURL := fmt.Sprintf("%s/ce/subscribe/events?client_id=%s&channel=%s",
			base, clientID+"-sub", channel)
		req, err := http.NewRequest("GET", sseURL, nil)
		if err != nil {
			log.Fatal("create SSE request:", err)
		}
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal("SSE connect:", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Fatalf("SSE connect failed: %d", resp.StatusCode)
		}

		scanner := bufio.NewScanner(resp.Body)
		var eventType, data string
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if eventType == "cloudevent" && data != "" {
					received <- data
					return
				}
				eventType, data = "", ""
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue // keepalive
			}
			if strings.HasPrefix(line, "event:") {
				eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			} else if strings.HasPrefix(line, "data:") {
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		if err := scanner.Err(); err != nil {
			log.Println("SSE read error:", err)
		}
	}()

	// Allow SSE subscription to establish.
	time.Sleep(500 * time.Millisecond)

	// Build and send CloudEvent (structured mode).
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.events.sent")
	event.SetSource("kubemq-ce-go-example")
	event.SetSubject(channel) // subject = KubeMQ channel
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]string{
		"message": "Hello from Go CloudEvents example!",
	})

	body, err := json.Marshal(event)
	if err != nil {
		log.Fatal("marshal event:", err)
	}

	req, err := http.NewRequest("POST", base+"/ce/send/event", strings.NewReader(string(body)))
	if err != nil {
		log.Fatal("create POST request:", err)
	}
	req.Header.Set("Content-Type", "application/cloudevents+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("send event:", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("Published: status=%d is_error=%v\n", resp.StatusCode, result["is_error"])

	// Wait for subscriber to receive the event.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case data := <-received:
		var ce map[string]interface{}
		_ = json.Unmarshal([]byte(data), &ce)
		fmt.Printf("Received event:\n")
		fmt.Printf("  type:    %v\n", ce["type"])
		fmt.Printf("  source:  %v\n", ce["source"])
		fmt.Printf("  subject: %v\n", ce["subject"])
		fmt.Printf("  data:    %v\n", ce["data"])
	case <-ctx.Done():
		log.Fatal("Timed out waiting for event")
	}
}

// Expected output:
// Published: status=202 is_error=false
// Received event:
//   type:    com.kubemq.examples.events.sent
//   source:  kubemq-ce-go-example
//   subject: go-ce-events.basic-pubsub
//   data:    map[message:Hello from Go CloudEvents example!]
