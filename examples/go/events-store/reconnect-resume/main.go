// Example: events-store/reconnect-resume
//
// Demonstrates SSE reconnection with Last-Event-ID for events-store.
// 1. Subscribe, receive the first 3 of 6 events, capture the last SSE id.
// 2. Disconnect (close the response body).
// 3. Reconnect with Last-Event-ID header — receive the remaining 3 events.
//
// Run: go run ./events-store/reconnect-resume/main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
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

func publishAll(base, channel string, count int) {
	for i := 1; i <= count; i++ {
		event := cloudevents.NewEvent()
		event.SetType("com.kubemq.examples.eventsstore.reconnect")
		event.SetSource("kubemq-ce-go-example")
		event.SetSubject(channel)
		_ = event.SetData(cloudevents.ApplicationJSON, map[string]int{"n": i})

		body, _ := json.Marshal(event)
		req, _ := http.NewRequest("POST", base+"/ce/send/event-store",
			strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/cloudevents+json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal("send:", err)
		}
		resp.Body.Close()
	}
	fmt.Printf("Published %d events to events-store.\n", count)
}

// readN reads exactly n cloudevents from an SSE stream, returning the last SSE id.
func readN(body io.ReadCloser, n int) (lastID string) {
	scanner := bufio.NewScanner(body)
	var evType, data, sseID string
	received := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if evType == "cloudevent" && data != "" {
				received++
				lastID = sseID
				var ce map[string]interface{}
				_ = json.Unmarshal([]byte(data), &ce)
				fmt.Printf("  [%d] id=%s data=%v\n", received, sseID, ce["data"])
				if received == n {
					return lastID
				}
			}
			evType, data, sseID = "", "", ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "id:") {
			sseID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		} else if strings.HasPrefix(line, "event:") {
			evType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
	return lastID
}

func openSSE(base, channel, clientID, lastEventID string) (*http.Response, error) {
	// When reconnecting, omit events_store_type so Last-Event-ID takes precedence.
	var sseURL string
	if lastEventID == "" {
		sseURL = fmt.Sprintf(
			"%s/ce/subscribe/events-store?client_id=%s&channel=%s&events_store_type=2",
			base, clientID, channel)
	} else {
		sseURL = fmt.Sprintf(
			"%s/ce/subscribe/events-store?client_id=%s&channel=%s",
			base, clientID, channel)
	}

	req, _ := http.NewRequest("GET", sseURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	client := &http.Client{Timeout: 0}
	return client.Do(req)
}

func main() {
	base := serverURL()
	channel := "go-ce-events-store.reconnect-resume"
	const totalEvents = 6
	const firstBatch = 3

	// Publish all events.
	publishAll(base, channel, totalEvents)
	time.Sleep(200 * time.Millisecond)

	// First connection: receive first batch.
	fmt.Printf("\nFirst connection (StartFromFirst, reading first %d events):\n", firstBatch)
	resp1, err := openSSE(base, channel, "go-reconnect-sub", "")
	if err != nil {
		log.Fatal("first connect:", err)
	}
	lastID := readN(resp1.Body, firstBatch)
	resp1.Body.Close()
	fmt.Printf("Disconnected. Last-Event-ID captured: %s\n", lastID)

	// Second connection: resume from lastID.
	fmt.Printf("\nReconnecting with Last-Event-ID: %s\n", lastID)
	resp2, err := openSSE(base, channel, "go-reconnect-sub", lastID)
	if err != nil {
		log.Fatal("reconnect:", err)
	}
	defer resp2.Body.Close()

	remaining := totalEvents - firstBatch
	fmt.Printf("Receiving remaining %d events:\n", remaining)
	readN(resp2.Body, remaining)

	fmt.Println("\nReconnect-resume demonstration complete.")
}

// Expected output:
// Published 6 events to events-store.
//
// First connection (StartFromFirst, reading first 3 events):
//   [1] id=1 data=map[n:1]
//   [2] id=2 data=map[n:2]
//   [3] id=3 data=map[n:3]
// Disconnected. Last-Event-ID captured: 3
//
// Reconnecting with Last-Event-ID: 3
// Receiving remaining 3 events:
//   [1] id=4 data=map[n:4]
//   [2] id=5 data=map[n:5]
//   [3] id=6 data=map[n:6]
//
// Reconnect-resume demonstration complete.
