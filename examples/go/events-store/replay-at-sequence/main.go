// Example: events-store/replay-at-sequence
//
// Publishes 10 events, then subscribes with StartAtSequence
// (events_store_type=4, events_store_value=5) to receive only
// events with sequence >= 5.
//
// Run: go run ./events-store/replay-at-sequence/main.go
package main

import (
	"bufio"
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
	channel := "go-ce-events-store.replay-at-sequence"
	const totalEvents = 10
	const startSeq = 5 // replay from sequence 5

	// Publish 10 events.
	for i := 1; i <= totalEvents; i++ {
		event := cloudevents.NewEvent()
		event.SetType("com.kubemq.examples.eventsstore.seqreplay")
		event.SetSource("kubemq-ce-go-example")
		event.SetSubject(channel)
		_ = event.SetData(cloudevents.ApplicationJSON, map[string]int{"seq": i})

		body, _ := json.Marshal(event)
		req, _ := http.NewRequest("POST", base+"/ce/send/event-store",
			strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/cloudevents+json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal("send:", err)
		}
		resp.Body.Close()
		fmt.Printf("Published event %d/%d\n", i, totalEvents)
	}

	time.Sleep(200 * time.Millisecond)

	// Subscribe with StartAtSequence=5.
	// events_store_type=4, events_store_value=5
	sseURL := fmt.Sprintf(
		"%s/ce/subscribe/events-store?client_id=go-replay-seq-sub&channel=%s&events_store_type=4&events_store_value=%d",
		base, channel, startSeq)

	fmt.Printf("\nSubscribing with StartAtSequence=%d (expecting events %d-%d):\n",
		startSeq, startSeq, totalEvents)

	req, _ := http.NewRequest("GET", sseURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("SSE connect:", err)
	}
	defer resp.Body.Close()

	expected := totalEvents - startSeq + 1
	count := 0
	scanner := bufio.NewScanner(resp.Body)
	var evType, data, sseID string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if evType == "cloudevent" && data != "" {
				var ce map[string]interface{}
				_ = json.Unmarshal([]byte(data), &ce)
				count++
				fmt.Printf("  [seq=%s] data=%v\n", sseID, ce["data"])
				if count == expected {
					break
				}
			}
			if evType == "error" {
				log.Printf("SSE error: %s", data)
				break
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

	fmt.Printf("\nReceived %d events starting from sequence %d.\n", count, startSeq)
}

// Expected output:
// Published event 1/10
// ...
// Published event 10/10
//
// Subscribing with StartAtSequence=5 (expecting events 5-10):
//   [seq=5] data=map[seq:5]
//   [seq=6] data=map[seq:6]
//   [seq=7] data=map[seq:7]
//   [seq=8] data=map[seq:8]
//   [seq=9] data=map[seq:9]
//   [seq=10] data=map[seq:10]
//
// Received 6 events starting from sequence 5.
