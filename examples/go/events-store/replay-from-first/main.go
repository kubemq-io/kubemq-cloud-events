// Example: events-store/replay-from-first
//
// Publishes 5 events to an events-store channel, then subscribes with
// StartFromFirst (events_store_type=2) to replay all stored events.
//
// Run: go run ./events-store/replay-from-first/main.go
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

func publishEvents(base, channel string, count int) {
	for i := 1; i <= count; i++ {
		event := cloudevents.NewEvent()
		event.SetType("com.kubemq.examples.eventsstore.replay")
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
		fmt.Printf("Published event %d of %d\n", i, count)
	}
}

func subscribeFromFirst(base, channel, clientID string, expected int) {
	// events_store_type=2 = StartFromFirst: replay all stored events.
	sseURL := fmt.Sprintf(
		"%s/ce/subscribe/events-store?client_id=%s&channel=%s&events_store_type=2",
		base, clientID, channel)
	req, _ := http.NewRequest("GET", sseURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("SSE connect:", err)
	}
	defer resp.Body.Close()

	fmt.Printf("\nSubscribed with StartFromFirst — replaying all %d events:\n", expected)

	count := 0
	scanner := bufio.NewScanner(resp.Body)
	var evType, data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if evType == "cloudevent" && data != "" {
				var ce map[string]interface{}
				_ = json.Unmarshal([]byte(data), &ce)
				count++
				fmt.Printf("  [%d/%d] data=%v\n", count, expected, ce["data"])
				if count == expected {
					return
				}
			}
			if evType == "error" {
				log.Printf("SSE error: %s", data)
				return
			}
			evType, data = "", ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			evType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

func main() {
	base := serverURL()
	channel := "go-ce-events-store.replay-from-first"
	const numEvents = 5

	// Publish all events first.
	publishEvents(base, channel, numEvents)

	// Wait briefly for persistence.
	time.Sleep(200 * time.Millisecond)

	// Subscribe after publishing — StartFromFirst replays from the beginning.
	subscribeFromFirst(base, channel, "kubemq-ce-go-replay-sub", numEvents)

	fmt.Println("\nAll stored events replayed successfully.")
}

// Expected output:
// Published event 1 of 5
// Published event 2 of 5
// Published event 3 of 5
// Published event 4 of 5
// Published event 5 of 5
//
// Subscribed with StartFromFirst — replaying all 5 events:
//   [1/5] data=map[seq:1]
//   [2/5] data=map[seq:2]
//   [3/5] data=map[seq:3]
//   [4/5] data=map[seq:4]
//   [5/5] data=map[seq:5]
//
// All stored events replayed successfully.
