// Example: events-store/basic
//
// Publishes a single event to an events-store channel, then subscribes
// with StartNewOnly (events_store_type=1) to receive only new messages.
//
// Run: go run ./events-store/basic/main.go
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

func sendEventStore(base, channel string, msg string) {
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.eventsstore.stored")
	event.SetSource("kubemq-ce-go-example")
	event.SetSubject(channel)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]string{"msg": msg})

	body, _ := json.Marshal(event)
	req, _ := http.NewRequest("POST", base+"/ce/send/event-store", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("send:", err)
	}
	resp.Body.Close()
	fmt.Printf("Published to events-store: %s\n", msg)
}

func main() {
	base := serverURL()
	channel := "go-ce-events-store.basic"
	clientID := "kubemq-ce-go-example"

	received := make(chan string, 1)

	// Subscribe with StartNewOnly (type=1) — only messages arriving after subscribe.
	go func() {
		sseURL := fmt.Sprintf(
			"%s/ce/subscribe/events-store?client_id=%s&channel=%s&events_store_type=1",
			base, clientID+"-sub", channel)
		req, _ := http.NewRequest("GET", sseURL, nil)
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")

		client := &http.Client{Timeout: 0}
		resp, err := client.Do(req)
		if err != nil {
			log.Fatal("SSE connect:", err)
		}
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		var evType, data string
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if evType == "cloudevent" && data != "" {
					received <- data
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
	}()

	time.Sleep(500 * time.Millisecond)
	sendEventStore(base, channel, "hello events-store!")

	select {
	case data := <-received:
		var ce map[string]interface{}
		_ = json.Unmarshal([]byte(data), &ce)
		fmt.Printf("Received from events-store:\n")
		fmt.Printf("  type:    %v\n", ce["type"])
		fmt.Printf("  data:    %v\n", ce["data"])
	case <-time.After(10 * time.Second):
		log.Fatal("Timed out")
	}
}

// Expected output:
// Published to events-store: hello events-store!
// Received from events-store:
//   type:    com.kubemq.examples.eventsstore.stored
//   data:    map[msg:hello events-store!]
