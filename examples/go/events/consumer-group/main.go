// Example: events/consumer-group
//
// Demonstrates load-balanced subscriber groups. Two subscribers join the
// same group ("workers"). When two events are published, each subscriber
// receives exactly one — demonstrating that the server distributes
// messages across the group.
//
// Run: go run ./events/consumer-group/main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func serverURL() string {
	if u := os.Getenv("KUBEMQ_CE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}

func subscribe(base, clientID, channel, group string, results chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	sseURL := fmt.Sprintf("%s/ce/subscribe/events?client_id=%s&channel=%s&group=%s",
		base, clientID, channel, group)
	req, _ := http.NewRequest("GET", sseURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// Use a client with no timeout for SSE.
	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[%s] SSE connect error: %v", clientID, err)
		return
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	var eventType, data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if eventType == "cloudevent" && data != "" {
				results <- fmt.Sprintf("[%s] received: %s", clientID, data)
				return
			}
			if eventType == "error" {
				log.Printf("[%s] SSE error: %s", clientID, data)
				return
			}
			eventType, data = "", ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

func sendEvent(base, channel string, seq int) {
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.events.grouped")
	event.SetSource("kubemq-ce-go-example")
	event.SetSubject(channel)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]int{"seq": seq})

	body, _ := json.Marshal(event)
	req, _ := http.NewRequest("POST", base+"/ce/send/event", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("send event:", err)
	}
	resp.Body.Close()
	fmt.Printf("Published event seq=%d\n", seq)
}

func main() {
	base := serverURL()
	channel := "go-ce-events.consumer-group"
	group := "workers"

	results := make(chan string, 2)
	var wg sync.WaitGroup

	// Start two subscribers in the same group.
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go subscribe(base, fmt.Sprintf("worker-%d", i), channel, group, results, &wg)
	}

	// Allow subscriptions to establish.
	time.Sleep(600 * time.Millisecond)

	// Publish two events — each subscriber should get exactly one.
	sendEvent(base, channel, 1)
	sendEvent(base, channel, 2)

	// Collect results with a deadline.
	deadline := time.After(10 * time.Second)
	received := 0
	for received < 2 {
		select {
		case msg := <-results:
			fmt.Println(msg)
			received++
		case <-deadline:
			log.Fatal("Timed out waiting for events")
		}
	}
}

// Expected output (order may vary):
// Published event seq=1
// Published event seq=2
// [worker-1] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
// [worker-2] received: {"specversion":"1.0","type":"com.kubemq.examples.events.grouped",...}
