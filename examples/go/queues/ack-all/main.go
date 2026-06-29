// Example: queues/ack-all
//
// Sends 5 messages to a queue, then uses POST /ce/queue/ack_all to
// drain all messages atomically without receiving them individually.
//
// Run: go run ./queues/ack-all/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func serverURL() string {
	if u := os.Getenv("KUBEMQ_CE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}

type CEResponse struct {
	IsError bool            `json:"is_error"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func main() {
	base := serverURL()
	channel := "go-ce-queues.ack-all"
	clientID := "kubemq-ce-go-example"
	const numMessages = 5

	// Send 5 messages to the queue.
	for i := 1; i <= numMessages; i++ {
		event := cloudevents.NewEvent()
		event.SetType("com.kubemq.examples.queues.ackall")
		event.SetSource("kubemq-ce-go-example")
		event.SetSubject(channel)
		_ = event.SetData(cloudevents.ApplicationJSON, map[string]int{"n": i})

		body, _ := json.Marshal(event)
		req, _ := http.NewRequest("POST", base+"/ce/queue/send", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/cloudevents+json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal("send:", err)
		}
		resp.Body.Close()
	}
	fmt.Printf("Sent %d messages to queue '%s'.\n", numMessages, channel)

	// Peek to confirm messages exist.
	peekURL := fmt.Sprintf("%s/ce/queue/receive?channel=%s&client_id=%s&max_messages=100&wait_timeout=3&is_peek=true",
		base, channel, clientID)
	req, _ := http.NewRequest("POST", peekURL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("peek:", err)
	}
	var peekResult CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&peekResult)
	resp.Body.Close()

	var peekData struct {
		MessagesReceived int `json:"messages_received"`
	}
	_ = json.Unmarshal(peekResult.Data, &peekData)
	fmt.Printf("Peek: %d messages in queue.\n", peekData.MessagesReceived)

	// Ack all — drain the queue atomically.
	ackURL := fmt.Sprintf("%s/ce/queue/ack_all?channel=%s&client_id=%s&wait_timeout=5",
		base, channel, clientID)
	req, _ = http.NewRequest("POST", ackURL, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("ack_all:", err)
	}
	var ackResult CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&ackResult)
	resp.Body.Close()
	fmt.Printf("ack_all: is_error=%v message=%s\n", ackResult.IsError, ackResult.Message)

	// Confirm queue is empty.
	req, _ = http.NewRequest("POST", peekURL, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("final peek:", err)
	}
	_ = json.NewDecoder(resp.Body).Decode(&peekResult)
	resp.Body.Close()
	_ = json.Unmarshal(peekResult.Data, &peekData)
	fmt.Printf("After ack_all: %d messages remaining.\n", peekData.MessagesReceived)
}

// Expected output:
// Sent 5 messages to queue 'go-ce-queues.ack-all'.
// Peek: 5 messages in queue.
// ack_all: is_error=false message=OK
// After ack_all: 0 messages remaining.
