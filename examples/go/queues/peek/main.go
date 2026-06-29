// Example: queues/peek
//
// Sends 3 messages to a queue, then peeks (is_peek=true) to inspect
// them without consuming. A final normal receive confirms messages remain.
//
// Run: go run ./queues/peek/main.go
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

type QueueReceiveData struct {
	MessagesReceived int                      `json:"messages_received"`
	Messages         []map[string]interface{} `json:"messages"`
}

func sendToQueue(base, channel string, count int) {
	for i := 1; i <= count; i++ {
		event := cloudevents.NewEvent()
		event.SetType("com.kubemq.examples.queues.peek")
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
	fmt.Printf("Sent %d messages to queue.\n", count)
}

func receiveOrPeek(base, channel, clientID string, isPeek bool, label string) int {
	isPeakStr := "false"
	if isPeek {
		isPeakStr = "true"
	}
	url := fmt.Sprintf(
		"%s/ce/queue/receive?channel=%s&client_id=%s&max_messages=10&wait_timeout=3&is_peek=%s",
		base, channel, clientID, isPeakStr)
	req, _ := http.NewRequest("POST", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("receive:", err)
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)
	if result.IsError {
		fmt.Printf("[%s] error: %s\n", label, result.Message)
		return 0
	}

	var data QueueReceiveData
	_ = json.Unmarshal(result.Data, &data)
	fmt.Printf("[%s] messages_received=%d\n", label, data.MessagesReceived)
	return data.MessagesReceived
}

func main() {
	base := serverURL()
	channel := "go-ce-queues.peek"

	sendToQueue(base, channel, 3)

	fmt.Println("\nPeeking (is_peek=true) — messages NOT consumed:")
	n1 := receiveOrPeek(base, channel, "go-peek-client", true, "peek #1")

	fmt.Println("\nPeeking again — same messages still in queue:")
	n2 := receiveOrPeek(base, channel, "go-peek-client", true, "peek #2")

	fmt.Println("\nNormal receive — messages consumed:")
	n3 := receiveOrPeek(base, channel, "go-peek-client", false, "consume")

	fmt.Printf("\nPeek1=%d Peek2=%d Consumed=%d (peek does not remove messages)\n", n1, n2, n3)
}

// Expected output:
// Sent 3 messages to queue.
//
// Peeking (is_peek=true) — messages NOT consumed:
// [peek #1] messages_received=3
//
// Peeking again — same messages still in queue:
// [peek #2] messages_received=3
//
// Normal receive — messages consumed:
// [consume] messages_received=3
//
// Peek1=3 Peek2=3 Consumed=3 (peek does not remove messages)
