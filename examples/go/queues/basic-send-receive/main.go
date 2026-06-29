// Example: queues/basic-send-receive
//
// Sends 3 messages to a KubeMQ queue channel, then polls to receive
// them one at a time via POST /ce/queue/receive.
//
// Run: go run ./queues/basic-send-receive/main.go
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

func sendToQueue(base, channel string, i int) {
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.queues.task")
	event.SetSource("kubemq-ce-go-example")
	event.SetSubject(channel)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"task_id":  i,
		"task":     "process-item",
		"priority": "normal",
	})

	body, _ := json.Marshal(event)
	req, _ := http.NewRequest("POST", base+"/ce/queue/send", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("queue send:", err)
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("Sent task %d: status=%d is_error=%v\n", i, resp.StatusCode, result.IsError)
}

func receiveFromQueue(base, channel, clientID string) {
	url := fmt.Sprintf("%s/ce/queue/receive?channel=%s&client_id=%s&max_messages=1&wait_timeout=5",
		base, channel, clientID)
	req, _ := http.NewRequest("POST", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("queue receive:", err)
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)
	if result.IsError {
		fmt.Printf("Receive error: %s\n", result.Message)
		return
	}

	var data QueueReceiveData
	_ = json.Unmarshal(result.Data, &data)
	if data.MessagesReceived == 0 {
		fmt.Println("No messages (queue empty)")
		return
	}
	for _, msg := range data.Messages {
		fmt.Printf("Received: type=%v data=%v\n", msg["type"], msg["data"])
	}
}

func main() {
	base := serverURL()
	channel := "go-ce-queues.basic"
	clientID := "kubemq-ce-go-worker"

	const numMessages = 3
	fmt.Printf("Sending %d messages to queue '%s':\n", numMessages, channel)
	for i := 1; i <= numMessages; i++ {
		sendToQueue(base, channel, i)
	}

	fmt.Printf("\nReceiving %d messages from queue:\n", numMessages)
	for i := 0; i < numMessages; i++ {
		receiveFromQueue(base, channel, clientID)
	}
}

// Expected output:
// Sending 3 messages to queue 'go-ce-queues.basic':
// Sent task 1: status=202 is_error=false
// Sent task 2: status=202 is_error=false
// Sent task 3: status=202 is_error=false
//
// Receiving 3 messages from queue:
// Received: type=com.kubemq.examples.queues.task data=map[priority:normal task:process-item task_id:1]
// Received: type=com.kubemq.examples.queues.task data=map[priority:normal task:process-item task_id:2]
// Received: type=com.kubemq.examples.queues.task data=map[priority:normal task:process-item task_id:3]
