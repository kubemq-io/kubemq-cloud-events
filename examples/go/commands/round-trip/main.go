// Example: commands/round-trip
//
// Demonstrates an RPC command round-trip:
//   - Responder goroutine subscribes via SSE GET /ce/subscribe/commands
//   - Sender goroutine sends a command via POST /ce/send/command
//   - Responder extracts _kubemq_request_id and _kubemq_reply_channel
//   - Responder sends back a response via POST /ce/send/response?request_id=...
//   - Sender receives the execution acknowledgement in the POST response body
//
// Run: go run ./commands/round-trip/main.go
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

type CEResponse struct {
	IsError bool            `json:"is_error"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// startResponder subscribes to commands and replies to each one.
func startResponder(base, channel string, ready chan<- struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	sseURL := fmt.Sprintf("%s/ce/subscribe/commands?client_id=go-cmd-responder&channel=%s",
		base, channel)
	req, _ := http.NewRequest("GET", sseURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("responder SSE connect:", err)
	}
	defer resp.Body.Close()

	close(ready) // signal that SSE is connected

	scanner := bufio.NewScanner(resp.Body)
	var evType, data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if evType == "cloudevent" && data != "" {
				// Parse command to extract KubeMQ correlation fields.
				var raw map[string]interface{}
				_ = json.Unmarshal([]byte(data), &raw)

				requestID, _ := raw["_kubemq_request_id"].(string)
				replyChannel, _ := raw["_kubemq_reply_channel"].(string)
				fmt.Printf("[responder] command received: type=%v request_id=%s\n",
					raw["type"], requestID)

				// Send response back.
				sendResponse(base, requestID, replyChannel)
				return
			}
			if evType == "error" {
				log.Printf("[responder] SSE error: %s", data)
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

func sendResponse(base, requestID, replyChannel string) {
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.commands.response")
	event.SetSource("go-cmd-responder")
	event.SetSubject(replyChannel) // subject = reply channel
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"executed": true,
		"status":   "command processed successfully",
	})

	body, _ := json.Marshal(event)
	url := fmt.Sprintf("%s/ce/send/response?request_id=%s", base, requestID)
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("send response:", err)
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("[responder] response sent: is_error=%v\n", result.IsError)
}

func sendCommand(base, channel string) {
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.commands.reboot")
	event.SetSource("kubemq-ce-go-sender")
	event.SetSubject(channel)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]string{
		"device_id": "sensor-42",
		"action":    "reboot",
	})

	body, _ := json.Marshal(event)
	req, _ := http.NewRequest("POST", base+"/ce/send/command", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")

	fmt.Println("[sender] sending command...")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("send command:", err)
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("[sender] command ack received: status=%d is_error=%v data=%s\n",
		resp.StatusCode, result.IsError, string(result.Data))
}

func main() {
	base := serverURL()
	channel := "go-ce-commands.round-trip"

	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go startResponder(base, channel, ready, &wg)

	// Wait for SSE to be established.
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		log.Fatal("Timed out waiting for responder to connect")
	}
	time.Sleep(100 * time.Millisecond)

	// Send command — blocks until response arrives or timeout.
	sendCommand(base, channel)
	wg.Wait()
}

// Expected output:
// [sender] sending command...
// [responder] command received: type=com.kubemq.examples.commands.reboot request_id=<uuid>
// [responder] response sent: is_error=false
// [sender] command ack received: status=202 is_error=false data={...}
