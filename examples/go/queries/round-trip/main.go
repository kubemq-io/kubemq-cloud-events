// Example: queries/round-trip
//
// Demonstrates an RPC query round-trip:
//   - Responder subscribes via SSE GET /ce/subscribe/queries
//   - Sender sends a query via POST /ce/send/query (blocks for response)
//   - Responder extracts _kubemq_request_id and sends data response
//   - Sender receives the data payload in the query response
//
// Run: go run ./queries/round-trip/main.go
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

func startQueryResponder(base, channel string, ready chan<- struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	sseURL := fmt.Sprintf("%s/ce/subscribe/queries?client_id=go-query-responder&channel=%s",
		base, channel)
	req, _ := http.NewRequest("GET", sseURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("query responder SSE:", err)
	}
	defer resp.Body.Close()

	close(ready)

	scanner := bufio.NewScanner(resp.Body)
	var evType, data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if evType == "cloudevent" && data != "" {
				var raw map[string]interface{}
				_ = json.Unmarshal([]byte(data), &raw)

				requestID, _ := raw["_kubemq_request_id"].(string)
				replyChannel, _ := raw["_kubemq_reply_channel"].(string)

				// Extract the query payload.
				queryData, _ := raw["data"].(map[string]interface{})
				sku, _ := queryData["sku"].(string)
				fmt.Printf("[responder] query received: sku=%s request_id=%s\n", sku, requestID)

				// Send data response.
				sendQueryResponse(base, requestID, replyChannel, sku)
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

func sendQueryResponse(base, requestID, replyChannel, sku string) {
	// Simulate database lookup.
	inventory := map[string]int{"WIDGET-100": 42, "GADGET-200": 7}
	qty := inventory[sku]

	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.queries.inventory-result")
	event.SetSource("go-query-responder")
	event.SetSubject(replyChannel)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"sku":      sku,
		"quantity": qty,
	})

	body, _ := json.Marshal(event)
	url := fmt.Sprintf("%s/ce/send/response?request_id=%s", base, requestID)
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("send query response:", err)
	}
	defer resp.Body.Close()
	fmt.Println("[responder] response sent.")
}

func sendQuery(base, channel string) {
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.queries.inventory-check")
	event.SetSource("kubemq-ce-go-sender")
	event.SetSubject(channel)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]string{"sku": "WIDGET-100"})

	body, _ := json.Marshal(event)
	req, _ := http.NewRequest("POST", base+"/ce/send/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")

	fmt.Println("[sender] sending query for sku=WIDGET-100...")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("send query:", err)
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)

	// The response data contains the CE response from the responder.
	fmt.Printf("[sender] query response: status=%d is_error=%v\n",
		resp.StatusCode, result.IsError)
	fmt.Printf("[sender] response data: %s\n", string(result.Data))
}

func main() {
	base := serverURL()
	channel := "go-ce-queries.round-trip"

	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go startQueryResponder(base, channel, ready, &wg)

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		log.Fatal("Timed out waiting for query responder")
	}
	time.Sleep(100 * time.Millisecond)

	sendQuery(base, channel)
	wg.Wait()
}

// Expected output:
// [sender] sending query for sku=WIDGET-100...
// [responder] query received: sku=WIDGET-100 request_id=<uuid>
// [responder] response sent.
// [sender] query response: status=200 is_error=false
// [sender] response data: {"specversion":"1.0","type":"com.kubemq.examples.queries.inventory-result",...,"data":{"quantity":42,"sku":"WIDGET-100"}}
