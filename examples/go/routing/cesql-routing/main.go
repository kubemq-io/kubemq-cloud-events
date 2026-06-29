// Example: routing/cesql-routing
//
// Demonstrates KubeMQ CESQL routing with CloudEvents.
// CESQL routing is SERVER-SIDE configuration — this example shows:
//   1. How to configure routing rules in KubeMQ TOML config
//   2. How to publish events whose attributes match CESQL expressions
//   3. How to verify routing worked by subscribing to the target channels
//
// Prerequisite: KubeMQ server configured with the routing rules shown below.
//
// Run: go run ./routing/cesql-routing/main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
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

// NOTE: Configure KubeMQ with the following routing rules before running:
//
// [Routing]
//   Enable = true
//   Data = '[
//     {"key":"type = '\''com.kubemq.examples.routing.order'\''","keyType":"cesql","routes":"events:order-archive"},
//     {"key":"type = '\''com.kubemq.examples.routing.alert'\''","keyType":"cesql","routes":"events:alert-stream"},
//     {"key":"type LIKE '\''com.kubemq.examples.routing.%'\''","keyType":"cesql","routes":"events:all-events"}
//   ]'
//
// Template substitution example:
//   {"key":"type LIKE 'com.kubemq.examples.routing.%'","keyType":"cesql","routes":"events:{ce_type}"}
// Routes the event to a channel named after the event type.

func sendEvent(base, evType, channel string) {
	event := cloudevents.NewEvent()
	event.SetType(evType)
	event.SetSource("kubemq-ce-go-cesql")
	event.SetSubject(channel)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]string{
		"description": fmt.Sprintf("Event of type %s", evType),
	})

	body, _ := json.Marshal(event)
	req, _ := http.NewRequest("POST", base+"/ce/send/event", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/cloudevents+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("send error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("Published type=%s to channel=%s (status=%d)\n", evType, channel, resp.StatusCode)
}

func subscribeAndPrint(base, channel, clientID string, maxMsgs int) {
	sseURL := fmt.Sprintf("%s/ce/subscribe/events?client_id=%s&channel=%s",
		base, clientID, channel)
	req, _ := http.NewRequest("GET", sseURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("[%s] subscribe error: %v\n", channel, err)
		return
	}
	defer resp.Body.Close()

	count := 0
	scanner := bufio.NewScanner(resp.Body)
	var evType, data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if evType == "cloudevent" && data != "" {
				var ce map[string]interface{}
				_ = json.Unmarshal([]byte(data), &ce)
				fmt.Printf("  [%s] type=%v\n", channel, ce["type"])
				count++
				if count >= maxMsgs {
					return
				}
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

	fmt.Println(`CESQL Routing Example
=====================
This example requires KubeMQ to be configured with CESQL routing rules.
See the comments in main.go for the required server configuration.

Publishing events with different types to the source channel 'routing-source'.
CESQL rules will route them to:
  - com.kubemq.examples.routing.order  -> events:order-archive + events:all-events
  - com.kubemq.examples.routing.alert  -> events:alert-stream  + events:all-events
  - com.kubemq.examples.routing.info   -> events:all-events only
`)

	// Subscribe to routed destination channels.
	go subscribeAndPrint(base, "order-archive", "go-cesql-order-sub", 1)
	go subscribeAndPrint(base, "alert-stream", "go-cesql-alert-sub", 1)
	go subscribeAndPrint(base, "all-events", "go-cesql-all-sub", 3)

	time.Sleep(500 * time.Millisecond)

	// Publish events — CESQL rules on the server route them to target channels.
	sendEvent(base, "com.kubemq.examples.routing.order", "routing-source")
	sendEvent(base, "com.kubemq.examples.routing.alert", "routing-source")
	sendEvent(base, "com.kubemq.examples.routing.info", "routing-source")

	// Allow routing to complete.
	time.Sleep(2 * time.Second)
	fmt.Println("\nCESQL routing demonstration complete.")
}

// Expected output (with routing rules configured):
// CESQL Routing Example
// ...
// Published type=com.kubemq.examples.routing.order to channel=routing-source (status=202)
// Published type=com.kubemq.examples.routing.alert to channel=routing-source (status=202)
// Published type=com.kubemq.examples.routing.info to channel=routing-source (status=202)
//   [order-archive] type=com.kubemq.examples.routing.order
//   [alert-stream] type=com.kubemq.examples.routing.alert
//   [all-events] type=com.kubemq.examples.routing.order
//   [all-events] type=com.kubemq.examples.routing.alert
//   [all-events] type=com.kubemq.examples.routing.info
//
// CESQL routing demonstration complete.
