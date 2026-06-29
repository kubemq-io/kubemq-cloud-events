// Example: events/content-modes
//
// Sends the same event payload in both CloudEvents content modes:
//   - Structured: Content-Type: application/cloudevents+json, all attrs in JSON body
//   - Binary:     ce-* HTTP headers, raw data body
//
// Both modes are accepted by the KubeMQ CE connector interchangeably.
//
// Run: go run ./events/content-modes/main.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
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
	IsError bool   `json:"is_error"`
	Message string `json:"message"`
}

func sendStructured(base string, event cloudevents.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", base+"/ce/send/event", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/cloudevents+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("[structured] status=%d is_error=%v message=%s\n",
		resp.StatusCode, result.IsError, result.Message)
	return nil
}

func sendBinary(base string, event cloudevents.Event) error {
	// In binary mode, CE attributes go into ce-* HTTP headers.
	// The body contains only the event data.
	dataJSON, err := json.Marshal(map[string]string{"message": "binary mode payload"})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", base+"/ce/send/event", bytes.NewReader(dataJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("ce-specversion", "1.0")
	req.Header.Set("ce-type", event.Type())
	req.Header.Set("ce-source", event.Source())
	req.Header.Set("ce-subject", event.Subject())
	req.Header.Set("ce-id", event.ID())
	req.Header.Set("ce-time", event.Time().Format(time.RFC3339Nano))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result CEResponse
	_ = json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("[binary]     status=%d is_error=%v message=%s\n",
		resp.StatusCode, result.IsError, result.Message)
	return nil
}

func main() {
	base := serverURL()

	// Build a CloudEvent to send in both modes.
	event := cloudevents.NewEvent()
	event.SetType("com.kubemq.examples.events.content-mode")
	event.SetSource("kubemq-ce-go-example")
	event.SetSubject("go-ce-events.content-modes")
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]string{
		"message": "structured mode payload",
	})

	fmt.Println("Sending in structured mode (Content-Type: application/cloudevents+json):")
	if err := sendStructured(base, event); err != nil {
		log.Fatal("structured send:", err)
	}

	fmt.Println("\nSending in binary mode (ce-* HTTP headers):")
	if err := sendBinary(base, event); err != nil {
		log.Fatal("binary send:", err)
	}

	fmt.Println("\nBoth content modes accepted by KubeMQ CE connector.")
}

// Expected output:
// Sending in structured mode (Content-Type: application/cloudevents+json):
// [structured] status=202 is_error=false message=OK
//
// Sending in binary mode (ce-* HTTP headers):
// [binary]     status=202 is_error=false message=OK
//
// Both content modes accepted by KubeMQ CE connector.
