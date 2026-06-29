package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

func BuildStructuredRequest(endpoint string, event cloudevents.Event) (*http.Request, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/cloudevents+json")
	return req, nil
}

func BuildBinaryRequest(endpoint string, event cloudevents.Event) (*http.Request, error) {
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(event.Data()))
	if err != nil {
		return nil, err
	}
	ct := event.DataContentType()
	if ct == "" {
		ct = "application/json"
	}
	req.Header.Set("Content-Type", ct)
	req.Header.Set("ce-specversion", event.SpecVersion())
	req.Header.Set("ce-type", event.Type())
	req.Header.Set("ce-source", event.Source())
	req.Header.Set("ce-id", event.ID())
	if event.Subject() != "" {
		req.Header.Set("ce-subject", event.Subject())
	}
	if !event.Time().IsZero() {
		req.Header.Set("ce-time", event.Time().Format(time.RFC3339Nano))
	}
	for k, v := range event.Extensions() {
		req.Header.Set("ce-"+k, fmt.Sprintf("%v", v))
	}
	return req, nil
}

func BuildRequest(endpoint string, event cloudevents.Event, seq uint64, mode string) (*http.Request, error) {
	switch mode {
	case "structured":
		return BuildStructuredRequest(endpoint, event)
	case "binary":
		return BuildBinaryRequest(endpoint, event)
	case "alternating":
		if seq%2 == 0 {
			return BuildStructuredRequest(endpoint, event)
		}
		return BuildBinaryRequest(endpoint, event)
	default:
		return BuildStructuredRequest(endpoint, event)
	}
}

func ContentModeForSeq(seq uint64, mode string) string {
	switch mode {
	case "structured":
		return "structured"
	case "binary":
		return "binary"
	case "alternating":
		if seq%2 == 0 {
			return "structured"
		}
		return "binary"
	default:
		return "structured"
	}
}
