package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SSEEvent struct {
	ID    string
	Event string
	Data  string
}

type CEMessage struct {
	SpecVersion string                 `json:"specversion"`
	Type        string                 `json:"type"`
	Source      string                 `json:"source"`
	ID          string                 `json:"id"`
	Subject     string                 `json:"subject"`
	Time        string                 `json:"time"`
	Data        json.RawMessage        `json:"data"`
	Extensions  map[string]interface{} `json:"-"`

	KubeMQRequestID    string `json:"-"`
	KubeMQReplyChannel string `json:"-"`
}

// SSEConfig holds reconnection parameters for the SSE client.
type SSEConfig struct {
	ReconnectDelay time.Duration
	MaxDelay       time.Duration
	Multiplier     float64
	Logger         *slog.Logger
}

type SSEClient struct {
	httpClient     *http.Client
	lastEventID    string
	reconnectDelay time.Duration
	maxDelay       time.Duration
	multiplier     float64
	logger         *slog.Logger

	OnCloudevent func(ce CEMessage)
	OnMessage    func(data string)
	OnError      func(err error)
	OnKeepalive  func()
	OnReconnect  func()

	mu             sync.Mutex
	activeBody     io.ReadCloser
	reconnecting   atomic.Bool
	reconnectCount atomic.Int64
}

func NewSSEClient(httpClient *http.Client, cfg SSEConfig) *SSEClient {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &SSEClient{
		httpClient:     httpClient,
		reconnectDelay: cfg.ReconnectDelay,
		maxDelay:       cfg.MaxDelay,
		multiplier:     cfg.Multiplier,
		logger:         logger,
	}
}

// Connect establishes a single SSE connection and blocks until the stream ends.
func (c *SSEClient) Connect(ctx context.Context, sseURL string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", sseURL, nil)
	if err != nil {
		return fmt.Errorf("create SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if c.lastEventID != "" {
		req.Header.Set("Last-Event-ID", c.lastEventID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connect: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return fmt.Errorf("SSE connect failed: %d %s", resp.StatusCode, string(body))
	}

	c.mu.Lock()
	c.activeBody = resp.Body
	c.mu.Unlock()

	err = c.readLoop(ctx, resp.Body)

	c.mu.Lock()
	c.activeBody = nil
	c.mu.Unlock()

	return err
}

func (c *SSEClient) readLoop(ctx context.Context, body io.ReadCloser) error {
	defer func() { _ = body.Close() }()
	scanner := bufio.NewScanner(body)

	var event SSEEvent
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Text()

		if line == "" {
			if event.Data != "" {
				c.dispatch(event)
			}
			event = SSEEvent{}
			continue
		}

		if strings.HasPrefix(line, ":") {
			if c.OnKeepalive != nil {
				c.OnKeepalive()
			}
			continue
		}

		if strings.HasPrefix(line, "id:") {
			event.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		} else if strings.HasPrefix(line, "event:") {
			event.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			dataLine := strings.TrimPrefix(line, "data:")
			if len(dataLine) > 0 && dataLine[0] == ' ' {
				dataLine = dataLine[1:]
			}
			if event.Data != "" {
				event.Data += "\n" + dataLine
			} else {
				event.Data = dataLine
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("SSE read: %w", err)
	}
	return fmt.Errorf("SSE stream ended")
}

func (c *SSEClient) dispatch(event SSEEvent) {
	if event.ID != "" {
		c.lastEventID = event.ID
	}
	switch event.Event {
	case "cloudevent":
		if c.OnCloudevent != nil {
			ce := ParseCEMessage(event.Data)
			c.OnCloudevent(ce)
		}
	case "message":
		if c.OnMessage != nil {
			c.OnMessage(event.Data)
		}
	case "error":
		if c.OnError != nil {
			c.OnError(fmt.Errorf("SSE error event: %s", event.Data))
		}
	default:
		c.logger.Debug("unknown SSE event type", "event", event.Event)
	}
}

// ConnectWithReconnect maintains a persistent SSE connection with automatic
// exponential-backoff reconnection. Blocks until ctx is cancelled.
// On reconnect when lastEventID is set, the events_store_type query param
// is stripped from the URL so Last-Event-ID header takes precedence.
func (c *SSEClient) ConnectWithReconnect(ctx context.Context, sseURL string) {
	delay := c.reconnectDelay
	for {
		connectURL := sseURL
		if c.lastEventID != "" {
			connectURL = stripQueryParam(connectURL, "events_store_type")
		}

		err := c.Connect(ctx, connectURL)
		if ctx.Err() != nil {
			return
		}

		if c.OnError != nil {
			c.OnError(fmt.Errorf("SSE disconnected: %v", err))
		}
		if c.OnReconnect != nil {
			c.OnReconnect()
		}
		c.reconnecting.Store(true)
		c.reconnectCount.Store(100)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
			delay = time.Duration(float64(delay) * c.multiplier)
			if delay > c.maxDelay {
				delay = c.maxDelay
			}
		}
	}
}

// DisconnectForced closes the active SSE response body, causing the readLoop
// scanner to exit and triggering a reconnection via ConnectWithReconnect.
func (c *SSEClient) DisconnectForced() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeBody != nil {
		_ = c.activeBody.Close()
		c.activeBody = nil
	}
}

// IsReconnecting returns true within the first 100 messages after a reconnect.
// Consumers use this to suppress false duplicate detection during reconnection.
func (c *SSEClient) IsReconnecting() bool {
	return c.reconnecting.Load()
}

// DecrementReconnectCounter decrements the post-reconnect message counter.
// After 100 messages, the reconnecting flag is cleared.
func (c *SSEClient) DecrementReconnectCounter() {
	if c.reconnectCount.Add(-1) <= 0 {
		c.reconnecting.Store(false)
	}
}

// LastEventID returns the most recent SSE event ID for Last-Event-ID replay.
func (c *SSEClient) LastEventID() string {
	return c.lastEventID
}

func stripQueryParam(rawURL, param string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := u.Query()
	q.Del(param)
	u.RawQuery = q.Encode()
	return u.String()
}

// ParseCEMessage parses a JSON string from an SSE data field into a CEMessage.
// Standard CE attributes go into named fields. All other top-level JSON keys
// become Extensions. KubeMQ internal fields (_kubemq_request_id,
// _kubemq_reply_channel) are extracted separately for command/query correlation.
func ParseCEMessage(data string) CEMessage {
	var ce CEMessage
	_ = json.Unmarshal([]byte(data), &ce)

	var raw map[string]json.RawMessage
	_ = json.Unmarshal([]byte(data), &raw)

	standardKeys := map[string]bool{
		"specversion":     true,
		"type":            true,
		"source":          true,
		"id":              true,
		"subject":         true,
		"time":            true,
		"data":            true,
		"data_base64":     true,
		"datacontenttype": true,
		"dataschema":      true,
	}
	kubemqKeys := map[string]bool{
		"_kubemq_request_id":    true,
		"_kubemq_reply_channel": true,
	}

	ce.Extensions = make(map[string]interface{})
	for k, v := range raw {
		if standardKeys[k] || kubemqKeys[k] {
			continue
		}
		var val interface{}
		_ = json.Unmarshal(v, &val)
		ce.Extensions[k] = val
	}

	if v, ok := raw["_kubemq_request_id"]; ok {
		var s string
		_ = json.Unmarshal(v, &s)
		ce.KubeMQRequestID = s
	}
	if v, ok := raw["_kubemq_reply_channel"]; ok {
		var s string
		_ = json.Unmarshal(v, &s)
		ce.KubeMQReplyChannel = s
	}

	return ce
}
