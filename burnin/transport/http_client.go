package transport

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type PostClient struct {
	client  *http.Client
	baseURL string
}

func NewPostClient(baseURL string) *PostClient {
	transport := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &PostClient{
		client:  &http.Client{Transport: transport, Timeout: 30 * time.Second},
		baseURL: baseURL,
	}
}

func (pc *PostClient) Do(req *http.Request) (*http.Response, error) {
	return pc.client.Do(req)
}

func (pc *PostClient) BaseURL() string {
	return pc.baseURL
}

func NewSSEHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     0,
		DisableKeepAlives:   false,
	}
	return &http.Client{Transport: transport, Timeout: 0}
}

type CEResponse struct {
	IsError bool            `json:"is_error"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func ParseResponse(resp *http.Response) (*CEResponse, error) {
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	var ceResp CEResponse
	if err := json.NewDecoder(resp.Body).Decode(&ceResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &ceResp, nil
}
