package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"

	"github.com/kubemq-io/kubemq-cloud-events/burnin/config"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/metrics"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/payload"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/transport"
)

type QueriesWorker struct {
	*BaseWorker
}

func NewQueriesWorker(cfg *config.Config, channelIndex int, logger *slog.Logger) *QueriesWorker {
	channelName := fmt.Sprintf("ce_burnin_queries_%04d", channelIndex)
	return &QueriesWorker{
		BaseWorker: NewBaseWorker(PatternQueries, channelName, channelIndex, cfg, logger),
	}
}

func (w *QueriesWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	numResponders := w.patternCfg.RespondersPerChannel
	for i := 0; i < numResponders; i++ {
		responderID := fmt.Sprintf("r-%s-%04d-%03d", w.pattern, w.channelIndex, i)
		stat := &WorkerStat{ID: responderID}
		w.consumerStats = append(w.consumerStats, stat)

		w.consumerWG.Add(1)
		go w.startResponder(responderID, stat)
	}

	close(w.consumerReady)
	return nil
}

func (w *QueriesWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(w.consumerCtx)

	numSenders := w.patternCfg.SendersPerChannel
	for i := 0; i < numSenders; i++ {
		senderID := fmt.Sprintf("s-%s-%04d-%03d", w.pattern, w.channelIndex, i)
		stat := &WorkerStat{ID: senderID}
		w.producerStats = append(w.producerStats, stat)

		w.producerWG.Add(1)
		go w.startSender(senderID, stat)
	}
}

func (w *QueriesWorker) DisconnectConsumers() {
	w.DisconnectAllSSE()
}

func (w *QueriesWorker) startSender(senderID string, stat *WorkerStat) {
	defer w.producerWG.Done()

	var seq uint64
	for {
		if err := w.WaitForRate(w.producerCtx); err != nil {
			return
		}
		if w.producerCtx.Err() != nil {
			return
		}
		seq++

		msgSize := w.SelectMessageSize()
		body, crcHex := payload.Encode(metrics.SDK(), w.pattern, senderID, seq, msgSize)

		event := cloudevents.New()
		event.SetType("com.kubemq.burnin.query")
		event.SetSource(fmt.Sprintf("burnin-ce/%s", senderID))
		event.SetSubject(w.channelName)
		event.SetID(uuid.NewString())
		event.SetTime(time.Now())
		event.SetExtension("sequence", seq)
		event.SetExtension("contenthash", crcHex)
		event.SetExtension("producerid", senderID)
		_ = event.SetData(cloudevents.ApplicationJSON, body)

		req, err := transport.BuildRequest(w.baseURL+"/ce/send/query", event, seq, w.contentMode)
		if err != nil {
			w.rpcErrorCount.Add(1)
			w.errors.Add(1)
			metrics.IncRPCResponse(w.pattern, "error")
			continue
		}

		rpcCtx, rpcCancel := context.WithTimeout(w.producerCtx,
			time.Duration(w.cfg.RPC.TimeoutMs+2000)*time.Millisecond)
		req = req.WithContext(rpcCtx)

		t0 := time.Now()
		resp, err := w.postClient.Do(req)
		rpcCancel()
		elapsed := time.Since(t0)

		if err != nil {
			w.rpcErrorCount.Add(1)
			w.errors.Add(1)
			metrics.IncRPCResponse(w.pattern, "error")
			continue
		}

		metrics.IncHTTPStatus(w.pattern, resp.StatusCode)

		if resp.StatusCode >= 400 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == 503 {
				metrics.IncError(w.pattern, "server_timeout")
			} else {
				metrics.IncError(w.pattern, "http_error")
			}
			w.rpcErrorCount.Add(1)
			w.errors.Add(1)
			metrics.IncRPCResponse(w.pattern, "error")
			continue
		}

		ceResp, err := transport.ParseResponse(resp)
		if err != nil {
			w.rpcErrorCount.Add(1)
			w.errors.Add(1)
			metrics.IncError(w.pattern, "decode_failure")
			metrics.IncRPCResponse(w.pattern, "error")
			continue
		}
		if ceResp.IsError {
			if strings.Contains(ceResp.Message, "timeout") {
				w.rpcTimeoutCount.Add(1)
				metrics.IncRPCResponse(w.pattern, "timeout")
			} else {
				w.rpcErrorCount.Add(1)
				metrics.IncRPCResponse(w.pattern, "error")
			}
			continue
		}

		// Response CRC validation: extract the inner data from the response
		// CE envelope and verify the echoed body matches the original payload.
		// ceResp.Data is the full CE event envelope; the payload is in its "data" field.
		var respEnvelope struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(ceResp.Data, &respEnvelope); err != nil || respEnvelope.Data == nil {
			w.corrupted.Add(1)
			metrics.IncCorrupted(w.pattern)
			continue
		}

		if !payload.VerifyCRC(respEnvelope.Data, crcHex) {
			w.corrupted.Add(1)
			metrics.IncCorrupted(w.pattern)
			continue
		}

		w.rpcSuccessCount.Add(1)
		metrics.IncRPCResponse(w.pattern, "success")
		metrics.ObserveRPCDuration(w.pattern, elapsed)
		w.rpcLatAccum.Record(elapsed)
		w.sent.Add(1)
		stat.Sent.Add(1)
		w.bytesSent.Add(uint64(len(body)))
		metrics.IncSent(w.pattern, senderID)
		metrics.RecordBytesSent(w.pattern, len(body))

		w.trk.Record(senderID, seq)
	}
}

func (w *QueriesWorker) startResponder(responderID string, stat *WorkerStat) {
	defer w.consumerWG.Done()

	sseURL := fmt.Sprintf("%s/ce/subscribe/queries?client_id=%s&channel=%s",
		w.baseURL, responderID, w.channelName)

	sseClient := transport.NewSSEClient(w.sseHTTP, w.NewSSEConfig())
	w.RegisterSSEClient(sseClient)

	sseClient.OnCloudevent = func(ce transport.CEMessage) {
		stat.Recv.Add(1)

		requestID := ce.KubeMQRequestID
		replyChannel := ce.KubeMQReplyChannel

		if requestID == "" {
			w.errors.Add(1)
			metrics.IncError(w.pattern, "missing_request_id")
			return
		}
		if replyChannel == "" {
			w.errors.Add(1)
			metrics.IncError(w.pattern, "missing_reply_channel")
			return
		}

		warmup, _ := ce.Extensions["warmup"].(string)
		isWarmup := warmup == "true"

		// Echo the query data in the response for sender-side CRC validation
		respEvent := cloudevents.New()
		respEvent.SetType("com.kubemq.burnin.query.response")
		respEvent.SetSource(fmt.Sprintf("burnin-ce/%s", responderID))
		respEvent.SetSubject(replyChannel)
		respEvent.SetID(uuid.NewString())
		respEvent.SetTime(time.Now())
		_ = respEvent.SetData(cloudevents.ApplicationJSON, ce.Data)

		respURL := fmt.Sprintf("%s/ce/send/response?request_id=%s", w.baseURL, requestID)
		req, err := transport.BuildRequest(respURL, respEvent, 0, w.contentMode)
		if err != nil {
			if !isWarmup {
				w.errors.Add(1)
				metrics.IncError(w.pattern, "response_send_failure")
			}
			return
		}

		resp, err := w.postClient.Do(req)
		if err != nil {
			if !isWarmup {
				w.errors.Add(1)
				metrics.IncError(w.pattern, "response_send_failure")
			}
			return
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	sseClient.OnReconnect = func() {
		w.reconnections.Add(1)
		metrics.IncReconnection(w.pattern)
	}

	sseClient.ConnectWithReconnect(w.consumerCtx, sseURL)
}
