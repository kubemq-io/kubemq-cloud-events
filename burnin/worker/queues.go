package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"

	"github.com/kubemq-io/kubemq-cloud-events/burnin/config"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/metrics"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/payload"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/transport"
)

type QueueReceiveResult struct {
	MessagesReceived int               `json:"messages_received"`
	Messages         []json.RawMessage `json:"messages"`
}

type QueuesWorker struct {
	*BaseWorker
}

func NewQueuesWorker(cfg *config.Config, channelIndex int, logger *slog.Logger) *QueuesWorker {
	channelName := fmt.Sprintf("ce_burnin_queues_%04d", channelIndex)
	return &QueuesWorker{
		BaseWorker: NewBaseWorker(PatternQueues, channelName, channelIndex, cfg, logger),
	}
}

func (w *QueuesWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	numConsumers := w.patternCfg.ConsumersPerChannel
	for i := 0; i < numConsumers; i++ {
		consumerID := fmt.Sprintf("c-%s-%04d-%03d", w.pattern, w.channelIndex, i)
		stat := &WorkerStat{ID: consumerID}
		w.consumerStats = append(w.consumerStats, stat)

		w.consumerWG.Add(1)
		go w.startConsumer(consumerID, stat)
	}

	close(w.consumerReady)
	return nil
}

func (w *QueuesWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(w.consumerCtx)

	numProducers := w.patternCfg.ProducersPerChannel
	for i := 0; i < numProducers; i++ {
		producerID := fmt.Sprintf("p-%s-%04d-%03d", w.pattern, w.channelIndex, i)
		stat := &WorkerStat{ID: producerID}
		w.producerStats = append(w.producerStats, stat)

		w.producerWG.Add(1)
		go w.startProducer(producerID, stat)
	}
}

func (w *QueuesWorker) DisconnectConsumers() {
	// Queue consumers use stateless polling; no persistent connection to disconnect
}

func (w *QueuesWorker) startProducer(producerID string, stat *WorkerStat) {
	defer w.producerWG.Done()

	var seq uint64
	for {
		if err := w.WaitForRate(w.producerCtx); err != nil {
			return
		}
		if w.producerCtx.Err() != nil {
			return
		}
		if w.BackpressureCheck() {
			continue
		}
		seq++

		msgSize := w.SelectMessageSize()
		body, crcHex := payload.Encode(metrics.SDK(), w.pattern, producerID, seq, msgSize)

		event := cloudevents.New()
		event.SetType("com.kubemq.burnin.queue")
		event.SetSource(fmt.Sprintf("burnin-ce/%s", producerID))
		event.SetSubject(w.channelName)
		event.SetID(uuid.NewString())
		event.SetTime(time.Now())
		event.SetExtension("sequence", seq)
		event.SetExtension("contenthash", crcHex)
		event.SetExtension("producerid", producerID)
		_ = event.SetData(cloudevents.ApplicationJSON, body)

		req, err := transport.BuildRequest(w.baseURL+"/ce/queue/send", event, seq, w.contentMode)
		if err != nil {
			w.errors.Add(1)
			metrics.IncError(w.pattern, "send_failure")
			continue
		}

		w.tsStore.Store(producerID, seq, time.Now())
		metrics.IncContentMode(w.pattern, transport.ContentModeForSeq(seq, w.contentMode))

		t0 := time.Now()
		resp, err := w.postClient.Do(req)
		sendDuration := time.Since(t0)

		if err != nil {
			w.errors.Add(1)
			metrics.IncError(w.pattern, "send_failure")
			continue
		}

		metrics.ObserveSendDuration(w.pattern, sendDuration)
		metrics.IncHTTPStatus(w.pattern, resp.StatusCode)

		if resp.StatusCode >= 400 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == 503 {
				metrics.IncError(w.pattern, "server_timeout")
			} else {
				metrics.IncError(w.pattern, "http_error")
			}
			w.errors.Add(1)
			continue
		}

		ceResp, err := transport.ParseResponse(resp)
		if err != nil {
			w.errors.Add(1)
			metrics.IncError(w.pattern, "decode_failure")
			continue
		}
		if ceResp.IsError {
			w.errors.Add(1)
			metrics.IncError(w.pattern, "server_error")
			continue
		}

		w.sent.Add(1)
		stat.Sent.Add(1)
		w.bytesSent.Add(uint64(len(body)))
		metrics.IncSent(w.pattern, producerID)
		metrics.RecordBytesSent(w.pattern, len(body))
		w.rateWindow.Record()
		w.peakRate.Record()
	}
}

func (w *QueuesWorker) startConsumer(consumerID string, stat *WorkerStat) {
	defer w.consumerWG.Done()

	pollURL := fmt.Sprintf("%s/ce/queue/receive?channel=%s&client_id=%s&max_messages=%d&wait_timeout=%d",
		w.baseURL, w.channelName, consumerID,
		w.cfg.Queue.PollMaxMessages,
		w.cfg.Queue.PollWaitTimeoutSeconds)

	for {
		if w.consumerCtx.Err() != nil {
			return
		}

		req, err := http.NewRequestWithContext(w.consumerCtx, "POST", pollURL, nil)
		if err != nil {
			if w.consumerCtx.Err() != nil {
				return
			}
			metrics.IncError(w.pattern, "poll_failure")
			time.Sleep(1 * time.Second)
			continue
		}

		resp, err := w.postClient.Do(req)
		if err != nil {
			if w.consumerCtx.Err() != nil {
				return
			}
			metrics.IncError(w.pattern, "poll_failure")
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode >= 400 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			metrics.IncError(w.pattern, "poll_error")
			continue
		}

		ceResp, err := transport.ParseResponse(resp)
		if err != nil {
			metrics.IncError(w.pattern, "poll_error")
			continue
		}
		if ceResp.IsError {
			metrics.IncError(w.pattern, "poll_error")
			continue
		}

		var result QueueReceiveResult
		if err := json.Unmarshal(ceResp.Data, &result); err != nil {
			metrics.IncError(w.pattern, "decode_failure")
			continue
		}

		if result.MessagesReceived == 0 {
			metrics.IncQueuePollEmpty(w.pattern)
			continue
		}

		for _, rawMsg := range result.Messages {
			ce := transport.ParseCEMessage(string(rawMsg))

			seqStr, _ := ce.Extensions["sequence"].(string)
			crcHex, _ := ce.Extensions["contenthash"].(string)
			producerID, _ := ce.Extensions["producerid"].(string)

			if seqStr == "" || producerID == "" {
				metrics.IncError(w.pattern, "malformed_message")
				continue
			}

			seq, _ := strconv.ParseUint(seqStr, 10, 64)

			if !payload.VerifyCRC(ce.Data, crcHex) {
				w.corrupted.Add(1)
				metrics.IncCorrupted(w.pattern)
				continue
			}

			fidelity := validateCEFidelity(ce, w.pattern, producerID, w.channelName)
			if !fidelity.Valid {
				w.ceFidelityFails.Add(1)
				metrics.IncCEFidelityFailure(w.pattern, fidelity.FailedAttribute)
			}

			isDup, isOOO := w.trk.Record(producerID, seq)
			if isDup {
				w.duplicated.Add(1)
				metrics.IncDuplicated(w.pattern)
			}
			if isOOO {
				metrics.IncOutOfOrder(w.pattern)
			}

			if sendTime, ok := w.tsStore.LoadAndDelete(producerID, seq); ok {
				latency := time.Since(sendTime)
				w.latAccum.Record(latency)
				metrics.ObserveLatency(w.pattern, latency)
			}

			w.received.Add(1)
			stat.Recv.Add(1)
			w.bytesReceived.Add(uint64(len(ce.Data)))
			metrics.IncReceived(w.pattern, consumerID)
			metrics.RecordBytesReceived(w.pattern, len(ce.Data))
		}
	}
}
