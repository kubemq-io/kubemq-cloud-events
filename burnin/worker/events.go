package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"

	"github.com/kubemq-io/kubemq-cloud-events/burnin/config"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/metrics"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/payload"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/transport"
)

type EventsWorker struct {
	*BaseWorker
}

func NewEventsWorker(cfg *config.Config, channelIndex int, logger *slog.Logger) *EventsWorker {
	channelName := fmt.Sprintf("ce_burnin_events_%04d", channelIndex)
	return &EventsWorker{
		BaseWorker: NewBaseWorker(PatternEvents, channelName, channelIndex, cfg, logger),
	}
}

func (w *EventsWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	numConsumers := w.patternCfg.ConsumersPerChannel
	for i := 0; i < numConsumers; i++ {
		consumerID := fmt.Sprintf("c-%s-%04d-%03d", w.pattern, w.channelIndex, i)
		stat := &WorkerStat{ID: consumerID}
		w.consumerStats = append(w.consumerStats, stat)

		group := ""
		if w.patternCfg.ConsumerGroup {
			group = w.channelName + "_group"
		}

		w.consumerWG.Add(1)
		go w.startConsumer(consumerID, group, stat)
	}

	close(w.consumerReady)
	return nil
}

func (w *EventsWorker) StartProducers() {
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

func (w *EventsWorker) DisconnectConsumers() {
	w.DisconnectAllSSE()
}

func (w *EventsWorker) startProducer(producerID string, stat *WorkerStat) {
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
		event.SetType("com.kubemq.burnin.event")
		event.SetSource(fmt.Sprintf("burnin-ce/%s", producerID))
		event.SetSubject(w.channelName)
		event.SetID(uuid.NewString())
		event.SetTime(time.Now())
		event.SetExtension("sequence", seq)
		event.SetExtension("contenthash", crcHex)
		event.SetExtension("producerid", producerID)
		_ = event.SetData(cloudevents.ApplicationJSON, body)

		req, err := transport.BuildRequest(w.baseURL+"/ce/send/event", event, seq, w.contentMode)
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

func (w *EventsWorker) startConsumer(consumerID, group string, stat *WorkerStat) {
	defer w.consumerWG.Done()

	sseURL := fmt.Sprintf("%s/ce/subscribe/events?client_id=%s&channel=%s",
		w.baseURL, consumerID, w.channelName)
	if group != "" {
		sseURL += "&group=" + group
	}

	sseClient := transport.NewSSEClient(w.sseHTTP, w.NewSSEConfig())
	w.RegisterSSEClient(sseClient)

	sseClient.OnCloudevent = func(ce transport.CEMessage) {
		seqStr, _ := ce.Extensions["sequence"].(string)
		crcHex, _ := ce.Extensions["contenthash"].(string)
		producerID, _ := ce.Extensions["producerid"].(string)

		if seqStr == "" || producerID == "" {
			metrics.IncError(w.pattern, "malformed_message")
			return
		}

		seq, _ := strconv.ParseUint(seqStr, 10, 64)

		if !payload.VerifyCRC(ce.Data, crcHex) {
			w.corrupted.Add(1)
			metrics.IncCorrupted(w.pattern)
			return
		}

		result := validateCEFidelity(ce, w.pattern, producerID, w.channelName)
		if !result.Valid {
			w.ceFidelityFails.Add(1)
			metrics.IncCEFidelityFailure(w.pattern, result.FailedAttribute)
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

	sseClient.OnReconnect = func() {
		w.reconnections.Add(1)
		metrics.IncReconnection(w.pattern)
		w.StartDowntime()
	}

	sseClient.OnKeepalive = func() {
		w.StopDowntime()
	}

	sseClient.ConnectWithReconnect(w.consumerCtx, sseURL)
}
