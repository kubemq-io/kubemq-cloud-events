package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/kubemq-io/kubemq-cloud-events/burnin/config"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/metrics"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/payload"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/tracker"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/transport"
)

const (
	PatternEvents      = "events"
	PatternEventsStore = "events_store"
	PatternQueues      = "queues"
	PatternCommands    = "commands"
	PatternQueries     = "queries"
)

func AllPatterns() []string {
	return []string{PatternEvents, PatternEventsStore, PatternQueues, PatternCommands, PatternQueries}
}

type Worker interface {
	Pattern() string
	ChannelName() string
	ChannelIndex() int

	Start(ctx context.Context) error
	StartProducers()
	Stop()
	StopProducers()
	StopConsumers()
	DisconnectConsumers()
	ConsumerReady() <-chan struct{}

	Tracker() *tracker.Tracker
	LatencyAccumulator() *metrics.LatencyAccumulator
	RPCLatencyAccumulator() *metrics.LatencyAccumulator
	PeakRate() *metrics.PeakRateTracker
	RateWindow() *metrics.SlidingRateWindow
	TSStore() *metrics.SendTimestampStore

	SentCount() uint64
	ReceivedCount() uint64
	ErrorCount() uint64
	CorruptedCount() uint64
	ReconnectionCount() uint64
	DowntimeSeconds() float64
	BytesSentCount() uint64
	BytesReceivedCount() uint64
	DuplicatedCount() uint64
	CEFidelityFailures() uint64

	RPCSuccess() uint64
	RPCTimeout() uint64
	RPCError() uint64

	AdvanceRateWindows()
	ResetAfterWarmup()
	ProducerStatSnapshots() []WorkerStatSnapshot
	ConsumerStatSnapshots() []WorkerStatSnapshot
}

type WorkerStat struct {
	ID   string
	Sent atomic.Uint64
	Recv atomic.Uint64
}

type WorkerStatSnapshot struct {
	ID   string
	Sent uint64
	Recv uint64
}

type BaseWorker struct {
	pattern      string
	channelName  string
	channelIndex int
	cfg          *config.Config
	patternCfg   *config.PatternConfig
	logger       *slog.Logger

	postClient  *transport.PostClient
	sseHTTP     *http.Client
	baseURL     string
	contentMode string

	trk         *tracker.Tracker
	latAccum    *metrics.LatencyAccumulator
	rpcLatAccum *metrics.LatencyAccumulator
	peakRate    *metrics.PeakRateTracker
	rateWindow  *metrics.SlidingRateWindow
	tsStore     *metrics.SendTimestampStore
	sizeDistrib *payload.SizeDistribution

	limiter *rate.Limiter

	producerCtx    context.Context
	producerCancel context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	producerWG     sync.WaitGroup
	consumerWG     sync.WaitGroup
	consumerReady  chan struct{}

	sent            atomic.Uint64
	received        atomic.Uint64
	errors          atomic.Uint64
	corrupted       atomic.Uint64
	reconnections   atomic.Uint64
	downtime        atomic.Uint64 // nanoseconds
	bytesSent       atomic.Uint64
	bytesReceived   atomic.Uint64
	duplicated      atomic.Uint64
	ceFidelityFails atomic.Uint64

	rpcSuccessCount atomic.Uint64
	rpcTimeoutCount atomic.Uint64
	rpcErrorCount   atomic.Uint64

	producerStats []*WorkerStat
	consumerStats []*WorkerStat

	downtimeStart atomic.Int64

	sseClients   []*transport.SSEClient
	sseClientsMu sync.Mutex
}

func NewBaseWorker(pattern, channelName string, channelIndex int, cfg *config.Config, logger *slog.Logger) *BaseWorker {
	patternCfg := cfg.GetPatternConfig(pattern)

	targetRate := float64(patternCfg.Rate)
	burst := int(targetRate)
	if burst < 1 {
		burst = 1
	}

	var sizeDistrib *payload.SizeDistribution
	if cfg.Message.SizeMode == "distribution" {
		sizeDistrib, _ = payload.ParseDistribution(cfg.Message.SizeDistribution)
	}

	return &BaseWorker{
		pattern:      pattern,
		channelName:  channelName,
		channelIndex: channelIndex,
		cfg:          cfg,
		patternCfg:   patternCfg,
		logger:       logger.With("pattern", pattern, "channel", channelName),

		postClient:  transport.NewPostClient(cfg.CE.BaseURL),
		sseHTTP:     transport.NewSSEHTTPClient(),
		baseURL:     cfg.CE.BaseURL,
		contentMode: cfg.CE.ContentMode,

		trk:         tracker.New(cfg.Message.ReorderWindow),
		latAccum:    metrics.NewLatencyAccumulator(),
		rpcLatAccum: metrics.NewLatencyAccumulator(),
		peakRate:    metrics.NewPeakRateTracker(),
		rateWindow:  metrics.NewSlidingRateWindow(),
		tsStore:     metrics.NewSendTimestampStore(),
		sizeDistrib: sizeDistrib,

		limiter:       rate.NewLimiter(rate.Limit(targetRate), burst),
		consumerReady: make(chan struct{}),
	}
}

// --- Accessors ---

func (b *BaseWorker) Pattern() string                                    { return b.pattern }
func (b *BaseWorker) ChannelName() string                                { return b.channelName }
func (b *BaseWorker) ChannelIndex() int                                  { return b.channelIndex }
func (b *BaseWorker) ConsumerReady() <-chan struct{}                     { return b.consumerReady }
func (b *BaseWorker) Tracker() *tracker.Tracker                          { return b.trk }
func (b *BaseWorker) LatencyAccumulator() *metrics.LatencyAccumulator    { return b.latAccum }
func (b *BaseWorker) RPCLatencyAccumulator() *metrics.LatencyAccumulator { return b.rpcLatAccum }
func (b *BaseWorker) PeakRate() *metrics.PeakRateTracker                 { return b.peakRate }
func (b *BaseWorker) RateWindow() *metrics.SlidingRateWindow             { return b.rateWindow }
func (b *BaseWorker) TSStore() *metrics.SendTimestampStore               { return b.tsStore }

func (b *BaseWorker) SentCount() uint64          { return b.sent.Load() }
func (b *BaseWorker) ReceivedCount() uint64      { return b.received.Load() }
func (b *BaseWorker) ErrorCount() uint64         { return b.errors.Load() }
func (b *BaseWorker) CorruptedCount() uint64     { return b.corrupted.Load() }
func (b *BaseWorker) ReconnectionCount() uint64  { return b.reconnections.Load() }
func (b *BaseWorker) BytesSentCount() uint64     { return b.bytesSent.Load() }
func (b *BaseWorker) BytesReceivedCount() uint64 { return b.bytesReceived.Load() }
func (b *BaseWorker) DuplicatedCount() uint64    { return b.duplicated.Load() }
func (b *BaseWorker) CEFidelityFailures() uint64 { return b.ceFidelityFails.Load() }

func (b *BaseWorker) RPCSuccess() uint64 { return b.rpcSuccessCount.Load() }
func (b *BaseWorker) RPCTimeout() uint64 { return b.rpcTimeoutCount.Load() }
func (b *BaseWorker) RPCError() uint64   { return b.rpcErrorCount.Load() }

func (b *BaseWorker) DowntimeSeconds() float64 {
	return float64(b.downtime.Load()) / float64(time.Second)
}

// --- Lifecycle ---

func (b *BaseWorker) StopProducers() {
	if b.producerCancel != nil {
		b.producerCancel()
	}
	b.producerWG.Wait()
}

func (b *BaseWorker) StopConsumers() {
	if b.consumerCancel != nil {
		b.consumerCancel()
	}
	b.consumerWG.Wait()
}

func (b *BaseWorker) Stop() {
	b.StopProducers()
	b.StopConsumers()
}

// --- Rate control ---

func (b *BaseWorker) WaitForRate(ctx context.Context) error {
	return b.limiter.Wait(ctx)
}

func (b *BaseWorker) BackpressureCheck() bool {
	s := b.sent.Load()
	r := b.received.Load()
	if s <= r {
		return false
	}
	return s-r > uint64(b.cfg.Queue.MaxDepth)
}

// --- Message size ---

func (b *BaseWorker) SelectMessageSize() int {
	if b.cfg.Message.SizeMode == "distribution" && b.sizeDistrib != nil {
		return b.sizeDistrib.SelectSize()
	}
	return b.cfg.Message.SizeBytes
}

// --- Downtime tracking ---

func (b *BaseWorker) StartDowntime() {
	b.downtimeStart.Store(time.Now().UnixNano())
}

func (b *BaseWorker) StopDowntime() {
	start := b.downtimeStart.Swap(0)
	if start > 0 {
		dur := time.Now().UnixNano() - start
		b.downtime.Add(uint64(dur))
		metrics.AddDowntime(b.pattern, float64(dur)/float64(time.Second))
	}
}

// --- Rate windows ---

func (b *BaseWorker) AdvanceRateWindows() {
	b.rateWindow.Advance()
	b.peakRate.Advance()
}

// --- Warmup reset ---

func (b *BaseWorker) ResetAfterWarmup() {
	b.trk.Reset()
	b.latAccum.Reset()
	b.rpcLatAccum.Reset()
	b.peakRate = metrics.NewPeakRateTracker()
	b.rateWindow.Reset()
	b.tsStore.Purge(0)

	b.sent.Store(0)
	if b.pattern != PatternQueues {
		b.received.Store(0)
	}
	b.errors.Store(0)
	b.corrupted.Store(0)
	b.reconnections.Store(0)
	b.downtime.Store(0)
	b.bytesSent.Store(0)
	b.bytesReceived.Store(0)
	b.duplicated.Store(0)
	b.ceFidelityFails.Store(0)
	b.rpcSuccessCount.Store(0)
	b.rpcTimeoutCount.Store(0)
	b.rpcErrorCount.Store(0)
}

// --- Stat snapshots ---

func (b *BaseWorker) ProducerStatSnapshots() []WorkerStatSnapshot {
	snaps := make([]WorkerStatSnapshot, len(b.producerStats))
	for i, s := range b.producerStats {
		snaps[i] = WorkerStatSnapshot{
			ID:   s.ID,
			Sent: s.Sent.Load(),
			Recv: s.Recv.Load(),
		}
	}
	return snaps
}

func (b *BaseWorker) ConsumerStatSnapshots() []WorkerStatSnapshot {
	snaps := make([]WorkerStatSnapshot, len(b.consumerStats))
	for i, s := range b.consumerStats {
		snaps[i] = WorkerStatSnapshot{
			ID:   s.ID,
			Sent: s.Sent.Load(),
			Recv: s.Recv.Load(),
		}
	}
	return snaps
}

// --- SSE client tracking (for forced disconnect) ---

func (b *BaseWorker) RegisterSSEClient(c *transport.SSEClient) {
	b.sseClientsMu.Lock()
	b.sseClients = append(b.sseClients, c)
	b.sseClientsMu.Unlock()
}

func (b *BaseWorker) DisconnectAllSSE() {
	b.sseClientsMu.Lock()
	for _, c := range b.sseClients {
		c.DisconnectForced()
	}
	b.sseClientsMu.Unlock()
}

// --- SSE config helper ---

func (b *BaseWorker) NewSSEConfig() transport.SSEConfig {
	return transport.SSEConfig{
		ReconnectDelay: b.cfg.ReconnectInterval,
		MaxDelay:       b.cfg.ReconnectMaxInterval,
		Multiplier:     b.cfg.Recovery.ReconnectMultiplier,
		Logger:         b.logger,
	}
}

// --- CE Fidelity Validation ---

type CEFidelityResult struct {
	Valid           bool
	FailedAttribute string
	Expected        string
	Actual          string
}

func validateCEFidelity(ce transport.CEMessage, pattern, producerID, expectedChannel string) CEFidelityResult {
	if ce.Time == "" {
		return CEFidelityResult{FailedAttribute: "time", Expected: "non-empty RFC3339", Actual: "empty"}
	}
	if _, err := time.Parse(time.RFC3339Nano, ce.Time); err != nil {
		return CEFidelityResult{FailedAttribute: "time", Expected: "valid RFC3339", Actual: ce.Time}
	}

	expectedType := ceTypeForPattern(pattern)
	checks := []struct{ attr, expected, actual string }{
		{"specversion", "1.0", ce.SpecVersion},
		{"type", expectedType, ce.Type},
		{"source", fmt.Sprintf("burnin-ce/%s", producerID), ce.Source},
		{"subject", expectedChannel, ce.Subject},
	}
	for _, c := range checks {
		if c.actual != c.expected {
			return CEFidelityResult{FailedAttribute: c.attr, Expected: c.expected, Actual: c.actual}
		}
	}
	return CEFidelityResult{Valid: true}
}

func ceTypeForPattern(pattern string) string {
	switch pattern {
	case PatternEvents:
		return "com.kubemq.burnin.event"
	case PatternEventsStore:
		return "com.kubemq.burnin.event_store"
	case PatternQueues:
		return "com.kubemq.burnin.queue"
	case PatternCommands:
		return "com.kubemq.burnin.command"
	case PatternQueries:
		return "com.kubemq.burnin.query"
	default:
		return ""
	}
}
