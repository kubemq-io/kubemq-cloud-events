package disconnect

import (
	"context"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-cloud-events/burnin/metrics"
	"github.com/kubemq-io/kubemq-cloud-events/burnin/worker"
)

type Manager struct {
	interval time.Duration
	duration time.Duration
	workers  []worker.Worker
	logger   *slog.Logger
}

func New(interval, duration time.Duration, workers []worker.Worker, logger *slog.Logger) *Manager {
	return &Manager{
		interval: interval,
		duration: duration,
		workers:  workers,
		logger:   logger,
	}
}

func (m *Manager) Run(ctx context.Context) {
	if m.interval == 0 {
		return
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.IncForcedDisconnect()
			m.logger.Info("forced disconnect: closing SSE connections")
			for _, w := range m.workers {
				w.DisconnectConsumers()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(m.duration):
			}
			m.logger.Info("forced disconnect: consumers will auto-reconnect")
		}
	}
}
