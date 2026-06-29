package tracker

import (
	"sync"
	"sync/atomic"
)

// Tracker tracks message sequences per producer using a bitset-based approach.
// It detects gaps (loss), duplicates, and out-of-order delivery.
// Thread-safe for concurrent access from multiple consumer goroutines.
type Tracker struct {
	mu            sync.Mutex
	reorderWindow int
	producers     map[string]*producerState
}

type producerState struct {
	firstSeq         uint64
	highContiguous   uint64
	window           []uint64 // Bitset
	received         atomic.Uint64
	duplicates       atomic.Uint64
	outOfOrder       atomic.Uint64
	confirmedLost    atomic.Uint64
	lastReportedLost uint64 // For delta computation in DetectGaps
	lastSeen         uint64
	initialized      bool
}

// ProducerStats holds aggregate stats for a single producer.
type ProducerStats struct {
	Received      uint64
	Duplicates    uint64
	OutOfOrder    uint64
	ConfirmedLost uint64
}

func New(reorderWindow int) *Tracker {
	return &Tracker{
		reorderWindow: reorderWindow,
		producers:     make(map[string]*producerState),
	}
}

// Record records a received sequence for the given producer.
// Returns: isDuplicate, isOutOfOrder
func (t *Tracker) Record(producerID string, seq uint64) (isDuplicate bool, isOutOfOrder bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ps, ok := t.producers[producerID]
	if !ok {
		ps = &producerState{
			firstSeq:       seq,
			highContiguous: seq - 1,
			window:         make([]uint64, (t.reorderWindow+63)/64),
			initialized:    true,
			lastSeen:       seq,
		}
		t.producers[producerID] = ps
		setBit(ps.window, 0)
		ps.received.Add(1)
		t.advanceContiguous(ps)
		return false, false
	}

	ps.received.Add(1)

	if seq < ps.lastSeen {
		isOutOfOrder = true
		ps.outOfOrder.Add(1)
	}
	ps.lastSeen = seq

	if seq <= ps.highContiguous {
		ps.duplicates.Add(1)
		return true, isOutOfOrder
	}

	offset := seq - ps.highContiguous - 1
	if offset >= uint64(t.reorderWindow) {
		t.slideWindow(ps, seq)
		return false, isOutOfOrder
	}

	if getBit(ps.window, int(offset)) {
		ps.duplicates.Add(1)
		return true, isOutOfOrder
	}

	setBit(ps.window, int(offset))
	t.advanceContiguous(ps)
	return false, isOutOfOrder
}

// DetectGaps scans all producers and returns newly confirmed lost messages since the last call.
// Returns only the delta (new losses) to avoid double-counting when the caller adds to a counter.
func (t *Tracker) DetectGaps() map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	deltas := make(map[string]uint64)
	for id, ps := range t.producers {
		current := ps.confirmedLost.Load()
		delta := current - ps.lastReportedLost
		if delta > 0 {
			deltas[id] = delta
			ps.lastReportedLost = current
		}
	}
	return deltas
}

// Reset clears all tracking state (used after warmup).
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.producers = make(map[string]*producerState)
}

// Stats returns aggregate stats for all tracked producers.
func (t *Tracker) Stats() map[string]ProducerStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string]ProducerStats, len(t.producers))
	for pid, ps := range t.producers {
		result[pid] = ProducerStats{
			Received:      ps.received.Load(),
			Duplicates:    ps.duplicates.Load(),
			OutOfOrder:    ps.outOfOrder.Load(),
			ConfirmedLost: ps.confirmedLost.Load(),
		}
	}
	return result
}

// TotalReceived returns the total received count across all producers.
func (t *Tracker) TotalReceived() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.received.Load()
	}
	return total
}

// TotalDuplicates returns the total duplicate count across all producers.
func (t *Tracker) TotalDuplicates() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.duplicates.Load()
	}
	return total
}

// TotalLost returns the total confirmed lost count across all producers.
func (t *Tracker) TotalLost() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.confirmedLost.Load()
	}
	return total
}

// TotalOutOfOrder returns the total out-of-order count across all producers.
func (t *Tracker) TotalOutOfOrder() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.outOfOrder.Load()
	}
	return total
}

func (t *Tracker) advanceContiguous(ps *producerState) {
	for getBit(ps.window, 0) {

		ps.highContiguous++
		shiftLeft(ps.window)
	}
}

func (t *Tracker) slideWindow(ps *producerState, newSeq uint64) {
	newHigh := newSeq - uint64(t.reorderWindow) - 1
	if newHigh <= ps.highContiguous {
		offset := newSeq - ps.highContiguous - 1
		if int(offset) < t.reorderWindow {
			setBit(ps.window, int(offset))
		}
		return
	}

	slideDist := newHigh - ps.highContiguous
	if slideDist > uint64(t.reorderWindow) {
		slideDist = uint64(t.reorderWindow)
	}

	var gaps uint64
	for i := uint64(0); i < slideDist; i++ {
		if !getBit(ps.window, int(i)) {
			gaps++
		}
	}
	if gaps > 0 {
		ps.confirmedLost.Add(gaps)
	}

	for i := uint64(0); i < slideDist; i++ {
		shiftLeft(ps.window)
	}
	ps.highContiguous = newHigh

	offset := newSeq - ps.highContiguous - 1
	if int(offset) < t.reorderWindow {
		setBit(ps.window, int(offset))
	}

	t.advanceContiguous(ps)
}

func setBit(window []uint64, pos int) {
	if pos < 0 || pos >= len(window)*64 {
		return
	}
	window[pos/64] |= 1 << (uint(pos) % 64)
}

func getBit(window []uint64, pos int) bool {
	if pos < 0 || pos >= len(window)*64 {
		return false
	}
	return window[pos/64]&(1<<(uint(pos)%64)) != 0
}

func shiftLeft(window []uint64) {
	for i := 0; i < len(window); i++ {
		window[i] >>= 1
		if i+1 < len(window) {
			window[i] |= (window[i+1] & 1) << 63
		}
	}
}
