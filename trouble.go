package main

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// Error kinds, as the UIs show them.
const (
	errQueueFull = iota // MaxQueuedCommands reached
	errTimeout          // pool wait, read/write deadline, autopipeline timeout
	errConn             // dial, reset, closed connection
	errOther
	numErrKinds
)

var errKindNames = [numErrKinds]string{"queue full", "timeout", "connection", "other"}

func errKind(err error) int {
	var ne net.Error
	switch {
	case errors.Is(err, redis.ErrAutoPipelineQueueFull):
		return errQueueFull
	case errors.Is(err, redis.ErrPoolTimeout), errors.Is(err, redis.ErrAutoPipelineTimeout),
		errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return errTimeout
	case errors.As(err, &ne), errors.Is(err, redis.ErrClosed), errors.Is(err, redis.ErrPoolExhausted):
		return errConn
	}
	return errOther
}

// trouble is what went wrong or waited in the last tick: errors, requests that
// are blocked in the client, and requests slower than the threshold.
type trouble struct {
	ErrRates    [numErrKinds]float64 `json:"errRates"` // per second, by kind
	ErrTotal    int64                `json:"errTotal"`
	LastErr     string               `json:"lastErr"`
	Queued      int                  `json:"queued"`      // commands accepted by the autopipeliner, not yet sent
	PoolWaiting int                  `json:"poolWaiting"` // callers waiting for a pool connection now
	WaitsPerSec float64              `json:"waitsPerSec"` // pool waits per second
	AvgWaitMs   float64              `json:"avgWaitMs"`   // per pool wait
	PoolTOPerS  float64              `json:"poolTimeoutsPerSec"`
	InFlight    int64                `json:"inFlight"`    // requests started, not finished
	InFlightMax int                  `json:"inFlightMax"` // callers x in flight per caller
	WindowTotal int                  `json:"windowTotal"` // full duplex: window x pipes; 0 for other engines
	WindowFull  bool                 `json:"windowFull"`  // full duplex: the window is full, submitters block
	SlowMs      float64              `json:"slowMs"`      // the threshold used
	DelayedPct  float64              `json:"delayedPct"`  // of the requests in the last 2 s
	DelayedRate float64              `json:"delayedRate"` // per second
	Disorder    int64                `json:"disorder"`    // ordering check: replies out of order, since the engine started
	LagMs       float64              `json:"lagMs"`       // open loop: how far behind its schedule a caller was
}

// troubleMeter keeps the counters of the last tick, for the deltas.
type troubleMeter struct {
	run      *running
	errs     [numErrKinds]int64
	waits    uint32
	waitNs   int64
	timeouts uint32
	at       time.Time
}

func (t *troubleMeter) sample(r *running, w window, slow time.Duration, windowTotal int) trouble {
	out := trouble{SlowMs: ms(slow)}
	if r == nil {
		return out
	}
	now := time.Now()
	fresh := t.run != r
	dt := now.Sub(t.at).Seconds()
	var errs [numErrKinds]int64
	for k := range errs {
		errs[k] = r.errKinds[k].Load()
		out.ErrTotal += errs[k]
		if !fresh && dt > 0 {
			out.ErrRates[k] = float64(errs[k]-t.errs[k]) / dt
		}
	}
	if e, ok := r.lastErr.Load().(string); ok {
		out.LastErr = e
	}
	if r.ap != nil {
		out.Queued = r.ap.Len()
	}
	st := r.rdb.PoolStats()
	pool := st
	if (r.engine != Pool) && st.PipelineStats != nil {
		pool = (*redis.PoolStats)(st.PipelineStats)
	}
	out.PoolWaiting = int(pool.PendingRequests)
	if !fresh && dt > 0 {
		if n := pool.WaitCount - t.waits; n > 0 {
			out.WaitsPerSec = float64(n) / dt
			out.AvgWaitMs = float64(pool.WaitDurationNs-t.waitNs) / float64(n) / 1e6
		}
		out.PoolTOPerS = float64(pool.Timeouts-t.timeouts) / dt
	}
	out.Disorder = r.disorder.Load()
	out.LagMs = float64(r.lagNs.Swap(0)) / 1e6
	out.InFlight = r.inFlight.Load()
	out.InFlightMax = r.callers * r.window
	if r.engine == FullDuplex || r.engine == FullDuplexAsync {
		out.WindowTotal = windowTotal
		// a full window holds windowTotal on the wire; more waiting means submitters block
		out.WindowFull = out.InFlight >= int64(windowTotal)
	}
	if total := w.total(); total > 0 {
		over := w.countAbove(slow)
		out.DelayedPct = float64(over) / float64(total) * 100
		out.DelayedRate = float64(over) / w.span.Seconds()
	}
	t.run, t.errs, t.waits, t.waitNs, t.timeouts, t.at = r, errs, pool.WaitCount, pool.WaitDurationNs, pool.Timeouts, now
	return out
}
