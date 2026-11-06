package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const poolSize = 500 // the pool's default here; the UI can change it

// clientName is set on every engine connection, so CLIENT LIST can count them.
// The process id keeps two demos on one Redis from counting each other.
var clientName = fmt.Sprintf("demo-engines-%d", os.Getpid())

// Engine is one of the ways go-redis can carry commands.
type Engine int

const (
	Pool Engine = iota
	Batching
	BatchingAsync
	FullDuplex
	FullDuplexAsync
	numEngines
)

func (e Engine) String() string {
	return [...]string{"pool", "batching", "batching, async", "full duplex", "full duplex, async"}[e]
}

func (e Engine) Async() bool { return e == BatchingAsync || e == FullDuplexAsync }

func key(i int) string { return fmt.Sprintf("demo:key:%d", i) }

// commands is what the load generator calls: a client or an autopipeliner.
type commands interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	GetToBuffer(ctx context.Context, key string, buf []byte) *redis.ZeroCopyStringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd
	ExpireNX(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	BLPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd
}

// running is one engine under load.
type running struct {
	engine   Engine
	cfg      config
	rdb      *redis.Client
	ap       *redis.AutoPipeliner
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	hist     *liveHist
	value    []byte
	errs     atomic.Int64
	errKinds [numErrKinds]atomic.Int64 // by kind (trouble.go)
	cmds     commands                  // what the callers use: the client or the autopipeliner (faults.go)
	ctx      context.Context
	disorder atomic.Int64 // ordering check: replies that were not the last + 1
	lagNs    atomic.Int64 // open loop: how far behind its schedule the slowest caller is
	inFlight atomic.Int64 // requests started, not finished
	callers  int
	window   int
	lastErr  atomic.Value // string
}

func startEngine(addr string, auth target, e Engine, callers, window int, cfg config, hist *liveHist) (*running, error) {
	opt := &redis.Options{
		Addr:         addr,
		Username:     auth.user,
		Password:     auth.pass,
		Dialer:       demoDialer, // caps dials in flight, no TIME_WAIT (net.go)
		ClientName:   clientName,
		Protocol:     3,
		PoolSize:     cfg.PoolSize,
		PoolTimeout:  30 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxIdleConns: cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		// 0 = no limit; with a cap, callers past it block until a connection is released
		MaxActiveConns: cfg.MaxActive,
		PoolFIFO:       cfg.PoolFIFO,
		// zero means the library default (32 KiB; pipeline pool 128 KiB)
		ReadBufferSize:          cfg.BufferKiB << 10,
		WriteBufferSize:         cfg.BufferKiB << 10,
		PipelineReadBufferSize:  cfg.PipeBufKiB << 10,
		PipelineWriteBufferSize: cfg.PipeBufKiB << 10,
		// full duplex holds one pipeline-pool connection per pipe (NumShards);
		// the default pipeline pool is too small above about 10 pipes
		PipelinePoolSize: cfg.pipelinePoolSize(),
	}
	if e == FullDuplex || e == FullDuplexAsync {
		setPipeMinIdle(opt, cfg.PipeMinIdle) // v9.24+ only (pipeminidle_*.go)
	}
	// client options (config.go): zero keeps the values above
	if cfg.RESP2 {
		opt.Protocol = 2
	}
	if cfg.PoolTimeoutMs > 0 {
		opt.PoolTimeout = time.Duration(cfg.PoolTimeoutMs) * time.Millisecond
	}
	if cfg.MaxRetries != 0 {
		opt.MaxRetries = cfg.MaxRetries
	}
	if cfg.TimeoutMs > 0 {
		opt.ReadTimeout = time.Duration(cfg.TimeoutMs) * time.Millisecond
		opt.WriteTimeout = opt.ReadTimeout
	}
	opt.ConnMaxLifetime = time.Duration(cfg.ConnLifetimeMs) * time.Millisecond
	opt.DisableIdentity = cfg.DisableIdentity
	rdb := redis.NewClient(opt)
	r := &running{engine: e, cfg: cfg, rdb: rdb, hist: hist, value: make([]byte, cfg.ValueSize)}
	for i := range r.value {
		r.value[i] = 'v'
	}
	// zero fields take the library defaults
	delay := time.Duration(cfg.MaxFlushDelayUs) * time.Microsecond
	// batching: batch size and bytes, batches at once and the queue limit; the
	// flush delay is a full-duplex setting in this demo
	batching := &redis.AutoPipelineOptions{MaxBatchSize: cfg.MaxBatchSize, MaxBatchBytes: cfg.MaxBatchKiB << 10,
		MaxConcurrentBatches: cfg.MaxConcurrentBatches, Unordered: cfg.MaxConcurrentBatches > 1,
		MaxQueuedCommands: cfg.MaxQueued}
	// with nothing changed, use the no-options constructors: the blocking face's
	// preset differs from zero-value options (MaxBatchSize 300, not 200)
	if cfg.MaxBatchSize == 0 && cfg.MaxBatchKiB == 0 && cfg.MaxConcurrentBatches <= 1 && cfg.MaxQueued == 0 {
		batching = nil
	}
	fd := &redis.AutoPipelineOptions{FullDuplex: true, MaxFlushDelay: delay, FullDuplexWindow: cfg.FDWindow}
	setPipes(fd, cfg.NumShards) // NumShards or FullDuplexPipes, by go-redis version (pipes_*.go)
	var c commands = rdb
	var err error
	switch e {
	case Batching:
		// more connections: NumShards, on the blocking face only. Each caller
		// waits for its reply, so its order holds; the async face would need
		// Unordered for it
		blocking := batching
		if cfg.BatchShards > 1 {
			o := redis.AutoPipelineOptions{}
			if batching != nil {
				o = *batching
			}
			o.NumShards = cfg.BatchShards
			blocking = &o
		}
		if blocking == nil {
			r.ap, err = rdb.AutoPipeline()
		} else {
			r.ap, err = rdb.AutoPipelineWithOptions(blocking)
		}
	case BatchingAsync:
		if batching == nil {
			r.ap, err = rdb.AsyncAutoPipeline()
		} else {
			r.ap, err = rdb.AsyncAutoPipelineWithOptions(batching)
		}
	case FullDuplex:
		r.ap, err = rdb.AutoPipelineWithOptions(fd)
	case FullDuplexAsync:
		r.ap, err = rdb.AsyncAutoPipelineWithOptions(fd)
	}
	if err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("%s: %w", e, err)
	}
	if r.ap != nil {
		c = r.ap
	}
	if !e.Async() {
		window = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.callers, r.window, r.cmds, r.ctx = callers, window, c, ctx
	for i := 0; i < callers; i++ {
		r.wg.Add(1)
		go r.caller(ctx, c, i, window)
	}
	return r, nil
}

// caller keeps window requests in flight. On the async face it starts the
// first command of every request, then finishes each one; on the blocking
// faces window is 1 and every call waits for its reply.
func (r *running) caller(ctx context.Context, c commands, id, window int) {
	defer r.wg.Done()
	cfg, async := r.cfg, r.engine.Async()
	n := id * 7
	rnd := uint64(id)*0x9E3779B97F4A7C15 + 1
	starts := make([]time.Time, window)
	keys := make([]string, window)
	// open loop: each caller keeps a schedule of cfg.OpenRate / callers requests/s
	// and a request's latency starts at its scheduled time, not when it was sent
	var interval time.Duration
	next := time.Now()
	if cfg.OpenRate > 0 {
		interval = time.Duration(float64(time.Second) * float64(r.callers) / float64(cfg.OpenRate))
		next = next.Add(interval * time.Duration(id) / time.Duration(max(r.callers, 1))) // spread the callers
	}
	orderKey := fmt.Sprintf("demo:seq:%s:%d", clientName, id)
	var lastSeq int64
	first := make([][]redis.Cmder, window)
	var unawaited, previous []redis.Cmder // async cache fill: the SETs nobody waits for
	// zero copy: one buffer per request in flight, value size + 2 for the
	// fast path that reads the payload and its CRLF in one read
	var bufs [][]byte
	if cfg.zeroCopy() {
		bufs = make([][]byte, window)
		for i := range bufs {
			bufs[i] = make([]byte, cfg.ValueSize+2)
		}
	}
	get := func(i int, k string) redis.Cmder {
		if bufs != nil {
			return c.GetToBuffer(ctx, k, bufs[i])
		}
		return c.Get(ctx, k)
	}
	fail := func(err error) bool {
		if ctx.Err() != nil {
			return true
		}
		r.errs.Add(1)
		r.errKinds[errKind(err)].Add(1)
		r.lastErr.Store(err.Error())
		time.Sleep(10 * time.Millisecond)
		return false
	}
	for ctx.Err() == nil {
		// 1. start every request
		var scheduled time.Time
		if interval > 0 {
			scheduled = next
			next = next.Add(interval * time.Duration(window))
			if d := time.Until(scheduled); d > 0 {
				time.Sleep(d)
			} else if -d > interval {
				r.lagNs.Store(int64(-d)) // behind schedule: the latency shows it, not the request rate
			}
		}
		r.inFlight.Add(int64(window))
		for i := 0; i < window; i++ {
			k := key(n % cfg.Keys)
			n++
			starts[i], keys[i] = time.Now(), k
			if interval > 0 {
				starts[i] = scheduled
			}
			switch cfg.Workload {
			case WSet:
				first[i] = []redis.Cmder{c.Set(ctx, k, r.value, 0)}
			case WMix:
				rnd ^= rnd << 13
				rnd ^= rnd >> 7
				rnd ^= rnd << 17
				if int(rnd%100) < cfg.WritePct {
					first[i] = []redis.Cmder{c.Set(ctx, k, r.value, 0)}
				} else {
					first[i] = []redis.Cmder{get(i, k)}
				}
			case WGet:
				first[i] = []redis.Cmder{get(i, k)}
			case WOrder:
				first[i] = []redis.Cmder{c.IncrBy(ctx, orderKey, 1)}
			case WRateLimit:
				rk := "demo:rl:" + k
				first[i] = []redis.Cmder{c.IncrBy(ctx, rk, 1), c.ExpireNX(ctx, rk, time.Minute)}
			case WPage:
				// the page's keys; on the blocking faces each GET waits for its reply
				page := first[i][:0]
				for j := 0; j < cfg.FanOut; j++ {
					page = append(page, c.Get(ctx, key((n+j)%cfg.Keys)))
				}
				n += cfg.FanOut - 1
				first[i] = page
			default: // WCacheFill: a plain GET, its reply is the SET's value
				first[i] = []redis.Cmder{c.Get(ctx, k)}
			}
		}
		// the previous window's fire-and-forget SETs: checked only now, after this
		// window is on the road, so nobody waits for them
		for _, cmd := range previous {
			if e := cmd.Err(); e != nil && fail(e) {
				return
			}
		}
		previous = previous[:0]
		// 2. app work while the requests are on the road (async), or after the replies (blocking)
		if cfg.Workload == WPage && cfg.WorkUs > 0 {
			time.Sleep(cfg.work() * time.Duration(window)) // the work of every request in the window
		}
		// 3. finish each request
		for i := 0; i < window; i++ {
			var err error
			for _, cmd := range first[i] {
				if e := cmd.Err(); e != nil && e != redis.Nil && err == nil {
					err = e
				}
			}
			if err != nil {
				r.inFlight.Add(-1)
				if fail(err) {
					return
				}
				continue
			}
			if cfg.Workload == WOrder {
				// replies are read in submit order: in order, each is the last + 1
				v := first[i][0].(*redis.IntCmd).Val()
				if lastSeq != 0 && v != lastSeq+1 {
					r.disorder.Add(1)
				}
				lastSeq = v
			}
			if cfg.Workload == WCacheFill {
				// the page for the key just read, built from the GET's reply: this
				// SET cannot be sent before that reply is back, so no pipeline can
				// merge the two
				page := first[i][0].(*redis.StringCmd).Val()
				set := c.Set(ctx, "demo:page:"+keys[i], page, time.Minute)
				if async {
					unawaited = append(unawaited, set) // the request is done; the SET finishes on its own
				} else if e := set.Err(); e != nil { // the blocking face waits for the SET: two trips
					r.inFlight.Add(-1)
					if fail(e) {
						return
					}
					continue
				}
			}
			r.hist.record(time.Since(starts[i]))
			r.inFlight.Add(-1)
		}
		previous, unawaited = unawaited, previous[:0]
	}
}

func (r *running) stop() {
	r.cancel()
	r.wg.Wait()
	if r.ap != nil {
		_ = r.ap.Close()
	}
	_ = r.rdb.Close()
}

// liveHist counts request latencies in log buckets; snapshot swaps in a fresh one.
type liveHist struct{ cur atomic.Pointer[histCounts] }

const (
	histBuckets = 420
	histStep    = 1.05 // 5% per bucket, 1 µs to about 13 minutes
)

type histCounts struct{ b [histBuckets]atomic.Uint64 }

func newLiveHist() *liveHist {
	h := &liveHist{}
	h.cur.Store(&histCounts{})
	return h
}

func (h *liveHist) record(d time.Duration) {
	us := float64(d) / float64(time.Microsecond)
	i := 0
	if us > 1 {
		i = int(math.Log(us) / math.Log(histStep))
	}
	if i >= histBuckets {
		i = histBuckets - 1
	}
	h.cur.Load().b[i].Add(1)
}

// window is a plain copy of the counts for one interval.
type window struct {
	counts [histBuckets]uint64
	span   time.Duration
}

func (h *liveHist) snapshot(span time.Duration) window {
	old := h.cur.Swap(&histCounts{})
	var w window
	w.span = span
	for i := range old.b {
		w.counts[i] = old.b[i].Load()
	}
	return w
}

func merge(ws []window) window {
	var m window
	for _, w := range ws {
		m.span += w.span
		for i, c := range w.counts {
			m.counts[i] += c
		}
	}
	return m
}

func (w window) total() uint64 {
	var t uint64
	for _, c := range w.counts {
		t += c
	}
	return t
}

func (w window) rate() float64 {
	if w.span <= 0 {
		return 0
	}
	return float64(w.total()) / w.span.Seconds()
}

// quantile returns the upper edge of the bucket that holds quantile q.
func (w window) quantile(q float64) time.Duration {
	t := w.total()
	if t == 0 {
		return 0
	}
	want := uint64(math.Ceil(q * float64(t)))
	var seen uint64
	for i, c := range w.counts {
		seen += c
		if seen >= want {
			return time.Duration(math.Pow(histStep, float64(i+1)) * float64(time.Microsecond))
		}
	}
	return 0
}

// countAbove is how many requests took longer than d (by bucket lower edge).
func (w window) countAbove(d time.Duration) uint64 {
	us := float64(d) / float64(time.Microsecond)
	var n uint64
	for i, c := range w.counts {
		if math.Pow(histStep, float64(i)) >= us {
			n += c
		}
	}
	return n
}
