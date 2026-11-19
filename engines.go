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
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd
}

var (
	_ commands = (*redis.Client)(nil)
	_ commands = (*redis.AutoPipeliner)(nil)
)

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
	workload Workload     // what each request does (workloads.go)
	info     WorkloadInfo // its info under cfg
	lagNs    atomic.Int64 // open loop: how far behind its schedule the slowest caller is
	inFlight atomic.Int64 // requests started, not finished
	callers  int
	window   int
	lastErr  atomic.Value // string
}

func engineOptions(addr string, auth target, e Engine, cfg config) *redis.Options {
	opt := &redis.Options{
		Addr:         addr,
		Username:     auth.user,
		Password:     auth.pass,
		TLSConfig:    auth.tls,                      // also identifies TLS to go-redis maintenance endpoint selection
		Dialer:       auth.dialer(10 * time.Second), // TCP + verified TLS, including through the road
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
	return opt
}

func startEngine(addr string, auth target, e Engine, callers, window int, cfg config, hist *liveHist) (*running, error) {
	rdb := redis.NewClient(engineOptions(addr, auth, e, cfg))
	w := cfg.workload()
	r := &running{
		engine: e, cfg: cfg, rdb: rdb, hist: hist, value: make([]byte, cfg.ValueSize),
		workload: w, info: workloadInfoFor(w, cfg),
	}
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

// caller keeps window requests in flight. On the async face it starts every
// request of the window, then finishes each one; on the blocking faces window
// is 1 and every call waits for its reply. What a request does is the
// workload's (workloads.go).
func (r *running) caller(ctx context.Context, c commands, id, window int) {
	defer r.wg.Done()
	cfg := r.cfg
	work := r.info.Work
	n := id * 7
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
	env := &CallerEnv{Cmds: c, Config: cfg, Value: r.value, Async: r.engine.Async(), ID: id, Window: window, disorder: &r.disorder}
	// zero copy: one buffer per request in flight, value size + 2 for the
	// fast path that reads the payload and its CRLF in one read
	if cfg.zeroCopy() {
		env.bufs = make([][]byte, window)
		for i := range env.bufs {
			env.bufs[i] = make([]byte, cfg.ValueSize+2)
		}
	}
	wc := r.workload.NewCaller(env)
	first := make([][]redis.Cmder, window)
	var unawaited, previous []redis.Cmder // fire-and-forget commands (async cache fill: the SETs nobody waits for)
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
			first[i] = wc.Start(ctx, i, k)
		}
		// the previous window's fire-and-forget commands: checked only now, after
		// this window is on the road, so nobody waits for them
		for _, cmd := range previous {
			if e := cmd.Err(); e != nil && fail(e) {
				return
			}
		}
		previous = previous[:0]
		// 2. app work while the requests are on the road (async), or after the replies (blocking)
		if work > 0 {
			time.Sleep(work * time.Duration(window)) // the work of every request in the window
		}
		// 3. finish each request
		for i := 0; i < window; i++ {
			var err error
			for _, cmd := range first[i] {
				if e := cmd.Err(); e != nil && e != redis.Nil && err == nil {
					err = e
				}
			}
			var later []redis.Cmder
			if err == nil {
				later, err = wc.Finish(ctx, i, keys[i], first[i])
			}
			if err != nil {
				r.inFlight.Add(-1)
				if fail(err) {
					return
				}
				continue
			}
			unawaited = append(unawaited, later...)
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
	allocs rtDelta // what the Go runtime counted over the same interval (stats.go)
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
		m.allocs = m.allocs.add(w.allocs)
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

// lowAllocRate is the request rate under which the per-request allocation
// numbers are not reliable: the runtime counts the whole process, so the road
// proxy and the demo's own stats and web code weigh in.
const lowAllocRate = 500 // per second: 1000 in the 2 s window

// allocsPerReq is heap allocations per request completed, tiny ones included.
func (w window) allocsPerReq() float64 { return perReq(float64(w.allocs.objs), w.total()) }

// bytesPerReq is heap bytes allocated per request completed.
func (w window) bytesPerReq() float64 { return perReq(float64(w.allocs.bytes), w.total()) }

// allocLowRate says the per-request numbers are not reliable (lowAllocRate).
func (w window) allocLowRate() bool { return w.rate() < lowAllocRate }

func perReq(v float64, n uint64) float64 {
	if n == 0 {
		return 0
	}
	return v / float64(n)
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
