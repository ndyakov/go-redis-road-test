package main

import (
	"fmt"
	"strings"
	"time"
)

// config is everything you can change besides the engine, the road, the callers
// and the requests in flight. Engine options only apply to their engine.
type config struct {
	Workload  string `json:"workload"`  // a registered workload's ID (workloads.go)
	WritePct  int    `json:"writePct"`  // mix: 0-100
	ValueSize int    `json:"valueSize"` // bytes
	Keys      int    `json:"keys"`
	FanOut    int    `json:"fanOut"`   // page: keys per page
	WorkUs    int    `json:"workUs"`   // page: app work per page, µs
	SlowMs    int    `json:"slowMs"`   // a request slower than this counts as delayed; 0 = auto, 2 round trips + 1 ms
	OTel      bool   `json:"otel"`     // go-redis OpenTelemetry metrics (otel.go) on the engine
	OpenRate  int    `json:"openRate"` // open loop: target requests/s, scheduled; 0 = closed loop (each caller waits for its reply)
	ZeroCopy  bool   `json:"zeroCopy"` // workloads with ZeroCopy (get, mix): GetToBuffer into a buffer per request in flight, instead of Get

	// pool (redis.Options)
	PoolSize     int  `json:"poolSize"`
	MinIdleConns int  `json:"minIdleConns"`   // dialed ahead of the load
	PoolFIFO     bool `json:"poolFIFO"`       // reuse the oldest idle connection first
	BufferKiB    int  `json:"bufferKiB"`      // ReadBufferSize and WriteBufferSize; 0 = default (32 KiB)
	MaxActive    int  `json:"maxActiveConns"` // MaxActiveConns: hard cap on connections; 0 = no limit

	// batching and full duplex (AutoPipelineOptions, and the pipeline pool's buffers)
	MaxBatchSize         int  `json:"maxBatchSize"`         // batching; 0 = default (200, blocking face 300)
	MaxBatchKiB          int  `json:"maxBatchKiB"`          // batching: MaxBatchBytes; 0 = default (128 KiB)
	MaxConcurrentBatches int  `json:"maxConcurrentBatches"` // batching; > 1 sets Unordered: order is lost
	BatchShards          int  `json:"batchShards"`          // batching, blocking face only: NumShards, one connection each
	AdaptiveDelay        bool `json:"adaptiveDelay"`        // batching: delay by queue fill
	MaxQueued            int  `json:"maxQueued"`            // batching: MaxQueuedCommands; 0 = no limit
	MaxFlushDelayUs      int  `json:"maxFlushDelayUs"`      // both; 0 = default (off)
	PipeBufKiB           int  `json:"pipeBufKiB"`           // PipelineRead/WriteBufferSize; 0 = default (128 KiB)
	PipeMinIdle          int  `json:"pipeMinIdle"`          // full duplex, v9.24+: PipelineMinIdleConns; 0 = default (1 spare), -1 = none
	NumShards            int  `json:"numShards"`            // full duplex: pipes, one connection each
	FDWindow             int  `json:"fdWindow"`             // full duplex: FullDuplexWindow, commands in flight per pipe; 0 = default (65,536)

	// client options, every engine (redis.Options), and the Go runtime's GC
	RESP2           bool `json:"resp2"`           // Protocol 2 instead of 3
	PoolTimeoutMs   int  `json:"poolTimeoutMs"`   // PoolTimeout; 0 = the demo's 30 s
	MaxRetries      int  `json:"maxRetries"`      // MaxRetries; 0 = default (3), -1 = none
	TimeoutMs       int  `json:"timeoutMs"`       // ReadTimeout and WriteTimeout; 0 = the demo's 10 s
	ConnLifetimeMs  int  `json:"connLifetimeMs"`  // ConnMaxLifetime; 0 = no limit
	DisableIdentity bool `json:"disableIdentity"` // no CLIENT SETINFO on a new connection
	GOGC            int  `json:"gogc"`            // debug.SetGCPercent for this process; 0 = default (100)
}

const maxSeedBytes = 256 << 20 // keys x value size; the demo seeds every key

func defaultConfig() config {
	return config{Workload: defaultWorkload, WritePct: 20, ValueSize: 32, Keys: 1000, FanOut: 10, WorkUs: 1000,
		PoolSize: poolSize, NumShards: 1, BatchShards: 1, MaxConcurrentBatches: 1}
}

func clampInt(v, lo, hi int) int { return min(max(v, lo), hi) }

// clamp keeps every field in range; it never fails, so a bad request cannot
// stop the demo.
func (c config) clamp() config {
	if workloadByID(c.Workload) == nil {
		c.Workload = defaultWorkload
	}
	c.WritePct = clampInt(c.WritePct, 0, 100)
	c.ValueSize = clampInt(c.ValueSize, 1, 1<<20)
	c.Keys = clampInt(c.Keys, 1, 1_000_000)
	if c.Keys*c.ValueSize > maxSeedBytes {
		c.Keys = max(maxSeedBytes/c.ValueSize, 1)
	}
	c.FanOut = clampInt(c.FanOut, 1, 64)
	c.WorkUs = clampInt(c.WorkUs, 0, 100_000)
	c.SlowMs = clampInt(c.SlowMs, 0, 10_000)
	c.OpenRate = clampInt(c.OpenRate, 0, 10_000_000)
	c.PoolSize = clampInt(c.PoolSize, 1, 2000)
	c.MinIdleConns = clampInt(c.MinIdleConns, 0, c.PoolSize)
	c.BufferKiB = clampInt(c.BufferKiB, 0, 4096)
	c.MaxActive = clampInt(c.MaxActive, 0, 4000)
	c.MaxBatchSize = clampInt(c.MaxBatchSize, 0, 100_000)
	c.MaxBatchKiB = clampInt(c.MaxBatchKiB, 0, 64<<10)
	c.MaxConcurrentBatches = clampInt(c.MaxConcurrentBatches, 1, 64)
	c.BatchShards = clampInt(c.BatchShards, 1, 16)
	c.MaxQueued = clampInt(c.MaxQueued, 0, 10_000_000)
	c.PipeBufKiB = clampInt(c.PipeBufKiB, 0, 4096)
	c.PipeMinIdle = clampInt(c.PipeMinIdle, -1, 16)
	c.NumShards = clampInt(c.NumShards, 1, 16)
	c.MaxFlushDelayUs = clampInt(c.MaxFlushDelayUs, 0, 5000)
	c.FDWindow = clampInt(c.FDWindow, 0, 1<<20)
	c.PoolTimeoutMs = clampInt(c.PoolTimeoutMs, 0, 60_000)
	c.MaxRetries = clampInt(c.MaxRetries, -1, 20)
	c.TimeoutMs = clampInt(c.TimeoutMs, 0, 60_000)
	c.ConnLifetimeMs = clampInt(c.ConnLifetimeMs, 0, 3_600_000)
	c.GOGC = clampInt(c.GOGC, 0, 1000)
	return c
}

// workload is the selected workload; the default one for an unknown ID.
func (c config) workload() Workload {
	if w := workloadByID(c.Workload); w != nil {
		return w
	}
	return workloadByID(defaultWorkload)
}

// workloadInfo is the selected workload's info under this config.
func (c config) workloadInfo() WorkloadInfo { return workloadInfoFor(c.workload(), c) }

// zeroCopy reports whether the GETs of this workload use GetToBuffer.
func (c config) zeroCopy() bool { return c.ZeroCopy && c.workloadInfo().ZeroCopy }

// maxBatchBytesCode is MaxBatchBytes as Go: "16 << 20" or "64 << 10".
func (c config) maxBatchBytesCode() string {
	if c.MaxBatchKiB%1024 == 0 {
		return fmt.Sprintf("%d << 20", c.MaxBatchKiB>>10)
	}
	return fmt.Sprintf("%d << 10", c.MaxBatchKiB)
}

// loadKey names the load, without engine options: the scoreboard compares
// engines on the same load.
func (c config) loadKey() string {
	k := fmt.Sprintf("%s %dB %dkeys", c.Workload, c.ValueSize, c.Keys) + c.workloadInfo().LoadKey
	if c.OpenRate > 0 {
		k += fmt.Sprintf(" open%d", c.OpenRate)
	}
	return k
}

// optsLabel is the engine options that differ from the defaults, for the scoreboard.
func (c config) optsLabel(e Engine) string {
	var parts []string
	add := func(on bool, f string, v ...any) {
		if on {
			parts = append(parts, fmt.Sprintf(f, v...))
		}
	}
	switch e {
	case Pool:
		add(c.PoolSize != poolSize, "PoolSize %d", c.PoolSize)
		add(c.MinIdleConns > 0, "%d warm", c.MinIdleConns)
		add(c.PoolFIFO, "FIFO")
		add(c.BufferKiB > 0, "%d KiB buffers", c.BufferKiB)
		add(c.MaxActive > 0, "max %d active", c.MaxActive)
	case Batching, BatchingAsync:
		add(c.MaxBatchSize > 0, "batch %d", c.MaxBatchSize)
		add(c.MaxBatchKiB > 0, "batch bytes %s", c.maxBatchBytesCode())
		add(e == Batching && c.BatchShards > 1, "%d connections", c.BatchShards)
		add(c.MaxConcurrentBatches > 1, "%d batches at once, unordered", c.MaxConcurrentBatches)
		add(c.MaxQueued > 0, "queue %d", c.MaxQueued)
		add(c.PipeBufKiB > 0, "%d KiB buffers", c.PipeBufKiB)
	case FullDuplex, FullDuplexAsync:
		add(c.NumShards > 1, "%d pipes", c.NumShards)
		add(c.MaxFlushDelayUs > 0, "delay %d µs", c.MaxFlushDelayUs)
		add(c.FDWindow > 0, "window %d", c.FDWindow)
		add(hasPipeMinIdle && c.PipeMinIdle != 0, "spares %d", max(c.PipeMinIdle, 0))
		add(c.PipeBufKiB > 0, "%d KiB buffers", c.PipeBufKiB)
	}
	add(c.RESP2, "RESP2")
	add(c.PoolTimeoutMs > 0, "pool timeout %d ms", c.PoolTimeoutMs)
	add(c.MaxRetries == -1, "no retries")
	add(c.MaxRetries > 0, "%d retries", c.MaxRetries)
	add(c.TimeoutMs > 0, "timeouts %d ms", c.TimeoutMs)
	add(c.ConnLifetimeMs > 0, "conn lifetime %d ms", c.ConnLifetimeMs)
	add(c.DisableIdentity, "no SETINFO")
	add(c.GOGC > 0, "GOGC %d", c.GOGC)
	add(c.OTel, "OTel on")
	add(c.zeroCopy(), "GetToBuffer")
	return strings.Join(parts, ", ")
}

// code is the Go that builds this engine with these options: the line on screen.
// clientCode is the redis.Options fields every engine shares, when set.
func (c config) clientCode() []string {
	var o []string
	if c.RESP2 {
		o = append(o, "Protocol: 2")
	}
	if c.PoolTimeoutMs > 0 {
		o = append(o, fmt.Sprintf("PoolTimeout: %d * time.Millisecond", c.PoolTimeoutMs))
	}
	if c.MaxRetries != 0 {
		o = append(o, fmt.Sprintf("MaxRetries: %d", c.MaxRetries))
	}
	if c.TimeoutMs > 0 {
		o = append(o, fmt.Sprintf("ReadTimeout: %d * time.Millisecond, WriteTimeout: %d * time.Millisecond", c.TimeoutMs, c.TimeoutMs))
	}
	if c.ConnLifetimeMs > 0 {
		o = append(o, fmt.Sprintf("ConnMaxLifetime: %d * time.Millisecond", c.ConnLifetimeMs))
	}
	if c.DisableIdentity {
		o = append(o, "DisableIdentity: true")
	}
	return o
}

func (c config) code(e Engine) string {
	fd := e == FullDuplex || e == FullDuplexAsync
	client := c.clientCode()
	if e == Pool {
		client = append([]string{fmt.Sprintf("PoolSize: %d", c.PoolSize)}, client...)
		if c.MinIdleConns > 0 {
			client = append(client, fmt.Sprintf("MinIdleConns: %d", c.MinIdleConns))
		}
		if c.PoolFIFO {
			client = append(client, "PoolFIFO: true")
		}
		if c.BufferKiB > 0 {
			client = append(client, fmt.Sprintf("ReadBufferSize: %d << 10, WriteBufferSize: %d << 10", c.BufferKiB, c.BufferKiB))
		}
		if c.MaxActive > 0 {
			client = append(client, fmt.Sprintf("MaxActiveConns: %d", c.MaxActive))
		}
		return "rdb := redis.NewClient(&redis.Options{" + strings.Join(client, ", ") + "})"
	}
	if c.PipeBufKiB > 0 {
		client = append(client, fmt.Sprintf("PipelineReadBufferSize: %d << 10, PipelineWriteBufferSize: %d << 10", c.PipeBufKiB, c.PipeBufKiB))
	}
	if fd && hasPipeMinIdle && c.PipeMinIdle != 0 {
		client = append(client, fmt.Sprintf("PipelineMinIdleConns: %d", c.PipeMinIdle))
	}
	if (fd || e == Batching && c.BatchShards > 10) && c.pipelinePoolSize() > 10 {
		client = append(client, fmt.Sprintf("PipelinePoolSize: %d", c.pipelinePoolSize()))
	}
	var opts []string
	delay := func() {
		if c.MaxFlushDelayUs > 0 {
			opts = append(opts, fmt.Sprintf("MaxFlushDelay: %d * time.Microsecond", c.MaxFlushDelayUs))
		}
	}
	if fd {
		opts = append(opts, "FullDuplex: true")
		if c.NumShards > 1 {
			opts = append(opts, fmt.Sprintf("%s: %d", pipesField, c.NumShards))
		}
		delay()
		if c.FDWindow > 0 {
			opts = append(opts, fmt.Sprintf("FullDuplexWindow: %d", c.FDWindow))
		}
	} else {
		if c.MaxBatchSize > 0 {
			opts = append(opts, fmt.Sprintf("MaxBatchSize: %d", c.MaxBatchSize))
		}
		if c.MaxBatchKiB > 0 {
			opts = append(opts, "MaxBatchBytes: "+c.maxBatchBytesCode())
		}
		if e == Batching && c.BatchShards > 1 {
			opts = append(opts, fmt.Sprintf("NumShards: %d", c.BatchShards))
		}
		if c.MaxConcurrentBatches > 1 {
			opts = append(opts, fmt.Sprintf("MaxConcurrentBatches: %d, Unordered: true", c.MaxConcurrentBatches))
		}
		if c.MaxQueued > 0 {
			opts = append(opts, fmt.Sprintf("MaxQueuedCommands: %d", c.MaxQueued))
		}
	}
	name, method := "ap", "AutoPipeline"
	if e.Async() {
		name, method = "aap", "AsyncAutoPipeline"
	}
	line := fmt.Sprintf("%s, err := rdb.%s()", name, method)
	if len(opts) > 0 {
		line = fmt.Sprintf("%s, err := rdb.%sWithOptions(&redis.AutoPipelineOptions{%s})", name, method, strings.Join(opts, ", "))
	}
	if len(client) > 0 {
		line = "rdb := redis.NewClient(&redis.Options{" + strings.Join(client, ", ") + "})\n" + line
	}
	return line
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + ", " + b
}

func (c config) work() time.Duration { return time.Duration(c.WorkUs) * time.Microsecond }

// slowAt is the delayed threshold: the setting, or 2 round trips + 1 ms.
func (c config) slowAt(rtt time.Duration) time.Duration {
	if c.SlowMs > 0 {
		return time.Duration(c.SlowMs) * time.Millisecond
	}
	return 2*rtt + time.Millisecond
}

// windowTotal is how many commands full duplex keeps in flight before a
// submitter blocks: FullDuplexWindow per pipe, times the pipes.
func (c config) windowTotal() int {
	w := c.FDWindow
	if w == 0 {
		w = 65536
	}
	return w * max(c.NumShards, 1)
}

// pipelinePoolSize fits every pipe plus the warm spares (v9.24+ adds a spare
// only when the pipeline pool is larger than the pipes), and every batching
// connection: each shard flushes on its own pipeline-pool connection, and past
// the pool size a batch spills to the main pool. 10 is the default.
func (c config) pipelinePoolSize() int {
	return max(10, c.NumShards+max(c.PipeMinIdle, 1)+1, c.BatchShards)
}

// ceiling is the most requests/s this setup can do on the road, from round
// trips alone: requests in flight divided by the time each one needs. It ignores
// the CPU and the server, so a measured number at or below it is plausible and
// one above it is wrong. 0 when the road adds nothing (+0: CPU decides).
//
// Round trips per request: the workload's Trips on the blocking faces (two for
// the cache fill, GET then SET; FanOut for the page), one on the async face:
// it sends a request's commands at once and does not wait for the cache
// fill's SET. The workload's app work: after the replies on the blocking
// faces; on the async face it runs while the commands are on the road.
func (c config) ceiling(e Engine, callers, window int, rtt time.Duration) float64 {
	if rtt <= 0 {
		return 0
	}
	if !e.Async() {
		window = 1
	}
	inFlight := float64(callers * window)
	if e == Pool {
		inFlight = float64(min(callers, c.PoolSize))
		if c.MaxActive > 0 {
			inFlight = float64(min(callers, c.PoolSize, c.MaxActive))
		}
	}
	info := c.workloadInfo()
	trips := 1.0
	if !e.Async() {
		trips = float64(max(info.Trips, 1)) // one command after the other
	}
	perWindow := trips * rtt.Seconds()
	if info.Work > 0 {
		work := info.Work.Seconds() * float64(window)
		if e.Async() {
			perWindow = max(perWindow, work)
		} else {
			perWindow += work
		}
	}
	return inFlight / perWindow
}
