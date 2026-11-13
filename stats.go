package main

import (
	"context"
	"net"
	rtm "runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// serverStats reads the truth from Redis directly, not through the road:
// how many connections the engine holds and how many GETs Redis served.
type serverStats struct {
	admin    *redis.Client // straight to Redis
	probe    *redis.Client // through the road, one connection: the measured RTT
	mu       sync.Mutex
	lastGets int64
	lastAt   time.Time
}

// target is the Redis the demo talks to.
type target struct {
	addr, user, pass string
}

func newServerStats(t target, proxyAddr string) *serverStats {
	// Addr is a label and the Dialer goes to the real address, with a
	// connection that reports the label as its remote address. go-redis's OTel
	// metrics take server.address from there and the pool name from Addr, so
	// otel.go can leave these two clients out of the engine's numbers.
	opt := func(addr, name string, size int) *redis.Options {
		var d net.Dialer
		return &redis.Options{Addr: name + ":0", Username: t.user, Password: t.pass, ClientName: name, PoolSize: size,
			DialTimeout: 3 * time.Second, DialerRetries: 1,
			Dialer: func(ctx context.Context, network, _ string) (net.Conn, error) {
				c, err := d.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return labelConn{c, labelAddr(name)}, nil
			}}
	}
	return &serverStats{admin: redis.NewClient(opt(t.addr, "demo-admin", 2)), probe: redis.NewClient(opt(proxyAddr, "demo-probe", 1))}
}

// labelConn is a connection whose RemoteAddr is a label.
type labelConn struct {
	net.Conn
	label labelAddr
}

func (c labelConn) RemoteAddr() net.Addr { return c.label }

type labelAddr string

func (a labelAddr) Network() string { return "tcp" }
func (a labelAddr) String() string  { return string(a) + ":0" }

// version is redis_version from INFO server, or "".
func (s *serverStats) version(ctx context.Context) string {
	info, err := s.admin.Info(ctx, "server").Result()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			return v
		}
	}
	return ""
}

func (s *serverStats) Close() {
	_ = s.admin.Close()
	_ = s.probe.Close()
}

// engineConns counts the connections named clientName in CLIENT LIST.
func (s *serverStats) engineConns(ctx context.Context) int {
	n, _ := s.connCounts(ctx)
	return n
}

// connCounts is the engine's named connections, and the idle connections with
// no name that never ran a command: warm spares (v9.24 PipelineMinIdleConns),
// dialed but not set up until first use, so they carry no name yet. Another
// unnamed idle client of this Redis would count there too.
func (s *serverStats) connCounts(ctx context.Context) (named, spares int) {
	list, err := s.admin.ClientList(ctx).Result()
	if err != nil {
		return -1, 0
	}
	for _, line := range strings.Split(list, "\n") {
		f := map[string]string{}
		for _, kv := range strings.Fields(line) {
			if k, v, ok := strings.Cut(kv, "="); ok {
				f[k] = v
			}
		}
		switch {
		case f["name"] == clientName:
			named++
		case line != "" && f["name"] == "" && f["cmd"] == "NULL":
			spares++
		}
	}
	return named, spares
}

// commandsPerSecond is the delta of every command Redis counted (INFO
// commandstats, all cmdstat_* calls) since the last call. Only the demo's
// engine runs load here, so this is the engine's traffic plus a few admin calls.
func (s *serverStats) commandsPerSecond(ctx context.Context) float64 {
	n, dt := s.commandsDelta(ctx)
	if dt <= 0 {
		return -1
	}
	return float64(n) / dt.Seconds()
}

// commandsDelta is the commands Redis counted since the last call, and the time between.
func (s *serverStats) commandsDelta(ctx context.Context) (int64, time.Duration) {
	info, err := s.admin.Info(ctx, "commandstats").Result()
	if err != nil {
		return 0, 0
	}
	var calls int64
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		name, rest, ok := strings.Cut(line, ":")
		if !ok || !strings.HasPrefix(name, "cmdstat_") || name == "cmdstat_info" || name == "cmdstat_client|list" {
			continue
		}
		for _, kv := range strings.Split(rest, ",") {
			if v, ok := strings.CutPrefix(kv, "calls="); ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				calls += n
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var n int64
	var dt time.Duration
	if !s.lastAt.IsZero() {
		n, dt = calls-s.lastGets, now.Sub(s.lastAt)
	}
	s.lastGets, s.lastAt = calls, now
	return n, dt
}

// measuredRTT is the median of a few PINGs through the road on one connection.
func (s *serverStats) measuredRTT(ctx context.Context) time.Duration {
	var ds []time.Duration
	for i := 0; i < 5; i++ {
		start := time.Now()
		if err := s.probe.Ping(ctx).Err(); err != nil {
			return -1
		}
		ds = append(ds, time.Since(start))
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[len(ds)/2]
}

// seed writes the keys the load reads, in pipelines of 1000.
func (s *serverStats) seed(ctx context.Context, keys, size int) error {
	val := strings.Repeat("v", size)
	for from := 0; from < keys; from += 1000 {
		pipe := s.admin.Pipeline()
		for i := from; i < min(from+1000, keys); i++ {
			pipe.Set(ctx, key(i), val, 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// cpuMeter reports how many cores this process used since the last call:
// the client and the road proxy together, since both run here.
type cpuMeter struct {
	last   time.Duration
	lastAt time.Time
}

// delta is the CPU time this process used since the last call, and the wall time between.
func (m *cpuMeter) delta() (time.Duration, time.Duration) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	used := time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	now := time.Now()
	var cpu, wall time.Duration
	if !m.lastAt.IsZero() {
		cpu, wall = used-m.last, now.Sub(m.lastAt)
	}
	m.last, m.lastAt = used, now
	return cpu, wall
}

// rtMeter reads the Go runtime's own counters with runtime/metrics, which does
// not stop the world (runtime.ReadMemStats does). Process-wide: the engine,
// the road proxy and the demo's stats and web code together.
type rtMeter struct {
	s    []rtm.Sample
	last rtDelta // the cumulative counters at the last read
	ok   bool    // last is set
}

// rtDelta is what the runtime counted over one interval.
type rtDelta struct {
	objs, bytes   uint64  // heap allocations, tiny ones included, and their bytes
	gcCPU, useCPU float64 // CPU seconds in GC, and in all Go code (total minus idle)
}

func (a rtDelta) add(b rtDelta) rtDelta {
	return rtDelta{a.objs + b.objs, a.bytes + b.bytes, a.gcCPU + b.gcCPU, a.useCPU + b.useCPU}
}

// gcPct is the share of the CPU the process's Go code used that went to GC.
// The runtime updates these CPU counters at GC cycles, so a window without a
// GC shows 0.
func (a rtDelta) gcPct() float64 {
	if a.useCPU <= 0 {
		return 0
	}
	return 100 * a.gcCPU / a.useCPU
}

var rtNames = []string{
	"/gc/heap/allocs:objects",
	"/gc/heap/tiny/allocs:objects",
	"/gc/heap/allocs:bytes",
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/total:cpu-seconds",
	"/cpu/classes/idle:cpu-seconds",
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/heap/unused:bytes",
	"/memory/classes/total:bytes",
}

// read returns what the runtime counted since the last call, the bytes in
// heap objects now (live, and dead not yet swept), the heap in use
// (MemStats.HeapInuse) and what the runtime holds from the OS (MemStats.Sys).
func (m *rtMeter) read() (d rtDelta, objBytes, inuse, sys uint64) {
	if m.s == nil {
		m.s = make([]rtm.Sample, len(rtNames))
		for i, n := range rtNames {
			m.s[i].Name = n
		}
	}
	rtm.Read(m.s)
	u := func(i int) uint64 {
		if m.s[i].Value.Kind() != rtm.KindUint64 {
			return 0
		}
		return m.s[i].Value.Uint64()
	}
	f := func(i int) float64 {
		if m.s[i].Value.Kind() != rtm.KindFloat64 {
			return 0
		}
		return m.s[i].Value.Float64()
	}
	now := rtDelta{objs: u(0) + u(1), bytes: u(2), gcCPU: f(3), useCPU: f(4) - f(5)}
	if m.ok {
		d = rtDelta{now.objs - m.last.objs, now.bytes - m.last.bytes, now.gcCPU - m.last.gcCPU, now.useCPU - m.last.useCPU}
	}
	m.last, m.ok = now, true
	return d, u(6), u(6) + u(7), u(8)
}

func (m *cpuMeter) cores() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	used := time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	now := time.Now()
	c := -1.0
	if !m.lastAt.IsZero() {
		c = float64(used-m.last) / float64(now.Sub(m.lastAt))
	}
	m.last, m.lastAt = used, now
	return c
}
