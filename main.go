// Command engines shows the three go-redis engines side by side on one Redis:
// the connection pool, batching (AutoPipeline) and full duplex
// (AutoPipelineOptions.FullDuplex), each on the blocking and the async face.
//
// A small proxy inside the demo is the road between the client and Redis: it
// adds a round trip you can change while the load runs. Connections and the
// GETs Redis served are read from the server itself.
//
//	docker compose up -d --wait
//	go run .              # interactive
//	go run . --scripted   # hands-free tour, prints a table and exits
//	go run . --web 127.0.0.1:8080   # the same demo in a browser
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	callerSteps = []int{1, 16, 64, 256, 1024, 4096}
	windowSteps = []int{1, 8, 64}
	rttSteps    = []time.Duration{0, 2 * time.Millisecond, 10 * time.Millisecond}
)

const tick = 500 * time.Millisecond

// metrics is what one 2-second window showed.
type metrics struct {
	rate, serverRate float64
	p50, p99         time.Duration
	conns            int
	cores            float64
	errs             int64
	disorder         int64 // ordering check: replies out of order since the engine started
	lastErr          string
	measuredRTT      time.Duration
	warming          bool
	span             time.Duration // scoreboard: how long this scenario has run
	heapMB, sysMB    float64       // Go heap in use, and what the runtime holds from the OS
	ceiling          float64       // the road's limit for this setup (config.ceiling)
	spares           int           // unnamed idle connections: warm spares (stats.go)
	// Go runtime, process-wide (rtMeter): per request completed, the GC share of
	// the CPU, the largest heap since the warm-up ended, and whether the request
	// rate is too low for the per-request numbers to mean much
	allocsPerReq, bytesPerReq, gcPct, peakHeapMB float64
	allocLowRate                                 bool
}

// srvTick is one tick of what Redis counted and what this process used.
type srvTick struct {
	cmds      int64
	cmdSpan   time.Duration
	cpu, wall time.Duration
}

// tickPoint is one column of the charts.
type tickPoint struct {
	rate     float64
	p50, p99 time.Duration
	engine   Engine
	warming  bool
	errRate  float64 // errors per second in this tick, for the chart's marks
}

// warmUp is how long after a change the numbers are left out: the pool dials
// its connections and every connection does its HELLO through the road.
const warmUp = 1500 * time.Millisecond

type scenario struct {
	engine          Engine
	rtt             time.Duration
	callers, window int
	load            string // config.loadKey: the board compares engines on the same load
	opts            string // config.optsLabel: engine options that differ from the defaults
}

type demo struct {
	mu      sync.Mutex
	proxy   *roadProxy
	stats   *serverStats
	hist    *liveHist
	cpu     cpuMeter
	rt      rtMeter // runtime/metrics, read with the histogram so both cover one interval
	peak    uint64  // the most heap object bytes in a tick since the last warm-up
	run     *running
	engine  Engine
	callers int
	window  int
	cfg     config
	direct  bool          // engines skip the road proxy: no added RTT, nothing in between
	target  target        // the Redis; changed by connect
	rtt     time.Duration // the road setting, applied by the proxy
	connErr error         // why there is no Redis to talk to, or nil
	version string        // redis_version of the target
	ring    []window
	srvRing []srvTick // Redis commands and process CPU, tick by tick, aligned with ring
	lastAt  time.Time
	started time.Time
	warmTo  time.Time // until then, ticks are not counted
	last    metrics
	tm      troubleMeter
	otel    *otelMeter // go-redis OpenTelemetry, read in process (otel.go)
	otelV   otelView
	find    finder  // the best-setup finder (finder.go)
	errSeen errSeen // what the events log has reported for the running engine
	trouble trouble
	board   map[scenario]metrics // derived from agg, for the UIs
	agg     map[scenario]*window // every post-warm-up window of a scenario, merged
	history []tickPoint          // one per tick, for the charts
	marks   []byte               // action letter per tick, 0 for none
	pending byte
	events  []string
}

// newDemo starts the road proxy and connects to Redis. A Redis that is not
// reachable is not an error here: d.connErr says why, and the browser UI can
// connect later. The terminal UI and the tours need a connection to start.
func newDemo(t target) (*demo, error) {
	p, err := newRoadProxy(t.addr)
	if err != nil {
		return nil, err
	}
	d := &demo{proxy: p, hist: newLiveHist(), engine: Pool, callers: 1, window: 1, // start small: + adds callers
		board: map[scenario]metrics{}, agg: map[scenario]*window{}, cfg: defaultConfig(), target: t}
	d.otel = newOTelMeter()
	if err := d.otel.set(d.cfg.OTel); err != nil {
		d.event("ERROR otel: %v", err)
	}
	d.stats = newServerStats(t, p.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := d.stats.admin.Ping(ctx).Err(); err != nil {
		d.connErr = fmt.Errorf("redis at %s: %w", t.addr, err)
		return d, nil
	}
	d.version = d.stats.version(ctx)
	err = d.stats.seed(ctx, d.cfg.Keys, d.cfg.ValueSize)
	if err == nil {
		err = d.seedWorkload(ctx, d.stats, d.cfg)
	}
	if err != nil {
		d.connErr = fmt.Errorf("seed: %w", err)
	}
	return d, nil
}

func (d *demo) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.run != nil {
		d.run.stop()
	}
	d.stats.Close()
	_ = d.proxy.Close()
}

func (d *demo) event(format string, args ...any) {
	line := time.Now().Format("15:04:05") + "  " + fmt.Sprintf(format, args...)
	d.events = append(d.events, line)
	if len(d.events) > 200 {
		d.events = d.events[len(d.events)-200:]
	}
}

// restart stops the running engine and starts the current one; mark is the
// letter shown in the strip for the action that caused it.
func (d *demo) restart(mark byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.run != nil {
		d.run.stop()
		d.run = nil
	}
	d.hist.snapshot(0) // drop what the old engine recorded
	d.ring, d.srvRing = nil, nil
	d.lastAt = time.Now()
	if d.connErr != nil {
		return d.connErr
	}
	engineAddr := d.proxy.Addr()
	if d.direct {
		engineAddr = d.target.addr
	}
	if err := d.otel.set(d.cfg.OTel); err != nil {
		d.event("ERROR otel: %v", err)
	}
	r, err := startEngine(engineAddr, d.target, d.engine, d.callers, d.window, d.cfg, d.hist)
	if err != nil {
		d.event("ERROR %v", err)
		return err
	}
	d.run = r
	d.started = time.Now()
	d.warmTo = d.started.Add(warmUp)
	d.pending = mark
	w := "-"
	if d.engine.Async() {
		w = fmt.Sprint(d.window)
	}
	d.event("%s, %d callers, in flight per caller %s, RTT +%s", d.engine, d.callers, w, d.rtt)
	return nil
}

func (d *demo) setRTT(rtt time.Duration) {
	d.proxy.SetRTT(rtt)
	d.mu.Lock()
	d.rtt = rtt
	d.ring, d.srvRing = nil, nil
	d.started = time.Now()
	d.warmTo = d.started.Add(warmUp)
	d.pending = 'R'
	d.event("road: RTT +%s", rtt)
	d.mu.Unlock()
}

// sample closes the current tick and returns the last 2 s.
// errSeen is what the events log has reported for one engine run.
type errSeen struct {
	run            *running
	errs, disorder int64
	at             time.Time
}

// logErrors adds new errors, and new out-of-order replies in the ordering
// check, to the events log: at most one line every 2 s. d.mu held.
func (d *demo) logErrors(run *running, out metrics) {
	e := &d.errSeen
	if run != e.run { // a new engine: its counters start at 0
		*e = errSeen{run: run}
	}
	if run == nil || time.Since(e.at) < 2*time.Second {
		return
	}
	if n := out.errs - e.errs; n > 0 {
		msg := out.lastErr
		if len(msg) > 120 {
			msg = msg[:120] + "..."
		}
		d.event("ERROR %d new error%s, last: %s", n, plural(int(n)), msg)
		e.errs, e.at = out.errs, time.Now()
	}
	if d.cfg.workloadInfo().ChecksOrder {
		if n := out.disorder - e.disorder; n > 0 {
			d.event("ERROR %d more replies out of order (%d since the engine started)", n, out.disorder)
			e.disorder, e.at = out.disorder, time.Now()
		}
	}
}

// scenarioLocked is the scoreboard key of what runs now. d.mu held.
func (d *demo) scenarioLocked() scenario {
	sc := scenario{d.engine, d.rtt, d.callers, d.window, d.cfg.loadKey(), d.cfg.optsLabel(d.engine)}
	if !sc.engine.Async() {
		sc.window = 1
	}
	return sc
}

func (d *demo) sample(ctx context.Context) metrics {
	d.mu.Lock()
	now := time.Now()
	w := d.hist.snapshot(now.Sub(d.lastAt))
	var objBytes, inuse, sys uint64
	w.allocs, objBytes, inuse, sys = d.rt.read()
	d.lastAt = now
	warming := now.Before(d.warmTo)
	if warming {
		d.peak = 0 // a new run: its peak starts after the warm-up
	} else {
		d.peak = max(d.peak, objBytes)
	}
	peak := d.peak
	if !warming {
		d.ring = append(d.ring, w)
		if len(d.ring) > 4 {
			d.ring = d.ring[len(d.ring)-4:]
		}
	}
	m := merge(d.ring)
	run := d.run
	sc := d.scenarioLocked()
	st, connErr := d.stats, d.connErr
	slow := d.cfg.slowAt(d.rtt)
	settled := !warming && len(d.ring) > 0 // every window the panels show counts, so the board's best is never worse than a shown number
	d.mu.Unlock()

	out := metrics{rate: m.rate(), p50: m.quantile(0.50), p99: m.quantile(0.99), warming: warming}
	if connErr != nil { // nothing to measure
		return out
	}
	aw := m      // the window the allocation numbers cover, like requests/s
	if warming { // show this tick alone in the chart, dimmed; the panels keep their last numbers
		aw = w
		out.rate, out.p50, out.p99 = w.rate(), w.quantile(0.50), w.quantile(0.99)
	}
	out.allocsPerReq, out.bytesPerReq, out.gcPct, out.allocLowRate = aw.allocsPerReq(), aw.bytesPerReq(), aw.allocs.gcPct(), aw.allocLowRate()
	out.peakHeapMB = float64(peak) / (1 << 20)
	cmds, cmdSpan := st.commandsDelta(ctx)
	out.conns, out.spares = st.connCounts(ctx)
	cpu, wall := d.cpu.delta()
	out.heapMB, out.sysMB = float64(inuse)/(1<<20), float64(sys)/(1<<20)
	out.measuredRTT = st.measuredRTT(ctx)
	if run != nil {
		out.errs = run.errs.Load()
		out.disorder = run.disorder.Load()
		if e, ok := run.lastErr.Load().(string); ok {
			out.lastErr = e
		}
	}

	d.mu.Lock()
	// Redis's count and the CPU over the same window as the client's numbers
	if !warming {
		d.srvRing = append(d.srvRing, srvTick{cmds, cmdSpan, cpu, wall})
		if len(d.srvRing) > len(d.ring) {
			d.srvRing = d.srvRing[len(d.srvRing)-len(d.ring):]
		}
	}
	ticks := d.srvRing
	if warming {
		ticks = []srvTick{{cmds, cmdSpan, cpu, wall}}
	}
	var sum srvTick
	for _, t := range ticks {
		sum.cmds, sum.cmdSpan, sum.cpu, sum.wall = sum.cmds+t.cmds, sum.cmdSpan+t.cmdSpan, sum.cpu+t.cpu, sum.wall+t.wall
	}
	out.serverRate, out.cores = -1, -1
	if sum.cmdSpan > 0 {
		out.serverRate = float64(sum.cmds) / sum.cmdSpan.Seconds()
	}
	if sum.wall > 0 {
		out.cores = float64(sum.cpu) / float64(sum.wall)
	}
	out.ceiling = d.cfg.ceiling(sc.engine, d.callers, d.window, d.rtt)
	if !warming || d.last.rate == 0 {
		d.last = out
	} else {
		d.last.warming = true
		d.last.ceiling = out.ceiling
	}
	if settled {
		// all the time this engine ran this load after warm-up: requests/s is
		// the total over the total time, p50 and p99 come from every request
		a := d.agg[sc]
		if a == nil {
			a = &window{}
			d.agg[sc] = a
		}
		*a = merge([]window{*a, w})
		all := out
		all.rate, all.p50, all.p99 = a.rate(), a.quantile(0.50), a.quantile(0.99)
		all.allocsPerReq, all.bytesPerReq, all.gcPct, all.allocLowRate = a.allocsPerReq(), a.bytesPerReq(), a.allocs.gcPct(), a.allocLowRate()
		all.span = a.span
		d.board[sc] = all
	}
	d.trouble = d.tm.sample(run, m, slow, d.cfg.windowTotal())
	d.logErrors(run, out)
	d.otelV = d.otel.sample(ctx)
	var errRate float64
	for _, v := range d.trouble.ErrRates {
		errRate += v
	}
	d.history = append(d.history, tickPoint{rate: out.rate, p50: out.p50, p99: out.p99, engine: sc.engine, warming: warming,
		errRate: errRate})
	d.marks = append(d.marks, d.pending)
	d.pending = 0
	if len(d.history) > 400 {
		d.history = d.history[len(d.history)-400:]
		d.marks = d.marks[len(d.marks)-400:]
	}
	d.mu.Unlock()
	return out
}

func main() {
	addr := flag.String("addr", "localhost:6379", "Redis address; the demo's own container is 127.0.0.1:16421 (docker-compose.yml)")
	user := flag.String("user", os.Getenv("REDIS_USERNAME"), "Redis username (default: $REDIS_USERNAME)")
	direct := flag.Bool("direct", false, "engines connect straight to Redis, without the road proxy (the RTT setting then has no effect)")
	scripted := flag.Bool("scripted", false, "run a hands-free tour, print a table and exit")
	measure := flag.Duration("measure", 3*time.Second, "scripted: how long each point is measured")
	sweep := flag.Bool("sweep", false, "p99 of batching vs full duplex at every caller count, +0 and +2 ms")
	web := flag.String("web", "", "serve the browser UI on this address, e.g. 127.0.0.1:8080")
	rttcheck := flag.Bool("rttcheck", false, "measure where the round trip goes: timer, Docker, the road, CPU under load")
	leakcheck := flag.Bool("leakcheck", false, "start the pool, stop it while it is still dialing, count leftover connections")
	tlsEnabled := flag.Bool("tls", false, "verify TLS for every Redis connection, directly or through the road")
	tlsCA := flag.String("tls-ca", "", "additional PEM CA file (requires --tls)")
	tlsName := flag.String("tls-server-name", "", "certificate DNS name override; default is the Redis target host (requires --tls)")
	flag.Parse()
	policy, policyErr := loadTLS(*tlsEnabled, *tlsCA, *tlsName)
	if policyErr != nil {
		fmt.Fprintln(os.Stderr, "engines:", policyErr)
		os.Exit(1)
	}

	// the password only from the environment: a flag would show in ps
	d, err := newDemo(target{addr: *addr, user: *user, pass: os.Getenv("REDIS_PASSWORD"), tls: policy})
	if err == nil {
		d.direct = *direct
		redis.SetLogger(&demoLogger{d: d})
		if d.connErr != nil && *web == "" {
			err = d.connErr // only the browser UI can connect later
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "engines:", err)
		os.Exit(1)
	}
	defer d.close()
	fmt.Printf("Redis transport: %s (RESP3 default; see engine options for override)\n", d.target.security())
	if *sweep {
		err = runSweep(d)
	} else if *web != "" {
		err = runWeb(d, *web)
	} else if *rttcheck {
		err = runRTTCheck(d)
	} else if *leakcheck {
		err = runLeakCheck(d)
	} else if *scripted {
		err = runScripted(d, *measure)
	} else {
		err = runTUI(d)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "engines:", err)
		os.Exit(1)
	}
}

// runScripted walks every engine on a few roads and prints what Redis saw.
func runScripted(d *demo, measure time.Duration) error {
	ctx := context.Background()
	type step struct {
		rtt     time.Duration
		callers int
	}
	steps := []step{{0, 1024}, {2 * time.Millisecond, 64}, {2 * time.Millisecond, 1024}, {10 * time.Millisecond, 1024}}
	fmt.Printf("go-redis %s (%s for pipes)\n", goRedisVersion(), pipesField)
	fmt.Printf("go-redis engines, one request in flight per caller (async: %d), 100%% GET, %d keys x %d B\n", d.window, d.cfg.Keys, d.cfg.ValueSize)
	fmt.Println("requests/s: higher is better. p99 and server connections: lower is better.")
	for _, st := range steps {
		d.rtt = st.rtt
		d.proxy.SetRTT(st.rtt)
		d.callers = st.callers
		fmt.Printf("\nroad: RTT +%s, %d callers\n", st.rtt, st.callers)
		fmt.Printf("  %-20s %12s %12s %10s %10s %7s %6s %10s\n", "engine", "requests/s", "Redis cmds/s", "p50", "p99", "conns", "cores", "road RTT")
		for e := Engine(0); e < numEngines; e++ {
			d.engine = e
			if err := d.restart(0); err != nil {
				return err
			}
			time.Sleep(1500 * time.Millisecond) // warm up: dial, fill the pool
			d.mu.Lock()
			d.hist.snapshot(0)
			d.mu.Unlock()
			d.stats.commandsPerSecond(ctx)
			d.cpu.cores()
			start := time.Now()
			time.Sleep(measure)
			d.mu.Lock()
			w := d.hist.snapshot(time.Since(start))
			d.mu.Unlock()
			server := d.stats.commandsPerSecond(ctx)
			cores := d.cpu.cores()
			conns := d.stats.engineConns(ctx)
			road := d.stats.measuredRTT(ctx) // measured under this engine's load
			errs := ""
			if n := d.run.errs.Load(); n > 0 {
				errs = fmt.Sprintf("  (%d errors)", n)
			}
			fmt.Printf("  %-20s %12s %12s %10s %10s %7d %6.1f %10s%s\n", e, fmtRate(w.rate()), fmtRate(server),
				fmtDur(w.quantile(0.5)), fmtDur(w.quantile(0.99)), conns, cores, fmtDur(road), errs)
		}
	}
	fmt.Println("\ncores = this process: the client and the road proxy together.")
	return nil
}

func fmtRate(v float64) string {
	switch {
	case v < 0:
		return "-"
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.0fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func fmtDur(d time.Duration) string {
	switch {
	case d < 0:
		return "-"
	case d < time.Millisecond:
		return fmt.Sprintf("%.0f µs", float64(d)/float64(time.Microsecond))
	case d < 10*time.Millisecond:
		return fmt.Sprintf("%.2f ms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
	}
}

// runLeakCheck stops the pool engine during warm-up and counts the engine
// connections Redis still holds afterwards. Every one should be gone.
func runLeakCheck(d *demo) error {
	ctx := context.Background()
	for _, rtt := range []time.Duration{0, 2 * time.Millisecond} {
		for _, after := range []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 3 * time.Second} {
			d.rtt = rtt
			d.proxy.SetRTT(rtt)
			d.engine, d.callers = Pool, 1024
			if err := d.restart(0); err != nil {
				return err
			}
			time.Sleep(after)
			before := d.stats.engineConns(ctx)
			d.mu.Lock()
			d.run.stop()
			d.run = nil
			d.mu.Unlock()
			left := -1
			for i := 0; i < 20; i++ { // up to 2 s for closes to reach Redis
				time.Sleep(100 * time.Millisecond)
				if left = d.stats.engineConns(ctx); left == 0 {
					break
				}
			}
			fmt.Printf("RTT +%-5s stop after %-6s: %3d connections before stop, %3d left after 2 s\n", rtt, after, before, left)
			if left > 0 {
				time.Sleep(3 * time.Second)
				fmt.Printf("    after 5 s: %d left\n", d.stats.engineConns(ctx))
			}
		}
	}
	return nil
}

// runRTTCheck splits the measured round trip into its parts.
func runRTTCheck(d *demo) error {
	ctx := context.Background()
	pct := func(ds []time.Duration, q float64) time.Duration {
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		return ds[int(q*float64(len(ds)-1))]
	}
	for _, want := range []time.Duration{time.Millisecond, 5 * time.Millisecond} {
		var ds []time.Duration
		for i := 0; i < 200; i++ {
			s := time.Now()
			time.Sleep(want)
			ds = append(ds, time.Since(s)-want)
		}
		fmt.Printf("time.Sleep(%s) overshoot: p50 %s, p99 %s\n", want, fmtDur(pct(ds, .5)), fmtDur(pct(ds, .99)))
	}
	probe := func(c interface {
		Ping(context.Context) *redis.StatusCmd
	}) (time.Duration, time.Duration) {
		var ds []time.Duration
		for i := 0; i < 200; i++ {
			s := time.Now()
			if err := c.Ping(ctx).Err(); err != nil {
				return -1, -1
			}
			ds = append(ds, time.Since(s))
		}
		return pct(ds, .5), pct(ds, .99)
	}
	p50, p99 := probe(d.stats.admin)
	fmt.Printf("PING straight to Redis (Docker port forward): p50 %s, p99 %s\n", fmtDur(p50), fmtDur(p99))
	for _, rtt := range rttSteps {
		d.rtt = rtt
		d.proxy.SetRTT(rtt)
		p50, p99 = probe(d.stats.probe)
		fmt.Printf("PING through the road, +%-5s idle:  p50 %s, p99 %s\n", rtt, fmtDur(p50), fmtDur(p99))
	}
	for _, rtt := range rttSteps[:2] {
		d.rtt = rtt
		d.proxy.SetRTT(rtt)
		d.engine, d.callers = FullDuplex, 1024
		if err := d.restart(0); err != nil {
			return err
		}
		time.Sleep(2 * time.Second)
		d.cpu.cores()
		p50, p99 = probe(d.stats.probe)
		fmt.Printf("PING through the road, +%-5s under full duplex x1024: p50 %s, p99 %s, process CPU %.1f cores\n",
			rtt, fmtDur(p50), fmtDur(p99), d.cpu.cores())
		d.mu.Lock()
		d.run.stop()
		d.run = nil
		d.mu.Unlock()
	}
	return nil
}

// runSweep prints p50/p99 for the batching and full-duplex engines at every
// caller count, the same grid as the AWS benchmark (run12p).
func runSweep(d *demo) error {
	engines := []Engine{Batching, FullDuplex, BatchingAsync, FullDuplexAsync}
	for _, rtt := range []time.Duration{0, 2 * time.Millisecond} {
		d.rtt = rtt
		d.proxy.SetRTT(rtt)
		fmt.Printf("\nroad +%s, p50 / p99 (one request in flight per caller)\n  %7s", rtt, "callers")
		for _, e := range engines {
			fmt.Printf(" %22s", e)
		}
		fmt.Println()
		for _, n := range callerSteps {
			d.callers = n
			fmt.Printf("  %7d", n)
			for _, e := range engines {
				d.engine = e
				if err := d.restart(0); err != nil {
					return err
				}
				time.Sleep(warmUp)
				d.mu.Lock()
				d.hist.snapshot(0)
				d.mu.Unlock()
				start := time.Now()
				time.Sleep(2 * time.Second)
				d.mu.Lock()
				w := d.hist.snapshot(time.Since(start))
				d.mu.Unlock()
				fmt.Printf(" %10s / %9s", fmtDur(w.quantile(0.5)), fmtDur(w.quantile(0.99)))
			}
			fmt.Println()
		}
	}
	return nil
}
