package main

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

// ctl serialises control actions: the TUI's keys and the web UI's buttons can
// arrive while the sampler runs, and two clicks must not interleave a restart.
var ctl sync.Mutex

func (d *demo) state() (Engine, int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.engine, d.callers, d.window
}

func (d *demo) setEngine(e Engine) {
	if e < 0 || e >= numEngines {
		return
	}
	ctl.Lock()
	defer ctl.Unlock()
	d.mu.Lock()
	d.engine = e
	d.mu.Unlock()
	_ = d.restart(byte('1' + e))
}

func (d *demo) setCallers(n int) {
	if n < 1 || n > 4096 {
		return
	}
	ctl.Lock()
	defer ctl.Unlock()
	d.mu.Lock()
	d.callers = n
	d.mu.Unlock()
	_ = d.restart('C')
}

func (d *demo) setWindow(n int) {
	if n < 1 || n > 256 {
		return
	}
	ctl.Lock()
	defer ctl.Unlock()
	d.mu.Lock()
	d.window = n
	async := d.engine.Async()
	d.mu.Unlock()
	if async {
		_ = d.restart('W')
	}
}

func (d *demo) setRoad(rtt time.Duration) {
	if rtt < 0 || rtt > 50*time.Millisecond {
		return
	}
	ctl.Lock()
	defer ctl.Unlock()
	d.setRTT(rtt)
}

func (d *demo) clearBoard() {
	ctl.Lock()
	defer ctl.Unlock()
	d.mu.Lock()
	d.board = map[scenario]metrics{}
	d.agg = map[scenario]*window{}
	d.event("scoreboard cleared")
	d.mu.Unlock()
}

// key applies one TUI key; the web UI sends the same keys.
func (d *demo) key(k byte) {
	_, callers, window := d.state()
	switch {
	case k >= '1' && k <= '5':
		d.setEngine(Engine(k - '1'))
	case k == 'r' || k == 'R':
		d.mu.Lock()
		cur := d.rtt
		d.mu.Unlock()
		next := rttSteps[0]
		for i, r := range rttSteps {
			if r == cur {
				next = rttSteps[(i+1)%len(rttSteps)]
			}
		}
		d.setRoad(next)
	case k == '+' || k == '=':
		d.setCallers(callerSteps[min(indexOf(callerSteps, callers)+1, len(callerSteps)-1)])
	case k == '-' || k == '_':
		d.setCallers(callerSteps[max(indexOf(callerSteps, callers)-1, 0)])
	case k == 'w' || k == 'W':
		d.setWindow(windowSteps[(indexOf(windowSteps, window)+1)%len(windowSteps)])
	case k == 'x' || k == 'X':
		d.clearBoard()
	}
}

// setConfig applies a new workload or engine options: clamped, re-seeded when
// the keys or the value size change, then the engine restarts.
func (d *demo) setConfig(c config) {
	c = c.clamp()
	ctl.Lock()
	defer ctl.Unlock()
	d.mu.Lock()
	old := d.cfg
	d.cfg = c
	d.mu.Unlock()
	reseed := c.Keys != old.Keys || c.ValueSize != old.ValueSize
	_, seeder := c.workload().(Seeder)
	if reseed || seeder {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		d.mu.Lock()
		if d.run != nil { // stop the load while seeding, or it reads half-written values
			d.run.stop()
			d.run = nil
		}
		if reseed {
			d.event("seeding %d keys x %d B", c.Keys, c.ValueSize)
		}
		d.mu.Unlock()
		if reseed {
			if err := d.stats.seed(ctx, c.Keys, c.ValueSize); err != nil {
				d.mu.Lock()
				d.event("ERROR seed: %v", err)
				d.mu.Unlock()
			}
		}
		if err := d.seedWorkload(ctx, d.stats, c); err != nil {
			d.mu.Lock()
			d.event("ERROR seed: %v", err)
			d.mu.Unlock()
		}
	}
	if c.GOGC != old.GOGC {
		gc := c.GOGC
		if gc == 0 {
			gc = 100 // the Go default
		}
		debug.SetGCPercent(gc)
	}
	_ = d.restart('O')
}

// connect points the demo at another Redis: it tests the new one first and
// keeps the old one if that fails.
func (d *demo) connect(host string, port int, user, pass string) error {
	if host == "" || port < 1 || port > 65535 {
		return fmt.Errorf("host and a port from 1 to 65535, please")
	}
	ctl.Lock()
	defer ctl.Unlock()
	d.mu.Lock()
	policy := d.target.tls
	d.mu.Unlock()
	t := target{addr: net.JoinHostPort(host, strconv.Itoa(port)), user: user, pass: pass, tls: policy}
	st := newServerStats(t, d.proxy.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	err := st.admin.Ping(pctx).Err()
	pcancel()
	if err != nil {
		st.Close()
		d.mu.Lock()
		d.event("connect to %s failed: %v", t.addr, err)
		d.mu.Unlock()
		return fmt.Errorf("%s: %w", t.addr, err)
	}
	version := st.version(ctx)
	d.mu.Lock()
	if d.run != nil {
		d.run.stop()
		d.run = nil
	}
	old := d.stats
	d.stats, d.target, d.version, d.connErr = st, t, version, nil
	d.proxy.SetUpstream(t.addr)
	d.board, d.agg = map[scenario]metrics{}, map[scenario]*window{} // other server, other numbers
	d.event("connected to %s (Redis %s); seeding %d keys x %d B", t.addr, version, d.cfg.Keys, d.cfg.ValueSize)
	cfg := d.cfg
	d.mu.Unlock()
	old.Close()
	err = st.seed(ctx, cfg.Keys, cfg.ValueSize)
	if err == nil {
		err = d.seedWorkload(ctx, st, cfg)
	}
	if err != nil {
		d.mu.Lock()
		d.connErr = fmt.Errorf("seed: %w", err)
		d.mu.Unlock()
		return d.connErr
	}
	return d.restart('1')
}

// seedWorkload runs the workload's Seed, if it has one (workloads.go), on the
// admin client, and logs how long it took. The load must be stopped.
func (d *demo) seedWorkload(ctx context.Context, st *serverStats, cfg config) error {
	s, ok := cfg.workload().(Seeder)
	if !ok {
		return nil
	}
	start := time.Now()
	if err := s.Seed(ctx, st.admin, cfg); err != nil {
		return fmt.Errorf("%s: %w", cfg.Workload, err)
	}
	d.mu.Lock()
	d.event("%s: seed checked in %s", cfg.Workload, time.Since(start).Round(time.Microsecond))
	d.mu.Unlock()
	return nil
}
