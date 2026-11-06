package main

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"time"
)

// The finder runs every engine and its main mechanisms one after another, on
// the current road, workload, callers (or open-loop rate) and options, then
// names the best setup for latency, CPU, memory and throughput. It takes over
// the engine, in-flight, pipes and batching connections settings while it runs
// and puts them back.

// findMeasure is how long each setup is measured, after its warm-up.
const findMeasure = 4 * time.Second

type findCand struct {
	label   string
	engine  Engine
	window  int // in flight per caller (async faces)
	pipes   int // full duplex pipes; batching (blocking face) connections
	delayUs int // MaxFlushDelay in µs; 0 keeps the Options setting
}

// findCandidates lists the setups to run. With async, the async faces also
// keep several requests in flight per caller (x2 and up), which not every app
// can do; without it they run with one.
func findCandidates(async bool) []findCand {
	c := []findCand{
		{"pool", Pool, 1, 1, 0},
		{"batching", Batching, 1, 1, 0},
		{"batching, 2 connections", Batching, 1, 2, 0},
		{"batching, 4 connections", Batching, 1, 4, 0},
		{"full duplex", FullDuplex, 1, 1, 0},
		{"full duplex, 2 pipes", FullDuplex, 1, 2, 0},
		{"full duplex, 4 pipes", FullDuplex, 1, 4, 0},
		{"full duplex, delay 250 µs", FullDuplex, 1, 1, 250},
		// async with one request in flight: the commands inside a request still
		// go out together (the rate limiter's two, the page's GETs)
		{"batching, async x1", BatchingAsync, 1, 1, 0},
		{"full duplex, async x1", FullDuplexAsync, 1, 1, 0},
		{"full duplex, async x1, 2 pipes", FullDuplexAsync, 1, 2, 0},
		{"full duplex, async x1, delay 250 µs", FullDuplexAsync, 1, 1, 250},
		{"full duplex, async x1, delay 500 µs", FullDuplexAsync, 1, 1, 500},
	}
	if !async {
		return c
	}
	return append(c,
		findCand{"batching, async x2", BatchingAsync, 2, 1, 0},
		findCand{"batching, async x4", BatchingAsync, 4, 1, 0},
		findCand{"batching, async x8", BatchingAsync, 8, 1, 0},
		findCand{"full duplex, async x2", FullDuplexAsync, 2, 1, 0},
		findCand{"full duplex, async x4", FullDuplexAsync, 4, 1, 0},
		findCand{"full duplex, async x4, 2 pipes", FullDuplexAsync, 4, 2, 0},
		findCand{"full duplex, async x4, delay 250 µs", FullDuplexAsync, 4, 1, 250},
		findCand{"full duplex, async x8", FullDuplexAsync, 8, 1, 0},
		findCand{"full duplex, async x8, 2 pipes", FullDuplexAsync, 8, 2, 0},
	)
}

type findResult struct {
	Label    string  `json:"label"`
	Engine   int     `json:"engine"`
	Rate     float64 `json:"rate"`
	P50ms    float64 `json:"p50ms"`
	P99ms    float64 `json:"p99ms"`
	Cores    float64 `json:"cores"`
	CPUPerM  float64 `json:"cpuPerM"` // CPU seconds per million requests
	HeapMB   float64 `json:"heapMB"`
	Conns    int     `json:"conns"`
	Errs     int64   `json:"errs"`
	Disorder int64   `json:"disorder"` // ordering check: replies out of order
	KeptUp   bool    `json:"keptUp"`   // open loop: on schedule; closed loop: always true
	Ok       bool    `json:"ok"`       // no errors and kept up: it can be the best
}

type finderView struct {
	Running bool           `json:"running"`
	Step    int            `json:"step"`
	Total   int            `json:"total"`
	Current string         `json:"current"`
	Load    string         `json:"load"` // what it ran on
	Results []findResult   `json:"results"`
	Best    map[string]int `json:"best"` // "p99", "cpu", "mem", "rate": index into Results
	Note    string         `json:"note"`
}

// snapshot is the view for the page: lists are never null (the page reads .length).
func (v finderView) snapshot() finderView {
	if v.Results == nil {
		v.Results = []findResult{}
	}
	if v.Best == nil {
		v.Best = map[string]int{}
	}
	return v
}

type finder struct { // guarded by demo.mu
	view   finderView
	cancel context.CancelFunc
}

func (d *demo) startFinder(async bool) error {
	d.mu.Lock()
	if d.find.view.Running {
		d.mu.Unlock()
		return errors.New("the finder is already running")
	}
	if d.connErr != nil {
		d.mu.Unlock()
		return d.connErr
	}
	ctx, cancel := context.WithCancel(context.Background())
	cands := findCandidates(async)
	load := fmt.Sprintf("+%s, %d caller%s", d.rtt, d.callers, plural(d.callers))
	if d.cfg.OpenRate > 0 {
		load += fmt.Sprintf(", open loop %s/s", fmtRate(float64(d.cfg.OpenRate)))
	}
	load += ", " + d.cfg.loadKey()
	if !async {
		load += ", one request in flight per caller"
	}
	d.find = finder{view: finderView{Running: true, Total: len(cands), Load: load, Results: []findResult{}, Best: map[string]int{}}, cancel: cancel}
	d.event("finder: %d setups, %s each, on %s", len(cands), findMeasure, load)
	engine, window, cfg := d.engine, d.window, d.cfg
	d.mu.Unlock()
	go d.runFinder(ctx, cands, engine, window, cfg)
	return nil
}

func (d *demo) stopFinder() {
	d.mu.Lock()
	if d.find.cancel != nil {
		d.find.cancel()
	}
	d.mu.Unlock()
}

func (d *demo) runFinder(ctx context.Context, cands []findCand, engine Engine, window int, cfg config) {
	note := ""
	for i, c := range cands {
		if ctx.Err() != nil {
			note = "stopped"
			break
		}
		d.mu.Lock()
		d.find.view.Step, d.find.view.Current = i+1, c.label
		d.mu.Unlock()

		ctl.Lock()
		d.mu.Lock()
		d.engine, d.window = c.engine, c.window
		d.cfg.NumShards = cfg.NumShards
		if c.engine == FullDuplex || c.engine == FullDuplexAsync {
			d.cfg.NumShards = c.pipes
		}
		d.cfg.BatchShards = cfg.BatchShards
		if c.engine == Batching {
			d.cfg.BatchShards = c.pipes
		}
		d.cfg.MaxFlushDelayUs = cfg.MaxFlushDelayUs
		if c.delayUs > 0 {
			d.cfg.MaxFlushDelayUs = c.delayUs
		}
		d.mu.Unlock()
		err := d.restart('F')
		ctl.Unlock()
		if err != nil {
			note = err.Error()
			break
		}
		// every setup starts from the same heap: the last one's garbage (its
		// callers and in-flight commands, up to a few GB at x64) is collected
		// and returned to the OS now, in the warm-up, not during the measurement
		debug.FreeOSMemory()
		if !sleepCtx(ctx, warmUp+tick) {
			note = "stopped"
			break
		}
		r, ok := d.measureFind(ctx, c)
		if !ok {
			note = "stopped"
			break
		}
		d.mu.Lock()
		d.find.view.Results = append(d.find.view.Results, r)
		d.mu.Unlock()
	}

	// put the controls back
	ctl.Lock()
	d.mu.Lock()
	d.engine, d.window = engine, window
	d.cfg.NumShards = cfg.NumShards
	d.cfg.BatchShards = cfg.BatchShards
	d.cfg.MaxFlushDelayUs = cfg.MaxFlushDelayUs
	d.mu.Unlock()
	_ = d.restart('F')
	ctl.Unlock()

	d.mu.Lock()
	v := &d.find.view
	v.Running, v.Current, v.Best = false, "", bestOf(v.Results, cfg.OpenRate > 0)
	if note == "" && len(v.Best) == 0 && len(v.Results) > 0 {
		note = "no setup ran without errors, in order and on schedule"
	}
	v.Note = note
	d.find.cancel = nil
	if len(v.Best) > 0 {
		msg := fmt.Sprintf("finder: lowest p99 %s, least CPU %s, least memory %s",
			v.Results[v.Best["p99"]].Label, v.Results[v.Best["cpu"]].Label, v.Results[v.Best["mem"]].Label)
		if i, ok := v.Best["rate"]; ok {
			msg += ", most req/s " + v.Results[i].Label
		}
		d.event("%s", msg)
	} else {
		d.event("finder: done (%s)", note)
	}
	d.mu.Unlock()
}

// measureFind measures the running setup for findMeasure: latency and rate from
// the scoreboard's aggregate (cleared first), CPU and heap averaged over the ticks.
func (d *demo) measureFind(ctx context.Context, c findCand) (findResult, bool) {
	d.mu.Lock()
	sc := d.scenarioLocked()
	delete(d.agg, sc)
	delete(d.board, sc)
	openRate := d.cfg.OpenRate
	d.mu.Unlock()

	var cores, heap float64
	var n int
	var lagMax float64
	end := time.Now().Add(findMeasure)
	for time.Now().Before(end) {
		if !sleepCtx(ctx, tick) {
			return findResult{}, false
		}
		d.mu.Lock()
		if !d.last.warming && d.last.cores >= 0 {
			cores += d.last.cores
			heap += d.last.heapMB
			n++
		}
		lagMax = max(lagMax, d.trouble.LagMs)
		d.mu.Unlock()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	b := d.board[sc]
	r := findResult{Label: c.label, Engine: int(c.engine), Rate: b.rate, P50ms: ms(b.p50), P99ms: ms(b.p99),
		Conns: d.last.conns, Errs: d.last.errs, KeptUp: true}
	if d.cfg.Workload == WOrder {
		r.Disorder = d.trouble.Disorder // since this setup's engine started
	}
	if n > 0 {
		r.Cores, r.HeapMB = cores/float64(n), heap/float64(n)
	}
	if r.Rate > 0 {
		r.CPUPerM = r.Cores / r.Rate * 1e6
	}
	if openRate > 0 {
		r.KeptUp = lagMax < 100 && r.Rate >= 0.95*float64(openRate)
	}
	r.Ok = r.Errs == 0 && r.Disorder == 0 && r.KeptUp && r.Rate > 0
	return r, true
}

// bestOf names, among the setups that ran clean, the best for each measure.
// In open loop every setup that keeps up runs at the target rate, so there is
// no "most req/s".
func bestOf(rs []findResult, openLoop bool) map[string]int {
	best := map[string]int{}
	var ok []int
	for i, r := range rs {
		if r.Ok {
			ok = append(ok, i)
		}
	}
	if len(ok) == 0 {
		return best
	}
	pick := func(less func(a, b findResult) bool) int {
		s := append([]int(nil), ok...)
		sort.SliceStable(s, func(i, j int) bool { return less(rs[s[i]], rs[s[j]]) })
		return s[0]
	}
	// p99 is a histogram bucket edge, so ties are common: a tie goes to the
	// setup with more requests/s; CPU, memory and rate ties to the lower p99
	best["p99"] = pick(func(a, b findResult) bool {
		if a.P99ms != b.P99ms {
			return a.P99ms < b.P99ms
		}
		return a.Rate > b.Rate
	})
	best["cpu"] = pick(func(a, b findResult) bool {
		if a.CPUPerM != b.CPUPerM {
			return a.CPUPerM < b.CPUPerM
		}
		return a.P99ms < b.P99ms
	})
	best["mem"] = pick(func(a, b findResult) bool {
		if a.HeapMB != b.HeapMB {
			return a.HeapMB < b.HeapMB
		}
		return a.P99ms < b.P99ms
	})
	if openLoop {
		return best
	}
	best["rate"] = pick(func(a, b findResult) bool {
		if a.Rate != b.Rate {
			return a.Rate > b.Rate
		}
		return a.P99ms < b.P99ms
	})
	return best
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
