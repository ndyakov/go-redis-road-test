package main

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// Faults, injected while the load runs, to see how each engine takes them.

// the big-reply fault: up to 100 MB values (Redis allows 512 MB), up to 100 GETs at once
const (
	maxBigMB   = 100
	maxBigGets = 100
)

// fault injects one fault; mb and n are the value size and GET count of "big".
func (d *demo) fault(kind string, mb, n int) error {
	d.mu.Lock()
	r, st := d.run, d.stats
	d.mu.Unlock()
	if r == nil {
		return fmt.Errorf("no engine running")
	}
	ctx := context.Background()
	note := func(mark byte, format string, args ...any) {
		d.mu.Lock()
		d.pending = mark
		d.event(format, args...)
		d.mu.Unlock()
	}
	switch kind {
	case "kill":
		// every engine connection at once: watch errors, the dip, and how fast each engine is back
		list, err := st.admin.ClientList(ctx).Result()
		if err != nil {
			return err
		}
		n := 0
		for _, line := range strings.Split(list, "\n") {
			if !strings.Contains(line, " name="+clientName+" ") {
				continue
			}
			for _, f := range strings.Fields(line) {
				if id, ok := strings.CutPrefix(f, "id="); ok {
					if st.admin.Do(ctx, "CLIENT", "KILL", "ID", id).Err() == nil {
						n++
					}
				}
			}
		}
		note('K', "fault: killed %d engine connections", n)
	case "big":
		// n GETs of an mb MB value, at once, on the engine: on a shared connection
		// every reply behind them waits (head-of-line blocking); on the pool only
		// the callers that got those connections do
		mb, n = clampInt(mb, 1, maxBigMB), clampInt(n, 1, maxBigGets)
		key := fmt.Sprintf("demo:big:%d", mb)
		size := mb << 20
		if got, _ := st.admin.StrLen(ctx, key).Result(); got != int64(size) {
			if err := st.admin.Set(ctx, key, strings.Repeat("b", size), 0).Err(); err != nil {
				return err
			}
		}
		note('B', "fault: %d GET(s) of a %d MB value at once through the engine", n, mb)
		go func() {
			start := time.Now()
			times := make([]time.Duration, n)
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					t := time.Now()
					errs[i] = r.cmds.Get(r.ctx, key).Err()
					times[i] = time.Since(t)
				}()
			}
			wg.Wait()
			slices.Sort(times)
			failed := 0
			for _, e := range errs {
				if e != nil {
					failed++
				}
			}
			d.mu.Lock()
			if n == 1 {
				d.event("fault: the %d MB GET took %s (err: %v)", mb, fmtDur(times[0]), errs[0])
			} else {
				d.event("fault: %d x %d MB GETs: fastest %s, median %s, slowest %s, all done in %s, %d failed",
					n, mb, fmtDur(times[0]), fmtDur(times[n/2]), fmtDur(times[n-1]), fmtDur(time.Since(start)), failed)
			}
			d.mu.Unlock()
		}()
	case "block":
		// a blocking command: full duplex diverts it to a pooled connection, so the shared stream does not stall
		note('L', "fault: BLPOP with a 2 s timeout through the engine")
		go func() {
			start := time.Now()
			_ = r.cmds.BLPop(r.ctx, 2*time.Second, "demo:block:"+clientName).Err() // redis.Nil after 2 s
			d.mu.Lock()
			d.event("fault: BLPOP returned after %s", fmtDur(time.Since(start)))
			d.mu.Unlock()
		}()
	default:
		return fmt.Errorf("unknown fault %q", kind)
	}
	return nil
}

// setCores limits the client to n cores (GOMAXPROCS), 0 for all of them: the
// CPU-starved client of the talk, where MaxFlushDelay pays off. The road
// proxy runs in this process too, so it shares those cores.
func (d *demo) setCores(n int) {
	if n <= 0 || n > runtime.NumCPU() {
		n = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(n)
	d.mu.Lock()
	d.pending = 'P'
	d.event("client: %d of %d cores (GOMAXPROCS)", n, runtime.NumCPU())
	d.ring, d.srvRing = nil, nil // new numbers from here
	d.warmTo = time.Now().Add(warmUp)
	d.mu.Unlock()
}

// memory is the Go heap in use and what the Go runtime holds from the OS, in MB.
func memory() (heapMB, sysMB float64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse) / (1 << 20), float64(m.Sys) / (1 << 20)
}
