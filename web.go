package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

//go:embed web
var webFiles embed.FS

type engineInfo struct {
	Name  string `json:"name"`
	Code  string `json:"code"`
	Color string `json:"color"`
	Async bool   `json:"async"`
}

type kpi struct {
	Rate       float64 `json:"rate"`
	ServerRate float64 `json:"serverRate"`
	P50ms      float64 `json:"p50ms"`
	P99ms      float64 `json:"p99ms"`
	Conns      int     `json:"conns"`
	Spares     int     `json:"spares"`
	Ceiling    float64 `json:"ceiling"`
	HeapMB     float64 `json:"heapMB"`
	SysMB      float64 `json:"sysMB"`
	Cores      float64 `json:"cores"`
	Warming    bool    `json:"warming"`
	Errs       int64   `json:"errs"`
	LastErr    string  `json:"lastErr"`

	// runtime/metrics, process-wide: per request completed in the window
	AllocsPerReq float64 `json:"allocsPerReq"`
	BytesPerReq  float64 `json:"bytesPerReq"`
	GCPct        float64 `json:"gcPct"`        // GC share of the CPU the Go code used
	PeakHeapMB   float64 `json:"peakHeapMB"`   // most heap object bytes since the warm-up ended
	AllocLowRate bool    `json:"allocLowRate"` // too few requests: the per-request numbers are not reliable
}

type point struct {
	Rate    float64 `json:"rate"`
	P50ms   float64 `json:"p50ms"`
	P99ms   float64 `json:"p99ms"`
	Engine  int     `json:"engine"`
	Warming bool    `json:"warming"`
	Mark    string  `json:"mark"`
	ErrRate float64 `json:"errRate"`
}

type boardRow struct {
	Engine    int     `json:"engine"`
	Name      string  `json:"name"`
	Rate      float64 `json:"rate"`
	P99ms     float64 `json:"p99ms"`
	Conns     int     `json:"conns"`
	RunS      float64 `json:"runS"` // how long this scenario has run after warm-up
	MostRate  bool    `json:"mostRate"`
	LowestP99 bool    `json:"lowestP99"`
	Failed    string  `json:"failed"` // why it cannot win: "errors", "out of order"; "" when clean

	// heap allocations per request over the whole run (process-wide)
	AllocsPerReq float64 `json:"allocsPerReq"`
	AllocLowRate bool    `json:"allocLowRate"`
}

// roadInfo is the client's cores.
type roadInfo struct {
	Cores    int `json:"cores"`    // GOMAXPROCS
	CoresMax int `json:"coresMax"` // runtime.NumCPU
}

// features are the options this build's go-redis has (pipes_*.go, pipeminidle_*.go).
type features struct {
	PipesField  string `json:"pipesField"`
	PipeMinIdle bool   `json:"pipeMinIdle"`
}

// connInfo is the connection, as the page shows it: never the password.
type connInfo struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	User      string `json:"user"`
	Connected bool   `json:"connected"`
	Error     string `json:"error"`
	Version   string `json:"version"`
	Direct    bool   `json:"direct"`
}

type snapshot struct {
	Engine        int          `json:"engine"`
	Engines       []engineInfo `json:"engines"`
	RTTms         float64      `json:"rttMs"`
	RTTSteps      []float64    `json:"rttSteps"`
	MeasuredRTTms float64      `json:"measuredRttMs"`
	Callers       int          `json:"callers"`
	CallerSteps   []int        `json:"callerSteps"`
	Window        int          `json:"window"`
	WindowSteps   []int        `json:"windowSteps"`
	TickMs        int          `json:"tickMs"`
	Conn          connInfo     `json:"conn"`
	GoRedis       string       `json:"goRedis"` // the go-redis version this binary was built with
	Trouble       trouble      `json:"trouble"`
	OTel          otelView     `json:"otel"`
	Finder        finderView   `json:"finder"`
	ErrKinds      []string     `json:"errKinds"`
	Features      features     `json:"features"`
	Road          roadInfo     `json:"road"`
	Config        config       `json:"config"`
	Workloads     any          `json:"workloads"`
	Now           kpi          `json:"now"`
	History       []point      `json:"history"`
	Board         []boardRow   `json:"board"`
	Events        []string     `json:"events"`
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func hex(c rgb) string { return fmt.Sprintf("#%02x%02x%02x", c.r, c.g, c.b) }

func (d *demo) snapshot() snapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := snapshot{
		Engine: int(d.engine), RTTms: ms(d.rtt), MeasuredRTTms: ms(d.last.measuredRTT),
		Callers: d.callers, CallerSteps: callerSteps, Window: d.window, WindowSteps: windowSteps,
		TickMs: int(tick / time.Millisecond), Config: d.cfg, Workloads: workloadInfos(d.cfg),
		Conn: d.connInfo(), GoRedis: goRedisVersion(), Trouble: d.trouble, OTel: d.otelV, Finder: d.find.view.snapshot(), ErrKinds: errKindNames[:],
		Features: features{PipesField: pipesField, PipeMinIdle: hasPipeMinIdle},
		Road:     roadInfo{Cores: runtime.GOMAXPROCS(0), CoresMax: runtime.NumCPU()},
		Now: kpi{Rate: d.last.rate, ServerRate: d.last.serverRate, P50ms: ms(d.last.p50), P99ms: ms(d.last.p99),
			Conns: d.last.conns, Spares: d.last.spares, Ceiling: d.last.ceiling, HeapMB: d.last.heapMB, SysMB: d.last.sysMB, Cores: d.last.cores,
			AllocsPerReq: d.last.allocsPerReq, BytesPerReq: d.last.bytesPerReq, GCPct: d.last.gcPct, PeakHeapMB: d.last.peakHeapMB, AllocLowRate: d.last.allocLowRate,
			Warming: d.last.warming, Errs: d.last.errs, LastErr: d.last.lastErr},
		// never null in JSON: the page reads .length on all of them
		History: []point{}, Board: []boardRow{}, Events: []string{},
	}
	for e := Engine(0); e < numEngines; e++ {
		s.Engines = append(s.Engines, engineInfo{Name: e.String(), Code: d.cfg.code(e), Color: hex(engineColors[e]), Async: e.Async()})
	}
	for _, r := range rttSteps {
		s.RTTSteps = append(s.RTTSteps, ms(r))
	}
	from := max(len(d.history)-300, 0)
	for i := from; i < len(d.history); i++ {
		h := d.history[i]
		p := point{Rate: h.rate, P50ms: ms(h.p50), P99ms: ms(h.p99), Engine: int(h.engine), Warming: h.warming, ErrRate: h.errRate}
		if d.marks[i] != 0 {
			p.Mark = string(d.marks[i])
		}
		s.History = append(s.History, p)
	}
	rtt := d.rtt
	best, fastest := 0.0, time.Duration(1<<62)
	load := d.cfg.loadKey()
	for sc, m := range d.board {
		if sc.rtt != rtt || sc.callers != d.callers || sc.load != load {
			continue
		}
		name := sc.engine.String()
		if sc.engine.Async() {
			name += fmt.Sprintf(" x%d", sc.window)
		}
		if sc.opts != "" {
			name += ", " + sc.opts
		}
		failed := ""
		switch {
		case m.errs > 0:
			failed = "errors"
		case d.cfg.workloadInfo().ChecksOrder && m.disorder > 0:
			failed = "out of order"
		}
		s.Board = append(s.Board, boardRow{Engine: int(sc.engine), Name: name, Rate: m.rate, P99ms: ms(m.p99), Conns: m.conns,
			RunS: m.span.Seconds(), AllocsPerReq: m.allocsPerReq, AllocLowRate: m.allocLowRate, Failed: failed})
		if failed != "" {
			continue // a failing scenario cannot be the best
		}
		best = max(best, m.rate)
		if m.p99 > 0 {
			fastest = min(fastest, m.p99)
		}
	}
	sort.Slice(s.Board, func(i, j int) bool {
		if s.Board[i].Engine != s.Board[j].Engine {
			return s.Board[i].Engine < s.Board[j].Engine
		}
		return s.Board[i].Name < s.Board[j].Name
	})
	if len(s.Board) > 1 {
		for i := range s.Board {
			if s.Board[i].Failed != "" {
				continue
			}
			s.Board[i].MostRate = s.Board[i].Rate == best
			s.Board[i].LowestP99 = s.Board[i].P99ms == ms(fastest)
		}
	}
	from = max(len(d.events)-8, 0)
	s.Events = append(s.Events, d.events[from:]...)
	return s
}

// runWeb serves the browser UI on addr until Ctrl-C.
func runWeb(d *demo, addr string) error {
	if err := d.restart('1'); err != nil && d.connErr == nil {
		return err
	} // not connected: the page asks for host and port
	var latest atomic.Pointer[[]byte]
	publish := func() {
		b, err := json.Marshal(d.snapshot())
		if err == nil {
			latest.Store(&b)
		}
	}
	publish()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d.sample(context.Background())
				publish()
			}
		}
	}()

	static, err := fs.Sub(webFiles, "web")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(*latest.Load())
	})
	mux.HandleFunc("GET /api/stream", func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			if _, err := fmt.Fprintf(w, "data: %s\n\n", *latest.Load()); err != nil {
				return
			}
			fl.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-t.C:
			}
		}
	})
	act := func(f func(r *http.Request) bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !f(r) {
				http.Error(w, "bad value", http.StatusBadRequest)
				return
			}
			publish()
			w.WriteHeader(http.StatusNoContent)
		}
	}
	num := func(r *http.Request, name string) (int, bool) {
		v, err := strconv.Atoi(r.PathValue(name))
		return v, err == nil
	}
	mux.HandleFunc("POST /api/engine/{n}", act(func(r *http.Request) bool {
		n, ok := num(r, "n")
		if ok && n >= 0 && n < int(numEngines) {
			d.setEngine(Engine(n))
		}
		return ok
	}))
	mux.HandleFunc("POST /api/rtt/{ms}", act(func(r *http.Request) bool {
		n, ok := num(r, "ms")
		if ok {
			d.setRoad(time.Duration(n) * time.Millisecond)
		}
		return ok
	}))
	mux.HandleFunc("POST /api/callers/{n}", act(func(r *http.Request) bool {
		n, ok := num(r, "n")
		if ok {
			d.setCallers(n)
		}
		return ok
	}))
	mux.HandleFunc("POST /api/window/{n}", act(func(r *http.Request) bool {
		n, ok := num(r, "n")
		if ok {
			d.setWindow(n)
		}
		return ok
	}))
	mux.HandleFunc("POST /api/clear", act(func(r *http.Request) bool { d.clearBoard(); return true }))
	reply := func(w http.ResponseWriter, err error) {
		publish()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	mux.HandleFunc("POST /api/fault/{kind}", func(w http.ResponseWriter, r *http.Request) {
		mb, _ := strconv.Atoi(r.URL.Query().Get("mb"))
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		reply(w, d.fault(r.PathValue("kind"), mb, n))
	})
	mux.HandleFunc("POST /api/finder/start", func(w http.ResponseWriter, r *http.Request) {
		reply(w, d.startFinder(r.URL.Query().Get("async") != "0"))
	})
	mux.HandleFunc("POST /api/finder/stop", func(w http.ResponseWriter, r *http.Request) { d.stopFinder(); reply(w, nil) })
	mux.HandleFunc("POST /api/cores/{n}", act(func(r *http.Request) bool {
		n, ok := num(r, "n")
		if ok {
			d.setCores(n)
		}
		return ok
	}))
	mux.HandleFunc("POST /api/connect", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Host     string `json:"host"`
			Port     int    `json:"port"`
			User     string `json:"user"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		err := d.connect(strings.TrimSpace(req.Host), req.Port, req.User, req.Password)
		publish()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /api/rttus/{us}", act(func(r *http.Request) bool {
		n, ok := num(r, "us")
		if ok {
			d.setRoad(time.Duration(n) * time.Microsecond)
		}
		return ok
	}))
	mux.HandleFunc("POST /api/config", act(func(r *http.Request) bool {
		d.mu.Lock()
		c := d.cfg // fields the request leaves out keep their value
		d.mu.Unlock()
		if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 4096)).Decode(&c); err != nil {
			return false
		}
		d.setConfig(c)
		return true
	}))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	fmt.Printf("go-redis engines: open http://%s  (Ctrl-C to stop)\n", ln.Addr())
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (d *demo) connInfo() connInfo {
	host, port, _ := net.SplitHostPort(d.target.addr)
	p, _ := strconv.Atoi(port)
	c := connInfo{Host: host, Port: p, User: d.target.user, Connected: d.connErr == nil, Version: d.version, Direct: d.direct}
	if d.connErr != nil {
		c.Error = d.connErr.Error()
	}
	return c
}
