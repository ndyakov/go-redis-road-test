package main

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	redisotel "github.com/redis/go-redis/extra/redisotel-native/v9"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// go-redis's own OpenTelemetry metrics (extra/redisotel-native), read in
// process with a ManualReader every tick. What each engine records:
//   - pool and full duplex: db.client.operation.duration per command
//   - batching: one PIPELINE sample per batch, with db.operation.batch.size
//
// The demo's admin and probe clients have Addr set to a label (stats.go), so
// their series carry server.address=demo-admin or demo-probe and are dropped here.

// buckets are 10% apart, 10 µs to about 30 s: fine enough for a p99 at a 2 ms road.
func otelBuckets() []float64 {
	var b []float64
	for v := 10e-6; v < 30; v *= 1.1 {
		b = append(b, v)
	}
	return b
}

type otelMeter struct {
	reader *sdkmetric.ManualReader
	cfg    *redisotel.Config
	on     bool
	prevH  map[string]metricdata.HistogramDataPoint[float64] // cumulative, by metric and attributes
	prevS  map[string]int64
	last   time.Time
}

func newOTelMeter() *otelMeter {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	cfg := redisotel.NewConfig().WithEnabled(true).WithMeterProvider(mp).
		WithMetricGroups(redisotel.MetricGroupFlagCommand | redisotel.MetricGroupFlagConnectionBasic |
			redisotel.MetricGroupFlagConnectionAdvanced | redisotel.MetricGroupFlagResiliency).
		WithHistogramBuckets(otelBuckets())
	return &otelMeter{reader: reader, cfg: cfg, prevH: map[string]metricdata.HistogramDataPoint[float64]{}, prevS: map[string]int64{}}
}

// set turns the instrumentation on or off. Off removes the recorder, so the
// engines pay nothing. Call it before the engine starts.
func (o *otelMeter) set(on bool) error {
	if on == o.on {
		return nil
	}
	inst := redisotel.GetObservabilityInstance()
	if on {
		if err := inst.Init(o.cfg); err != nil {
			return err
		}
	} else if err := inst.Shutdown(); err != nil {
		return err
	}
	o.on = on
	return nil
}

// otelView is what the page shows, per tick.
type otelView struct {
	On        bool        `json:"on"`
	Ops       []otelOp    `json:"ops"`       // by db.operation.name, busiest first
	Pools     []otelPool  `json:"pools"`     // db.client.connection.count, by pool
	Pending   int64       `json:"pending"`   // db.client.connection.pending_requests
	WaitPerS  float64     `json:"waitPerS"`  // db.client.connection.wait_time: pool checkouts/s
	WaitP99Ms float64     `json:"waitP99Ms"` //
	DialsPerS float64     `json:"dialsPerS"` // db.client.connection.create_time
	DialP99Ms float64     `json:"dialP99Ms"` //
	Errors    []otelCount `json:"errors"`    // redis.client.errors, by error.type
	Closed    []otelCount `json:"closed"`    // redis.client.connection.closed, by reason
}

type otelOp struct {
	Name      string  `json:"name"`
	PerS      float64 `json:"perS"`
	MeanMs    float64 `json:"meanMs"`
	P50Ms     float64 `json:"p50Ms"`
	P99Ms     float64 `json:"p99Ms"`
	CmdsPerOp float64 `json:"cmdsPerOp,omitempty"` // PIPELINE: mean batch size
}

type otelPool struct {
	Name string `json:"name"` // "pool" or "pipeline pool", and the client number
	Idle int64  `json:"idle"`
	Used int64  `json:"used"`
}

type otelCount struct {
	Kind string  `json:"kind"`
	PerS float64 `json:"perS"`
}

// opAcc sums a histogram over attribute sets that differ only in batch size or peer.
type opAcc struct {
	count, cmds uint64
	sum         float64
	bounds      []float64
	buckets     []uint64
}

func (a *opAcc) add(p metricdata.HistogramDataPoint[float64], prev metricdata.HistogramDataPoint[float64], batch int64) {
	n := p.Count - prev.Count
	if n == 0 {
		return
	}
	a.count += n
	a.sum += p.Sum - prev.Sum
	if batch > 0 {
		a.cmds += n * uint64(batch)
	}
	if a.buckets == nil {
		a.bounds = p.Bounds
		a.buckets = make([]uint64, len(p.BucketCounts))
	}
	for i := range p.BucketCounts {
		if i < len(a.buckets) {
			d := p.BucketCounts[i]
			if i < len(prev.BucketCounts) {
				d -= prev.BucketCounts[i]
			}
			a.buckets[i] += d
		}
	}
}

// quantile is the upper bound of the bucket that holds q, in ms.
func (a *opAcc) quantile(q float64) float64 {
	if a.count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(a.count)))
	var seen uint64
	for i, c := range a.buckets {
		seen += c
		if seen >= rank {
			if i < len(a.bounds) {
				return a.bounds[i] * 1000
			}
			return a.bounds[len(a.bounds)-1] * 1000
		}
	}
	return 0
}

func attrKey(name string, s attribute.Set) string {
	return name + "|" + s.Encoded(attribute.DefaultEncoder())
}

func ours(s attribute.Set) bool {
	addr, _ := s.Value("server.address")
	a := addr.AsString()
	if a == "demo-admin" || a == "demo-probe" {
		return false
	}
	if pool, ok := s.Value("db.client.connection.pool.name"); ok {
		p := pool.AsString()
		return !strings.HasPrefix(p, "demo-admin") && !strings.HasPrefix(p, "demo-probe")
	}
	return true
}

// handshake commands every new connection sends: kept out of the table.
var handshake = map[string]bool{"hello": true, "client": true, "auth": true, "select": true, "ping": true}

// sample collects once and returns the change since the last sample.
func (o *otelMeter) sample(ctx context.Context) otelView {
	v := otelView{On: o.on, Ops: []otelOp{}, Pools: []otelPool{}, Errors: []otelCount{}, Closed: []otelCount{}}
	if !o.on {
		return v
	}
	var rm metricdata.ResourceMetrics
	if err := o.reader.Collect(ctx, &rm); err != nil {
		return v
	}
	now := time.Now()
	dt := now.Sub(o.last).Seconds()
	first := o.last.IsZero()
	o.last = now
	ops := map[string]*opAcc{}
	var wait, dial opAcc
	errs, closed := map[string]float64{}, map[string]float64{}
	pools := map[string]*otelPool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, p := range d.DataPoints {
					if !ours(p.Attributes) {
						continue
					}
					k := attrKey(m.Name, p.Attributes)
					prev := o.prevH[k]
					o.prevH[k] = p
					switch m.Name {
					case "db.client.operation.duration":
						nameV, _ := p.Attributes.Value("db.operation.name")
						name := nameV.AsString()
						if handshake[name] {
							continue
						}
						batch, _ := p.Attributes.Value("db.operation.batch.size")
						if ops[name] == nil {
							ops[name] = &opAcc{}
						}
						ops[name].add(p, prev, batch.AsInt64())
					case "db.client.connection.wait_time":
						wait.add(p, prev, 0)
					case "db.client.connection.create_time":
						dial.add(p, prev, 0)
					}
				}
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					if !ours(p.Attributes) {
						continue
					}
					k := attrKey(m.Name, p.Attributes)
					delta := p.Value - o.prevS[k]
					o.prevS[k] = p.Value
					switch m.Name {
					case "db.client.connection.count":
						pv, _ := p.Attributes.Value("db.client.connection.pool.name")
						st, _ := p.Attributes.Value("db.client.connection.state")
						name := poolLabel(pv.AsString())
						if pools[name] == nil {
							pools[name] = &otelPool{Name: name}
						}
						if st.AsString() == "used" {
							pools[name].Used += p.Value
						} else {
							pools[name].Idle += p.Value
						}
					case "db.client.connection.pending_requests":
						v.Pending += p.Value
					case "redis.client.errors":
						t, _ := p.Attributes.Value("error.type")
						errs[t.AsString()] += float64(delta)
					case "redis.client.connection.closed":
						r, _ := p.Attributes.Value("redis.client.connection.close.reason")
						closed[r.AsString()] += float64(delta)
					}
				}
			}
		}
	}
	if first || dt <= 0 {
		return v // the first collect holds everything since start: no rate yet
	}
	for name, a := range ops {
		if a.count == 0 {
			continue
		}
		op := otelOp{Name: strings.ToUpper(name), PerS: float64(a.count) / dt, MeanMs: a.sum / float64(a.count) * 1000,
			P50Ms: a.quantile(0.50), P99Ms: a.quantile(0.99)}
		if a.cmds > 0 {
			op.CmdsPerOp = float64(a.cmds) / float64(a.count)
		}
		v.Ops = append(v.Ops, op)
	}
	sort.Slice(v.Ops, func(i, j int) bool { return v.Ops[i].PerS > v.Ops[j].PerS })
	v.WaitPerS, v.WaitP99Ms = float64(wait.count)/dt, wait.quantile(0.99)
	v.DialsPerS, v.DialP99Ms = float64(dial.count)/dt, dial.quantile(0.99)
	for _, p := range pools {
		if p.Idle+p.Used > 0 {
			v.Pools = append(v.Pools, *p)
		}
	}
	sort.Slice(v.Pools, func(i, j int) bool { return v.Pools[i].Name < v.Pools[j].Name })
	for k, n := range errs {
		if n > 0 {
			v.Errors = append(v.Errors, otelCount{Kind: k, PerS: n / dt})
		}
	}
	for k, n := range closed {
		if n > 0 {
			v.Closed = append(v.Closed, otelCount{Kind: k, PerS: n / dt})
		}
	}
	return v
}

// poolLabel turns "127.0.0.1:61234_7_pipeline" into "pipeline pool #7".
func poolLabel(name string) string {
	kind := "pool"
	if s, ok := strings.CutSuffix(name, "_pipeline"); ok {
		kind, name = "pipeline pool", s
	} else if s, ok := strings.CutSuffix(name, "_pubsub"); ok {
		kind, name = "pubsub pool", s
	}
	if i := strings.LastIndex(name, "_"); i >= 0 {
		return kind + " #" + name[i+1:]
	}
	return kind
}
