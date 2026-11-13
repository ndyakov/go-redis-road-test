package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Workload is one kind of request. Register it once with RegisterWorkload;
// the demo makes one Caller per caller goroutine.
type Workload interface {
	Info() WorkloadInfo
	NewCaller(env *CallerEnv) Caller
}

// Caller runs the requests of one caller goroutine. It may keep state across
// requests (a counter, a random source): only its own goroutine calls it.
type Caller interface {
	// Start sends the commands of one request, in slot 0 to Window-1 of the
	// window. On the blocking faces each command waits for its reply; on the
	// async face it returns at once. Return every command sent: the loop
	// waits for them and counts their errors (redis.Nil is not an error).
	Start(ctx context.Context, slot int, key string) []redis.Cmder
	// Finish runs after the replies of Start are in, without errors. It may
	// send follow-up commands. later are fire-and-forget commands: the loop
	// checks their errors one window later, and the request's latency does
	// not wait for them. A non-nil error counts as a failed request.
	Finish(ctx context.Context, slot int, key string, cmds []redis.Cmder) (later []redis.Cmder, err error)
}

// WorkloadInfo describes a workload to the demo and the page.
type WorkloadInfo struct {
	ID   string `json:"id"`   // stable: the JSON API and the page's saved state use it
	Name string `json:"name"` // the button
	Note string `json:"note"` // the line under the buttons
	// Commands is how many commands one request sends, for the Redis card; 0 means 1.
	Commands int `json:"commands"`
	// Trips is the round trips of one request on the blocking faces, for the
	// road ceiling; 0 means 1. The async face sends a request's commands at once: 1.
	Trips int `json:"-"`
	// Work is the app work of one request. The loop sleeps Work x window once
	// per window, after Start and before Finish: while the commands travel on
	// the async face, after the replies on the blocking faces.
	Work time.Duration `json:"-"`
	// ZeroCopy: the GETs use env.Get, so the GetToBuffer option applies.
	ZeroCopy bool `json:"zeroCopy"`
	// ChecksOrder: the workload reports out-of-order replies (env.OutOfOrder);
	// a scenario with any cannot be the best.
	ChecksOrder bool `json:"checksOrder"`
	// LoadKey is added to the scoreboard's load name, for the options this
	// workload reads (" 20%w"), so different settings do not share a best.
	LoadKey string `json:"-"`
}

// configInfo is an optional Workload method, for a workload whose info
// depends on the config (the page: its fan-out and app work).
type configInfo interface {
	InfoFor(cfg config) WorkloadInfo
}

// Seeder is an optional Workload method, for a workload that reads its own
// keys. The demo calls Seed before the engine starts, with the load stopped,
// on the plain client (not the engine under test). Seed runs again on every
// option change, so it must be idempotent and cheap when nothing changed.
type Seeder interface {
	Seed(ctx context.Context, rdb *redis.Client, cfg config) error
}

// CallerEnv is what a Caller gets from the demo.
type CallerEnv struct {
	Cmds   commands // the client or the autopipeliner of the engine
	Config config
	Value  []byte // the value to SET, Config.ValueSize bytes
	Async  bool   // the async face: Start returns before the replies
	ID     int    // this caller, 0 to callers-1
	Window int    // requests in flight per caller: the slots; 1 on the blocking faces

	bufs     [][]byte // zero copy: one buffer per slot
	disorder *atomic.Int64
}

// Key is the i-th seeded key, wrapped to Config.Keys.
func (e *CallerEnv) Key(i int) string { return key(i % e.Config.Keys) }

// Get is a GET of k for this slot: GetToBuffer into the slot's buffer when
// the GetToBuffer option is on and the workload has ZeroCopy, else Get.
func (e *CallerEnv) Get(ctx context.Context, slot int, k string) redis.Cmder {
	if e.bufs != nil {
		return e.Cmds.GetToBuffer(ctx, k, e.bufs[slot])
	}
	return e.Cmds.Get(ctx, k)
}

// OutOfOrder counts one reply that came back out of order.
func (e *CallerEnv) OutOfOrder() { e.disorder.Add(1) }

// workloadList is every registered workload, in the page's order. The
// built-ins are set here, before any init() runs, so they come first.
var workloadList = []Workload{getWorkload{}, setWorkload{}, mixWorkload{}, cacheFillWorkload{}, pageWorkload{}, orderWorkload{}}

const defaultWorkload = "get"

// RegisterWorkload adds a workload to the page, the API and the terminal.
// Call it from an init() in package main. It panics on an empty or taken ID.
func RegisterWorkload(w Workload) {
	id := w.Info().ID
	if id == "" || workloadByID(id) != nil {
		panic(fmt.Sprintf("RegisterWorkload: empty or duplicate ID %q", id))
	}
	workloadList = append(workloadList, w)
}

func workloadByID(id string) Workload {
	for _, w := range workloadList {
		if w.Info().ID == id {
			return w
		}
	}
	return nil
}

// workloadInfoFor is w's info under cfg.
func workloadInfoFor(w Workload, cfg config) WorkloadInfo {
	if c, ok := w.(configInfo); ok {
		return c.InfoFor(cfg)
	}
	return w.Info()
}

// workloadInfos is the list for the page, never nil.
func workloadInfos(cfg config) []WorkloadInfo {
	out := make([]WorkloadInfo, 0, len(workloadList))
	for _, w := range workloadList {
		out = append(out, workloadInfoFor(w, cfg))
	}
	return out
}

// noFinish is a Finish with nothing to do.
type noFinish struct{}

func (noFinish) Finish(context.Context, int, string, []redis.Cmder) ([]redis.Cmder, error) {
	return nil, nil
}

// get: one GET per request.
type getWorkload struct{}

func (getWorkload) Info() WorkloadInfo {
	return WorkloadInfo{ID: "get", Name: "GET", Note: "one GET per request", ZeroCopy: true}
}
func (getWorkload) NewCaller(env *CallerEnv) Caller { return getCaller{env: env} }

type getCaller struct {
	noFinish
	env *CallerEnv
}

func (c getCaller) Start(ctx context.Context, slot int, k string) []redis.Cmder {
	return []redis.Cmder{c.env.Get(ctx, slot, k)}
}

// set: one SET per request.
type setWorkload struct{}

func (setWorkload) Info() WorkloadInfo {
	return WorkloadInfo{ID: "set", Name: "SET", Note: "one SET per request"}
}
func (setWorkload) NewCaller(env *CallerEnv) Caller { return setCaller{env: env} }

type setCaller struct {
	noFinish
	env *CallerEnv
}

func (c setCaller) Start(ctx context.Context, _ int, k string) []redis.Cmder {
	return []redis.Cmder{c.env.Cmds.Set(ctx, k, c.env.Value, 0)}
}

// mix: a SET for the write share (Config.WritePct), else a GET.
type mixWorkload struct{}

func (mixWorkload) Info() WorkloadInfo {
	return WorkloadInfo{ID: "mix", Name: "GET / SET mix", Note: "a SET for the write share, else a GET", ZeroCopy: true}
}

func (w mixWorkload) InfoFor(cfg config) WorkloadInfo {
	i := w.Info()
	i.LoadKey = fmt.Sprintf(" %d%%w", cfg.WritePct)
	return i
}

func (mixWorkload) NewCaller(env *CallerEnv) Caller {
	return &mixCaller{env: env, rnd: uint64(env.ID)*0x9E3779B97F4A7C15 + 1}
}

type mixCaller struct {
	noFinish
	env *CallerEnv
	rnd uint64 // xorshift
}

func (c *mixCaller) Start(ctx context.Context, slot int, k string) []redis.Cmder {
	c.rnd ^= c.rnd << 13
	c.rnd ^= c.rnd >> 7
	c.rnd ^= c.rnd << 17
	if int(c.rnd%100) < c.env.Config.WritePct {
		return []redis.Cmder{c.env.Cmds.Set(ctx, k, c.env.Value, 0)}
	}
	return []redis.Cmder{c.env.Get(ctx, slot, k)}
}

// ratelimit: INCRBY + EXPIRE NX, two commands known together. Not in
// workloadList (it was not offered before the registry either); add it there,
// or RegisterWorkload(rateLimitWorkload{}), to offer it.
type rateLimitWorkload struct{}

func (rateLimitWorkload) Info() WorkloadInfo {
	return WorkloadInfo{ID: "ratelimit", Name: "rate limiter", Note: "INCRBY + EXPIRE NX: two commands, known together", Commands: 2, Trips: 2}
}
func (rateLimitWorkload) NewCaller(env *CallerEnv) Caller { return rateLimitCaller{env: env} }

type rateLimitCaller struct {
	noFinish
	env *CallerEnv
}

func (c rateLimitCaller) Start(ctx context.Context, _ int, k string) []redis.Cmder {
	rk := "demo:rl:" + k
	return []redis.Cmder{c.env.Cmds.IncrBy(ctx, rk, 1), c.env.Cmds.ExpireNX(ctx, rk, time.Minute)}
}

// cachefill: GET, then SET a value built from its reply. The SET needs the
// GET's reply, so no pipeline can merge the two.
type cacheFillWorkload struct{}

func (cacheFillWorkload) Info() WorkloadInfo {
	return WorkloadInfo{ID: "cachefill", Name: "cache fill", Note: "GET, then SET: the SET needs the GET reply; async does not wait for the SET", Commands: 2, Trips: 2}
}
func (cacheFillWorkload) NewCaller(env *CallerEnv) Caller { return cacheFillCaller{env: env} }

type cacheFillCaller struct{ env *CallerEnv }

func (c cacheFillCaller) Start(ctx context.Context, _ int, k string) []redis.Cmder {
	return []redis.Cmder{c.env.Cmds.Get(ctx, k)} // a plain GET: its reply is the SET's value
}

func (c cacheFillCaller) Finish(ctx context.Context, _ int, k string, cmds []redis.Cmder) ([]redis.Cmder, error) {
	page := cmds[0].(*redis.StringCmd).Val()
	set := c.env.Cmds.Set(ctx, "demo:page:"+k, page, time.Minute)
	if c.env.Async {
		return []redis.Cmder{set}, nil // the request is done; the SET finishes on its own
	}
	return nil, set.Err() // the blocking face waits for the SET: two trips
}

// page: FanOut independent GETs and app work. Blocking: one GET after the
// other. Async: send all, work while they travel, wait once.
type pageWorkload struct{}

func (pageWorkload) Info() WorkloadInfo {
	return WorkloadInfo{ID: "page", Name: "page render", Note: "a page needs N keys and some app work. Blocking: one GET after the other, N round trips, then the work. Async: send all N, do the work while they travel, wait once"}
}

func (w pageWorkload) InfoFor(cfg config) WorkloadInfo {
	i := w.Info()
	i.Commands, i.Trips, i.Work = cfg.FanOut, cfg.FanOut, cfg.work()
	i.LoadKey = fmt.Sprintf(" fan%d work%dus", cfg.FanOut, cfg.WorkUs)
	return i
}

func (pageWorkload) NewCaller(env *CallerEnv) Caller {
	return &pageCaller{env: env, n: env.ID * 7, pages: make([][]redis.Cmder, env.Window)}
}

type pageCaller struct {
	noFinish
	env   *CallerEnv
	n     int             // the page's own key counter
	pages [][]redis.Cmder // one reused slice per slot
}

func (c *pageCaller) Start(ctx context.Context, slot int, _ string) []redis.Cmder {
	c.n++
	page := c.pages[slot][:0]
	for j := 0; j < c.env.Config.FanOut; j++ {
		page = append(page, c.env.Cmds.Get(ctx, c.env.Key(c.n+j)))
	}
	c.n += c.env.Config.FanOut - 1
	c.pages[slot] = page
	return page
}

// order: INCR on the caller's own key. Replies are read in submit order, so
// in order each is the last + 1.
type orderWorkload struct{}

func (orderWorkload) Info() WorkloadInfo {
	return WorkloadInfo{ID: "order", Name: "ordering check", Note: "INCR on each caller's own key: a reply that is not the last + 1 came back out of order", ChecksOrder: true}
}

func (orderWorkload) NewCaller(env *CallerEnv) Caller {
	return &orderCaller{env: env, key: fmt.Sprintf("demo:seq:%s:%d", clientName, env.ID)}
}

type orderCaller struct {
	env  *CallerEnv
	key  string
	last int64
}

func (c *orderCaller) Start(ctx context.Context, _ int, _ string) []redis.Cmder {
	return []redis.Cmder{c.env.Cmds.IncrBy(ctx, c.key, 1)}
}

func (c *orderCaller) Finish(_ context.Context, _ int, _ string, cmds []redis.Cmder) ([]redis.Cmder, error) {
	v := cmds[0].(*redis.IntCmd).Val()
	if c.last != 0 && v != c.last+1 {
		c.env.OutOfOrder()
	}
	c.last = v
	return nil, nil
}
