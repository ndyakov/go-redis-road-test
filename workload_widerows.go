package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// widerows: one HMGET of a wide hash row per request, every reply checked.
// It reproduces the workload of go-redis issue #4299 and is a full example of
// a plug-in workload with seeding (Seeder) and validation.

func init() { RegisterWorkload(wideRowsWorkload{}) }

const (
	wideCols      = 105
	wideSchema    = 1 // bump when the value rules below change: the rows are seeded again
	wideMarker    = "demo:row:seeded"
	wideBatch     = 500 // rows per seeding pipeline
	wideBigIntMin = 9007199254740993
)

// wideFields is the fixed schema, f000 to f104, in HMGET order.
var wideFields = func() []string {
	f := make([]string, wideCols)
	for i := range f {
		f[i] = fmt.Sprintf("f%03d", i)
	}
	return f
}()

func wideKey(row int) string { return "demo:row:" + strconv.Itoa(row) }

// The kind of a field: (row + col) % 4, so every row has all four.
const (
	wideMissing = iota // not in the hash: HMGET gives nil
	wideEmpty          // ""
	wideString         // a short string
	wideBigInt         // an integer above 2^53, as a decimal string
)

// wideValue appends the value of (row, col) to buf and reports whether the
// field exists.
func wideValue(buf []byte, row, col int) ([]byte, bool) {
	switch (row + col) % 4 {
	case wideMissing:
		return buf, false
	case wideEmpty:
		return buf, true
	case wideString:
		buf = append(buf, 's')
		buf = strconv.AppendInt(buf, int64(row), 10)
		buf = append(buf, '.')
		return strconv.AppendInt(buf, int64(col), 10), true
	default:
		return strconv.AppendInt(buf, int64(wideBigIntMin+row*wideCols+col), 10), true
	}
}

type wideRowsWorkload struct{}

func (wideRowsWorkload) Info() WorkloadInfo {
	return WorkloadInfo{
		ID: "widerows", Name: "wide HMGET rows",
		Note:     "HMGET of 105 fields per row (Keys rows), every reply checked: nil, empty, strings, big integers",
		Commands: 1, Trips: 1,
	}
}

func (wideRowsWorkload) NewCaller(env *CallerEnv) Caller {
	return &wideRowsCaller{env: env, n: env.ID * 7, rows: make([]int, env.Window), buf: make([]byte, 0, 32)}
}

// Seed writes Keys rows, unless the marker says they are there. Each row is
// deleted and written again, so a changed field is restored.
func (wideRowsWorkload) Seed(ctx context.Context, rdb *redis.Client, cfg config) error {
	want := fmt.Sprintf("%d:%d", cfg.Keys, wideSchema)
	if got, err := rdb.Get(ctx, wideMarker).Result(); err == nil && got == want {
		return nil
	} else if err != nil && err != redis.Nil {
		return err
	}
	args := make([]interface{}, 0, 2*wideCols)
	var buf []byte
	for from := 0; from < cfg.Keys; from += wideBatch {
		pipe := rdb.Pipeline()
		for row := from; row < min(from+wideBatch, cfg.Keys); row++ {
			args = args[:0]
			for col, f := range wideFields {
				var ok bool
				if buf, ok = wideValue(buf[:0], row, col); ok {
					args = append(args, f, string(buf))
				}
			}
			k := wideKey(row)
			pipe.Del(ctx, k)
			pipe.HSet(ctx, k, args...)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return rdb.Set(ctx, wideMarker, want, 0).Err()
}

type wideRowsCaller struct {
	env  *CallerEnv
	n    int    // the caller's own row counter
	rows []int  // the row of each slot
	buf  []byte // scratch for the expected value
}

func (c *wideRowsCaller) Start(ctx context.Context, slot int, _ string) []redis.Cmder {
	row := c.n % c.env.Config.Keys
	c.n++
	c.rows[slot] = row
	return []redis.Cmder{c.env.Cmds.HMGet(ctx, wideKey(row), wideFields...)}
}

// Finish checks every field of the reply against the schema.
func (c *wideRowsCaller) Finish(_ context.Context, slot int, _ string, cmds []redis.Cmder) ([]redis.Cmder, error) {
	row := c.rows[slot]
	vals := cmds[0].(*redis.SliceCmd).Val()
	if len(vals) != wideCols {
		return nil, fmt.Errorf("%s: %d fields, want %d", wideKey(row), len(vals), wideCols)
	}
	for col, v := range vals {
		var ok bool
		c.buf, ok = wideValue(c.buf[:0], row, col)
		if !ok {
			if v != nil {
				return nil, fmt.Errorf("%s %s: got %q, want nil", wideKey(row), wideFields[col], v)
			}
			continue
		}
		s, isStr := v.(string)
		if !isStr || s != string(c.buf) {
			return nil, fmt.Errorf("%s %s: got %#v, want %q", wideKey(row), wideFields[col], v, c.buf)
		}
	}
	return nil, nil
}

var _ Seeder = wideRowsWorkload{}
