package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"
)

// Opt-in correctness smoke only, never a performance test or server launcher.
// The operator must verify OWNED Redis Software and active/effective 4x QPF
// first, then set ROADTEST_SOFTWARE_SMOKE=1 and
// ROADTEST_SOFTWARE_VERIFIED_4X_QPF=1. REDIS_URI must use rediss; SSL_CERT_FILE
// supplies its CA. Keep the environment and logs private. No Community/Valkey/
// ElastiCache/Memorystore endpoint or the repository's Compose is permitted.
func TestTLSSoftwareSmoke(t *testing.T) {
	if os.Getenv("ROADTEST_SOFTWARE_SMOKE") != "1" {
		t.Skip("owned verified Software smoke is opt-in")
	}
	if os.Getenv("ROADTEST_SOFTWARE_VERIFIED_4X_QPF") != "1" {
		t.Fatal("verify active/effective Software 4x QPF first")
	}
	u, err := url.Parse(os.Getenv("REDIS_URI"))
	if err != nil || u.Scheme != "rediss" || u.Host == "" {
		t.Fatal("REDIS_URI must be a valid rediss endpoint")
	}
	policy, err := loadTLS(true, os.Getenv("SSL_CERT_FILE"), "")
	if err != nil {
		t.Fatal("cannot load fixture TLS policy")
	}
	auth := target{addr: u.Host, tls: policy}
	if u.User != nil {
		auth.user = u.User.Username()
		auth.pass, _ = u.User.Password()
	}
	proxy, err := newRoadProxy(auth.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	suffix := strconv.Itoa(os.Getpid()) + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
	prefix := "adbc:tls-road-smoke:" + suffix
	stringKey, hashKey := prefix+":string", prefix+":hash"
	for _, mode := range []string{"direct", "proxy"} {
		t.Run(mode, func(t *testing.T) {
			addr := auth.addr
			if mode == "proxy" {
				addr = proxy.Addr()
			}
			st := newServerStats(auth, addr)
			defer st.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			n, err := st.admin.Exists(ctx, stringKey, hashKey).Result()
			if err != nil || n != 0 {
				t.Fatal("smoke keys unavailable; no existing key will be changed")
			}
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := st.admin.Del(cleanup, stringKey, hashKey).Err(); err != nil {
					t.Error("smoke key cleanup failed")
				}
				if n, err := st.admin.Exists(cleanup, stringKey, hashKey).Result(); err != nil || n != 0 {
					t.Error("smoke keys remain after cleanup")
				}
			}()
			// Seed through the exact admin client supplied to workload Seed methods;
			// st.seed itself hardcodes demo:key:* and is intentionally not invoked.
			p := st.admin.Pipeline()
			p.Set(ctx, stringKey, "tls-smoke", 0)
			p.HSet(ctx, hashKey, "empty", "", "exact", "9223372036854775807")
			if _, err := p.Exec(ctx); err != nil {
				t.Fatal("verified TLS seed pipeline failed")
			}
			if err := st.admin.Ping(ctx).Err(); err != nil {
				t.Fatal("verified TLS admin ping failed")
			}
			if st.measuredRTT(ctx) < 0 {
				t.Fatal("verified TLS RTT probe failed")
			}
			cfg := defaultConfig()
			cfg.PoolSize = 2
			cfg.MinIdleConns = 0
			for e := Engine(0); e < numEngines; e++ {
				t.Run(e.String(), func(t *testing.T) {
					r, err := startEngine(addr, auth, e, 0, 1, cfg, newLiveHist())
					if err != nil {
						t.Fatalf("start %s failed", e)
					}
					defer r.stop()
					if r.rdb.Options().Protocol != 3 {
						t.Fatal("smoke requires RESP3")
					}
					if got, err := r.cmds.Get(ctx, stringKey).Result(); err != nil || got != "tls-smoke" {
						t.Fatal("engine GET mismatch/error")
					}
					values, err := r.cmds.HMGet(ctx, hashKey, "empty", "missing", "exact").Result()
					if err != nil || len(values) != 3 || values[0] != "" || values[1] != nil || values[2] != "9223372036854775807" {
						t.Fatal("engine HMGET nil/empty/exact bytes mismatch/error")
					}
				})
			}
			t.Log(fmt.Sprintf("verified TLS/RESP3: admin seed, ping, RTT probe and %d engine faces passed", numEngines))
		})
	}
}
