package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// A big pool on a laptop runs into two macOS limits:
//
//   - the listen queue holds 128 pending connections (kern.ipc.somaxconn): when
//     a fresh pool of 2000 dials all at once, the rest are dropped and time out;
//   - closed sockets sit in TIME_WAIT for 30 s and the ephemeral ports run out
//     (about 16k): every restart of a 2000-connection pool, through the road
//     proxy, uses two ports per connection.
//
// demoDialer caps the dials in flight and closes connections with linger 0
// (a reset, no TIME_WAIT). That is fine here: the demo closes a connection only
// after its load has stopped. Do not copy it into an application.
var dialSlots = make(chan struct{}, 64)

func demoDialer(ctx context.Context, network, addr string) (net.Conn, error) {
	select {
	case dialSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-dialSlots }()
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	noLinger(c)
	return c, nil
}

func noLinger(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
		_ = tc.SetNoDelay(true)
	}
}

// demoLogger keeps go-redis's log off the terminal (it would break the TUI) and
// puts it in the demo's events instead: at most one line a second, with a count.
type demoLogger struct {
	d     *demo
	count atomic.Int64
	mu    sync.Mutex
	last  time.Time
}

func (l *demoLogger) Printf(_ context.Context, format string, v ...interface{}) {
	n := l.count.Add(1)
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.last) < time.Second {
		return
	}
	l.last = time.Now()
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "can't assign requested address") {
		msg += " (out of local ports: too many connections opened and closed; lower PoolSize or wait 30 s)"
	}
	l.d.mu.Lock()
	l.d.event("go-redis (%d so far): %s", n, msg)
	l.d.mu.Unlock()
}
