package main

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// roadProxy sits between the demo clients and Redis and adds a round trip: each
// With TLS the road forwards encrypted bytes unchanged; clients authenticate
// the Redis target end to end. The upstream TCP socket is not a Redis client.
// Each direction holds every chunk for RTT/2. It stamps a chunk when it is read and
// releases it at stamp+delay, so many chunks are on the road at once. A proxy
// that waited per chunk before reading the next would serialize the traffic
// itself and hide the difference between the engines.
type roadProxy struct {
	ln       net.Listener
	upstream atomic.Value // string: the Redis address; changes when you connect elsewhere
	halfRTT  atomic.Int64 // nanoseconds, per direction
	wg       sync.WaitGroup
}

// oversleep is how late time.Sleep wakes, as parts per million of the sleep
// (about 15% on macOS). The road sleeps that much less, so the RTT it adds
// matches the setting. Learned as a moving average across all lanes.
var oversleep atomic.Int64

// sleepUntil never returns before release: a chunk released early would make
// the road shorter than its setting. Only the first sleep is shortened by the
// learned oversleep; whatever is left is slept again.
func sleepUntil(release time.Time) {
	first := true
	for {
		d := time.Until(release)
		if d <= 0 {
			return
		}
		s := d
		if first {
			ratio := float64(oversleep.Load()) / 1e6
			s = time.Duration(float64(d) / (1 + ratio))
		}
		t0 := time.Now()
		time.Sleep(s)
		if first && s > 50*time.Microsecond {
			got := int64(float64(time.Since(t0)-s) / float64(s) * 1e6)
			got = min(max(got, 0), 300_000) // cap at 30%: a busy CPU wakes late too, that is not the timer
			old := oversleep.Load()
			oversleep.Store(old + (got-old)/32)
		}
		first = false
	}
}

type chunk struct {
	data    []byte
	release time.Time
}

func newRoadProxy(upstream string) (*roadProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &roadProxy{ln: ln}
	p.upstream.Store(upstream)
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

func (p *roadProxy) Addr() string { return p.ln.Addr().String() }

// SetUpstream points new road connections at another Redis.
func (p *roadProxy) SetUpstream(addr string) { p.upstream.Store(addr) }

// SetRTT changes the added round trip for traffic read from now on. The learned
// oversleep starts again from the typical 15%: it depends on the sleep length and
// on the load, so a value learned on another road would be wrong here.
func (p *roadProxy) SetRTT(rtt time.Duration) {
	p.halfRTT.Store(int64(rtt / 2))
	oversleep.Store(150_000)
}

func (p *roadProxy) RTT() time.Duration { return time.Duration(p.halfRTT.Load() * 2) }

func (p *roadProxy) Close() error {
	err := p.ln.Close()
	p.wg.Wait()
	return err
}

func (p *roadProxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serve(c)
	}
}

func (p *roadProxy) serve(client net.Conn) {
	server, err := net.DialTimeout("tcp", p.upstream.Load().(string), 10*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	noLinger(client) // closes send a reset: no TIME_WAIT, no port exhaustion (net.go)
	noLinger(server)
	var once sync.Once
	closeBoth := func() { once.Do(func() { _ = client.Close(); _ = server.Close() }) }
	go p.lane(client, server, closeBoth)
	go p.lane(server, client, closeBoth)
}

// lane copies src to dst, holding each chunk until its release time.
func (p *roadProxy) lane(src, dst net.Conn, closeBoth func()) {
	q := make(chan chunk, 8192)
	go func() {
		defer closeBoth()
		for c := range q {
			sleepUntil(c.release)
			if _, err := dst.Write(c.data); err != nil {
				closeBoth()   // unblocks the reader's Read
				for range q { // drain so the reader can exit
				}
				return
			}
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			q <- chunk{data: data, release: time.Now().Add(time.Duration(p.halfRTT.Load()))}
		}
		if err != nil {
			if err != io.EOF {
				closeBoth()
			}
			close(q)
			return
		}
	}
}
