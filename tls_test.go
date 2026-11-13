package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"github.com/redis/go-redis/v9"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These are local transport fixtures, not Redis servers. The TLS peer echoes
// one byte only after certificate verification succeeds.
func testCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"redis.test", "localhost"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: leafKey}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}

func testPolicy(t *testing.T, ca []byte, name string) *tls.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, ca, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadTLS(true, path, name)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func tlsPeer(t *testing.T, cert tls.Certificate) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer raw.Close()
				_ = raw.SetDeadline(time.Now().Add(3 * time.Second))
				c := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
				if c.Handshake() != nil {
					return
				}
				b := make([]byte, 1)
				if _, err := io.ReadFull(c, b); err == nil {
					_, _ = c.Write(b)
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); wg.Wait() })
	return ln.Addr().String()
}

func assertSecureEcho(t *testing.T, dial func(context.Context, string, string) (net.Conn, error), addr string, label bool) {
	t.Helper()
	c, err := dial(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	underlying := c
	if label {
		lc, ok := c.(labelConn)
		if !ok {
			t.Fatalf("missing monitor label: %T", c)
		}
		underlying = lc.Conn
	}
	tc, ok := underlying.(*tls.Conn)
	if !ok || !tc.ConnectionState().HandshakeComplete || len(tc.ConnectionState().VerifiedChains) == 0 {
		t.Fatalf("unverified connection: %T", underlying)
	}
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(c, b); err != nil || b[0] != 'x' {
		t.Fatalf("echo: %q %v", b, err)
	}
}

func TestTLSAllClientPaths(t *testing.T) {
	cert, ca := testCertificate(t)
	addr := tlsPeer(t, cert)
	proxy, err := newRoadProxy(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	for _, mode := range []string{"direct", "proxy", "inferred direct", "inferred proxy"} {
		t.Run(mode, func(t *testing.T) {
			destination := addr
			if strings.Contains(mode, "proxy") {
				destination = proxy.Addr()
			}
			policy := testPolicy(t, ca, "redis.test")
			auth := target{addr: addr, tls: policy}
			if strings.HasPrefix(mode, "inferred") {
				_, port, _ := net.SplitHostPort(addr)
				auth.addr = net.JoinHostPort("localhost", port)
				policy.ServerName = ""
			}
			for e := Engine(0); e < numEngines; e++ {
				t.Run(e.String(), func(t *testing.T) {
					opt := engineOptions(destination, auth, e, defaultConfig())
					if opt.TLSConfig == nil {
						t.Fatal("missing TLS options metadata")
					}
					if opt.Protocol != 3 {
						t.Fatal("engine must default RESP3")
					}
					assertSecureEcho(t, opt.Dialer, opt.Addr, false)
				})
			}
			st := newServerStats(auth, destination)
			defer st.Close()
			for _, client := range []*redis.Client{st.admin, st.probe} {
				opt := client.Options()
				if opt.TLSConfig == nil {
					t.Fatal("missing TLS options metadata")
				}
				if opt.Protocol != 3 {
					t.Fatal("monitor must use RESP3")
				}
				assertSecureEcho(t, opt.Dialer, opt.Addr, true)
			}
		})
	}
}

func TestTLSRejectsUntrustedAndWrongName(t *testing.T) {
	cert, ca := testCertificate(t)
	_, wrongCA := testCertificate(t)
	addr := tlsPeer(t, cert)
	proxy, err := newRoadProxy(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	for _, destination := range []string{addr, proxy.Addr()} {
		for _, tc := range []struct {
			name   string
			ca     []byte
			server string
		}{
			{"wrong CA", wrongCA, "redis.test"}, {"wrong name", ca, "wrong.test"},
		} {
			t.Run(tc.name+destination, func(t *testing.T) {
				auth := target{addr: addr, tls: testPolicy(t, tc.ca, tc.server)}
				c, err := auth.dialer(time.Second)(context.Background(), "tcp", destination)
				if c != nil {
					c.Close()
					t.Fatal("failed TLS returned a connection")
				}
				if err == nil {
					t.Fatal("invalid certificate accepted")
				}
			})
		}
	}
}

func TestTLSHandshakeCancellationClosesSocket(t *testing.T) {
	for _, route := range []string{"direct", "proxy"} {
		t.Run(route, func(t *testing.T) {
			for _, mode := range []string{"cancel", "parent deadline", "dial timeout"} {
				t.Run(mode, func(t *testing.T) {
					ln, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer ln.Close()
					accepted := make(chan struct{})
					closed := make(chan error, 1)
					go func() {
						c, err := ln.Accept()
						if err != nil {
							closed <- err
							return
						}
						defer c.Close()
						close(accepted)
						_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
						_, err = io.Copy(io.Discard, c)
						closed <- err
					}()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					timeout := time.Second
					if mode == "parent deadline" {
						ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
						defer cancel()
					}
					if mode == "dial timeout" {
						timeout = 100 * time.Millisecond
					}
					if mode == "cancel" {
						go func() { <-accepted; cancel() }()
					}
					destination := ln.Addr().String()
					if route == "proxy" {
						proxy, err := newRoadProxy(destination)
						if err != nil {
							t.Fatal(err)
						}
						defer proxy.Close()
						destination = proxy.Addr()
					}
					auth := target{addr: ln.Addr().String(), tls: &tls.Config{MinVersion: tls.VersionTLS12}}
					c, err := auth.dialer(timeout)(ctx, "tcp", destination)
					if err == nil || c != nil {
						if c != nil {
							c.Close()
						}
						t.Fatalf("stalled handshake: conn=%v err=%v", c, err)
					}
					want := context.DeadlineExceeded
					if mode == "cancel" {
						want = context.Canceled
					}
					if !errors.Is(err, want) {
						t.Fatalf("expected %v, got %v", want, err)
					}
					select {
					case err := <-closed:
						if err != nil && !errors.Is(err, syscall.ECONNRESET) {
							t.Fatalf("socket not cleanly closed: %v", err)
						}
					case <-time.After(time.Second):
						t.Fatal("failed handshake left underlying socket open")
					}
				})
			}
		})
	}
}

func TestTLSPolicyAndDisplay(t *testing.T) {
	if c, err := loadTLS(false, "", ""); err != nil || c != nil {
		t.Fatal("plain compatibility")
	}
	for _, args := range [][2]string{{"ca.pem", ""}, {"", "name"}} {
		if _, err := loadTLS(false, args[0], args[1]); err == nil {
			t.Fatal("ignored TLS-only option")
		}
	}
	p := filepath.Join(t.TempDir(), "invalid.pem")
	_ = os.WriteFile(p, []byte("invalid"), 0600)
	if _, err := loadTLS(true, p, ""); err == nil {
		t.Fatal("invalid CA accepted")
	}
	original := &tls.Config{MinVersion: tls.VersionTLS12}
	first := target{addr: "first.test:6379", tls: original}
	second := first
	second.addr = "second.test:6380"
	for _, auth := range []target{first, second} {
		cfg, err := auth.tlsForTarget()
		if err != nil {
			t.Fatal(err)
		}
		host, _, _ := net.SplitHostPort(auth.addr)
		if cfg.ServerName != host || original.ServerName != "" {
			t.Fatal("mutated policy or wrong target name")
		}
		for e := Engine(0); e < numEngines; e++ {
			code := auth.clientCode(defaultConfig(), e)
			if !strings.Contains(code, "verified TLS") || !strings.Contains(code, "TLSConfig:") || !strings.Contains(code, host) {
				t.Fatalf("incomplete TLS sketch: %s", code)
			}
		}
	}
	if first.security() != "verified TLS" || (target{}).security() != "plain TCP" {
		t.Fatal("incorrect transport display")
	}
}

// A failed certificate verification must actively close the raw socket even
// when the peer has not closed its end. Count Close separately from TLS errors.
type countedCloseConn struct {
	net.Conn
	closes atomic.Int32
}

func (c *countedCloseConn) Close() error { c.closes.Add(1); return c.Conn.Close() }
func TestTLSVerificationFailureClosesUnderlying(t *testing.T) {
	cert, _ := testCertificate(t)
	client, server := net.Pipe()
	defer server.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{cert}})
		_ = s.Handshake()
	}()
	raw := &countedCloseConn{Conn: client}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := verifiedConnection(ctx, raw, &tls.Config{ServerName: "redis.test", RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12})
	if c != nil || err == nil || raw.closes.Load() != 1 {
		t.Fatalf("conn=%v err=%v closes=%d", c, err, raw.closes.Load())
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("peer handshake blocked")
	}
}
