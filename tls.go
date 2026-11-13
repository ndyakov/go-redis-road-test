package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// loadTLS builds an immutable policy. Each connection gets its own clone and
// derives the certificate name from the Redis target, never a proxy or label.
func loadTLS(enabled bool, caFile, serverName string) (*tls.Config, error) {
	if !enabled {
		if caFile != "" || serverName != "" {
			return nil, fmt.Errorf("--tls-ca and --tls-server-name require --tls")
		}
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("TLS CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("TLS CA file contains no certificates")
		}
		cfg.RootCAs = roots
	}
	return cfg, nil
}

func (t target) tlsForTarget() (*tls.Config, error) {
	if t.tls == nil {
		return nil, nil
	}
	cfg := t.tls.Clone()
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(t.addr)
		if err != nil {
			return nil, fmt.Errorf("TLS target address: %w", err)
		}
		cfg.ServerName = host
	}
	return cfg, nil
}

// go-redis skips its default TLS wrapping when a custom Dialer is present.
// Both engine and monitoring dialers must explicitly secure the socket exactly
// once. The road is an opaque byte relay: TLS ends at Redis, not at the relay.
func (t target) dialer(timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		cfg, err := t.tlsForTarget()
		if err != nil {
			return nil, err
		}
		raw, err := demoDialer(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return raw, nil
		}
		return verifiedConnection(ctx, raw, cfg)
	}
}

func verifiedConnection(ctx context.Context, raw net.Conn, cfg *tls.Config) (net.Conn, error) {
	secure := tls.Client(raw, cfg)
	if err := secure.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("TLS handshake with %s: %w", cfg.ServerName, err)
	}
	return secure, nil
}

func (t target) security() string {
	if t.tls == nil {
		return "plain TCP"
	}
	return "verified TLS"
}

// This is an options sketch, not a complete program. Root CAs and the verified
// target name must come from the same policy as the running clients.
func (t target) clientCode(c config, e Engine) string {
	code := c.code(e)
	if t.tls == nil {
		return "/* plain TCP */\n" + code
	}
	cfg, err := t.tlsForTarget()
	if err != nil {
		return "// invalid TLS target: " + err.Error()
	}
	fields := fmt.Sprintf("TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: %q", cfg.ServerName)
	if cfg.RootCAs != nil {
		fields += ", RootCAs: roots /* configured system + custom CA pool */"
	}
	fields += "}, "
	if !strings.Contains(code, "redis.NewClient") {
		code = "rdb := redis.NewClient(&redis.Options{})\n" + code
	}
	return "/* verified TLS; import crypto/tls; default dialer performs the handshake */\n" + strings.Replace(code, "&redis.Options{", "&redis.Options{"+fields, 1)
}
