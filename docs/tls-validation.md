# Verified TLS validation

The TLS change was validated on November 13, 2026, against repository baseline
`f8701d65c863ca96ce0d2c4c458e4aa4a945b60c`, go-redis and its native OTel module
v9.23.0, using Go 1.27.3, darwin/arm64 and GOMAXPROCS=4.

`go test -race -count=1 -timeout=90s ./...` passed. Local transport fixtures cover:

- Verified custom CA and explicit certificate name, plus inferred real-target
  hostname, for direct and transparent proxy paths across all five engine option
  constructors and both monitor clients. Monitor labels wrap verified TLS sockets.
- Rejection of wrong CAs and hostnames through both paths.
- Stalled handshake cancellation, parent deadline and dial timeout, through both
  paths; expected context error identity and prompt peer-observed socket closure
  (including the demo dialer's intentional TCP reset) are asserted.
- Certificate-rejection cleanup using an independently counted underlying Close.
- Plain-mode option compatibility, rejection of TLS-only flags without TLS,
  invalid CA rejection, immutable per-target hostname derivation and TLS-aware
  client option sketches.

The opt-in `TestTLSSoftwareSmoke` also passed on an owned Redis Software fixture
with active/effective 4x QPF verified immediately before use and unchanged since.
It exercised certificate-verified TLS/RESP3 admin seed pipelines and PING, RTT
probes, and actual GET/HMGET through all five engine faces, directly and through
the proxy. Nil, empty and exact Int64 bytes were checked. No background workload
callers were started. Exactly two fresh `adbc:tls-road-smoke:*` keys per mode were
created; exact DEL and post-DEL EXISTS=0 assertions passed. All clients were closed.

The final complete race suite, with the opt-in Software smoke enabled, passed in
2.223s. This was a correctness smoke, not a performance measurement. Raw logs,
endpoint identifiers, credentials and fixture configuration remain private. The
repository's plain Redis Compose was not run. No Community, Valkey, ElastiCache
or Memorystore endpoint was used.
