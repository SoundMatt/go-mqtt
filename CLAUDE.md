# go-mqtt — Claude session guide

Repo: `github.com/SoundMatt/go-mqtt`
Local path: repository root (clone location is environment-specific).

## Project overview

A pure-Go MQTT client library with swappable transport backends.
Designed for vehicle-signal transport and COVESA VISSR compatibility.

| Package | What it is |
|---|---|
| `.` | `mqtt` — interfaces, QoS, Message, MatchTopic, sentinel errors |
| `mock/` | In-process broker, zero deps, use for unit tests |
| `v3/` | Pure-Go MQTT v3.1.1 TCP client (TLS, WebSocket) |
| `v5/` | Pure-Go MQTT v5.0 TCP client (properties, topic aliases) |
| `broker/` | Minimal in-process MQTT v3.1.1 broker (edge/test harnesses) |
| `bridge/rest/` | HTTP gateway — REST/SSE pub/sub over MQTT |
| `bridge/mqtt/` | Broker-to-broker federation bridge |
| `bridge/vissr/` | COVESA VISSR bridge — VSS dot-paths ↔ MQTT topics |
| `cmd/go-mqtt/` | RELAY-conformant CLI (`version`, `capabilities`, `status`, `convert`, `send`, `subscribe`) |
| `examples/quickstart/` | Docker quickstart pub/sub binaries |

## Per-PR checklist

1. `git checkout main && git pull origin main`
2. `git checkout -b fix/<area>-<short>` or `feat/<area>-<short>`
3. Implement + tests.
4. `go build ./...`
5. `go vet ./...`
6. `go test -race -count=1 ./...`
7. Commit with DCO sign-off (see style below).
8. `git push origin <branch>`, open PR targeting `main`.
9. Wait for all CI checks green, then merge (squash).
10. Tag patch/minor releases after merge.

## Commit message style

```
type(scope): short summary

Body explaining *why*, not what. Reference relevant ROADMAP.md items.

Signed-off-by: Matt Jones <matt@jellybaby.com>
```

Use `git commit -F - <<'COMMIT' ... COMMIT` (heredoc) to avoid zsh
history expansion on `%`, `!`, and `(`.

## Go conventions

- Sentinel errors in `mqtt.go` — wrap with `fmt.Errorf("...: %w", mqtt.ErrClosed)`.
- `MatchTopic` is the canonical §4.7 implementation — do not duplicate it.
- `mock` is the default test backend; use `v3` tests only for wire-protocol behaviour.
- All public API must have tests; `go test -race` must pass.
- No `sync.Mutex` wrapping `sync.Map` — they're self-synchronising.
- `go vet` and `golangci-lint` must pass before pushing.

## COVESA/VISSR context

go-mqtt is used as a transport by covesa/vissr. VSS signal paths use
dot notation (`Vehicle.Speed`) but MQTT topics use slash notation (`Vehicle/Speed`).
The `bridge/vissr/` package handles this mapping. When designing new
API, prefer slash-separated topic paths to stay idiomatic to MQTT.

## Version history

| Tag | Highlights |
|---|---|
| v0.1 | Foundation: interfaces, mock, v3 client, CI, Docker quickstart |
| v0.2 | MQTT v5.0 client (`v5/`) — user properties, response topic, correlation data |
| v0.3 | TLS / mTLS transport (v3) |
| v0.4 | WebSocket transport (v3) |
| v0.5 | QoS 2 (ExactlyOnce) in v3 client |
| v0.8 | COVESA VISSR bridge (`bridge/vissr/`) |
| v0.9 | Embedded broker (`broker/`) |
| v0.10 | Observability — RELAY §9 `MetricsProvider` (mock + broker) |
| v1.4 | REST bridge (`bridge/rest/`) |
| v1.5 | MQTT federation bridge (`bridge/mqtt/`) |
| v1.6 | Interop testing — Mosquitto round-trip + `mosquitto_pub`/`sub` cross-checks |

> Keep this table in sync with `ROADMAP.md`; the authoritative shipped-milestone
> list lives there.
