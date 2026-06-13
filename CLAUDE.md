# go-mqtt — Claude session guide

Repo: `github.com/SoundMatt/go-mqtt`
Local path: `/Users/matt/Documents/Coding/SoundMatt/go-mqtt`

## Project overview

A pure-Go MQTT client library with swappable transport backends.
Designed for vehicle-signal transport and COVESA VISSR compatibility.

| Package | What it is |
|---|---|
| `.` | `mqtt` — interfaces, QoS, Message, MatchTopic, sentinel errors |
| `mock/` | In-process broker, zero deps, use for unit tests |
| `v3/` | Pure-Go MQTT v3.1.1 TCP client |
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

go-mqtt will be used as a transport by covesa/vissr. VSS signal paths use
dot notation (`Vehicle.Speed`) but MQTT topics use slash notation (`Vehicle/Speed`).
The planned `bridge/vissr/` package handles this mapping. When designing new
API, prefer slash-separated topic paths to stay idiomatic to MQTT.

## Version history

| Tag | Highlights |
|---|---|
| v0.1 | Foundation: interfaces, mock, v3 client, CI, Docker quickstart |
