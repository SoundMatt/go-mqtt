# Contributing to go-mqtt

Thank you for your interest in contributing.

## Developer Certificate of Origin (DCO)

All contributions must be signed off under the
[Developer Certificate of Origin v1.1](https://developercertificate.org).

Add a `Signed-off-by` trailer to every commit:

```
git commit -s -m "feat: add awesome thing"
```

This produces:

```
feat: add awesome thing

Signed-off-by: Your Name <your@email.com>
```

A GitHub Actions check (`DCO`) verifies every commit in a PR. PRs without
sign-offs will not be merged.

## Copyright

By contributing you agree that your contributions are licensed under the
[Mozilla Public License v2.0](LICENSE) and that copyright in go-mqtt remains
with Matt Jones.

## Coding style

- `gofmt` — run `gofmt -w ./...` before pushing.
- `go vet` — must pass with zero warnings.
- `golangci-lint run` — must pass (config in `.golangci.yml`).
- Tests — new public API must be accompanied by tests.
  Run `go test -race -count=1 ./...` locally.

## Pull requests

1. Fork the repo, create a branch from `main`.
2. Make your changes with signed-off commits.
3. `go test -race -count=1 ./...` must pass.
4. Open a PR targeting `main`.
5. Wait for CI green before requesting review.

## Project structure

| Directory | What it contains |
|---|---|
| `.` | `mqtt` package — interfaces, QoS, Message, MatchTopic |
| `mock/` | In-process broker for testing |
| `v3/` | MQTT v3.1.1 pure-Go TCP client |
| `examples/quickstart/` | Docker quickstart publisher and subscriber |
| `docker/` | Dockerfile and docker-compose.yml |
| `.github/workflows/` | CI, DCO, Docker publish, release workflows |

## Commit message style

```
type(scope): short summary

Body explaining *why*, not what. Reference relevant ROADMAP.md milestones.

Signed-off-by: Matt Jones <matt@jellybaby.com>
```

Types: `feat`, `fix`, `test`, `docs`, `chore`, `refactor`, `perf`.

Use `git commit -F - <<'COMMIT' ... COMMIT` (heredoc) to avoid shell
history expansion on `%`, `!`, and `(`.
