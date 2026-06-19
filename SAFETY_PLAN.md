# Software Safety Plan
## go-mqtt — ISO 26262 ASIL-B / IEC 61508 SIL 2

**Document ID:** SSP-001
**Version:** 0.2
**Date:** 2026-06-17
**Status:** Active
**Author:** Matt Jones (matt@jellybaby.com)
**Standards:** ISO 26262:2018 Part 8 §7, IEC 61508-3:2010 §5

---

## 1. Purpose and scope

This Software Safety Plan (SSP) defines the lifecycle, activities, methods, and
responsibilities for the development of go-mqtt
(`github.com/SoundMatt/go-mqtt`) in accordance with:

- ISO 26262:2018 — Road vehicles — Functional Safety (Parts 3, 4, 6, 8)
- IEC 61508:2010 — Functional Safety of E/E/PE Safety-related Systems (Part 3)

go-mqtt is developed as a **Safety Element Out Of Context (SEOOC)** targeting
ASIL-B (ISO 26262) / SIL 2 (IEC 61508). The integrating system is responsible
for system-level HARA, hardware fault model (FMEDA), and allocation.

---

## 2. Safety requirements

Requirements are annotated in source code with `//fusa:req REQ-XXX-NNN`
comments and verified by `//fusa:test` / `//fusa:sec-test` annotations,
managed by [go-FuSa](https://github.com/SoundMatt/go-FuSa). The machine-readable
requirement set is in `.fusa-reqs.json` (go-FuSa v0.30.0 moved the registry out
of `.fusa.json`, which now holds only project/rule/report configuration). The
related plans are [SVP.md](SVP.md), [SCMP.md](SCMP.md), [SQAP.md](SQAP.md), and
the SEOOC [SAFETY_MANUAL.md](SAFETY_MANUAL.md).

### 2.1 Requirement families

| Family | Count | Scope |
|---|---|---|
| REQ-MSG | 5 | MQTT message structure |
| REQ-V5-MSG | 5 | MQTT v5 message properties |
| REQ-QOS | 4 | Quality of Service levels |
| REQ-WIRE | varies | Wire encoding (v3/v5) |
| REQ-CONN | varies | Connection lifecycle |
| REQ-PUB | 4 | Publish path |
| REQ-SUB | 8 | Subscribe path |
| REQ-WILD | 8 | Topic wildcard matching (§4.7) |
| REQ-SAFETY | 8 | Cross-cutting safety constraints |
| REQ-MOCK | 5 | In-process broker |
| REQ-V5-CONN | varies | v5 connection properties |
| REQ-V5-WIRE | varies | v5 wire encoding |
| REQ-V5-PUB | varies | v5 publish properties |
| REQ-V5-SUB | varies | v5 subscription options |
| REQ-V5-ALIAS | varies | v5 topic alias |
| REQ-CONC | 3 | Concurrency safety |
| REQ-LEAK | 3 | Goroutine / resource leak prevention |
| REQ-ORDER | varies | Message ordering guarantees |
| REQ-FAULT | 10 | Fault injection (session loss, packet loss, invalid input) |
| REQ-RELAY | 14 | RELAY spec conformance (§5, §9, §10.3, §14, §15) |

Total: **≥ 120 atomic SEOOC requirements** annotated in source code.

### 2.2 Key safety requirements

| ID | Requirement |
|---|---|
| REQ-PUB-001 | Publish must validate topic is non-empty |
| REQ-PUB-002 | Publish must respect the requested QoS level |
| REQ-SUB-001 | Subscribe must validate topic filter is non-empty |
| REQ-SUB-002 | Subscribe must support MQTT §4.7 wildcard filters |
| REQ-SUB-003 | Subscription channel must not block the publishing goroutine |
| REQ-WILD-001..008 | MatchTopic must implement MQTT §4.7 including `$` system topics |
| REQ-SAFETY-001 | Publish must reject empty topic before network access |
| REQ-CONC-001 | Client must be safe for concurrent use from multiple goroutines |
| REQ-FAULT-004 | Publish after Close() must return ErrClosed |
| REQ-FAULT-009 | Full subscription channel must drop silently, not block |
| REQ-FAULT-010 | Concurrent Close() and Publish() must not panic |
| REQ-RELAY-001 | SpecVersion must equal "0.2" |
| REQ-RELAY-002..003 | Error sentinels must wrap relay.Err* sentinels via %w |

---

## 3. Verification

### 3.1 Test strategy

| Method | Tool | CI Gate |
|---|---|---|
| Unit tests (race detector) | `go test -race -count=1 ./...` | Required; blocks merge |
| Fuzz targets | `go test -fuzz=FuzzMatchTopic -fuzztime=500000x` | Required; blocks merge |
| Integration tests | Mosquitto (Docker) | Required; blocks merge |
| Static analysis | `golangci-lint`, `go vet` | Required; blocks merge |
| Safety analysis | `gofusa check` | Required; blocks merge |
| Benchmark smoke | `go test -bench=.` | Advisory |

### 3.2 Fault injection tests

`mock/fault_test.go` covers the following FMEA failure modes:

| Test | Failure Mode | Requirement |
|---|---|---|
| TestFaultPublishAfterClose | Session loss — publish path | REQ-FAULT-004 |
| TestFaultSubscribeAfterClose | Session loss — subscribe path | REQ-FAULT-005 |
| TestFaultIdempotentClose | Double-close | REQ-FAULT-006 |
| TestFaultEmptyTopicPublish | Invalid input — empty topic | REQ-FAULT-007 |
| TestFaultEmptyTopicSubscribe | Invalid input — empty filter | REQ-FAULT-007 |
| TestFaultContextCancelled | Packet loss / context timeout | REQ-FAULT-008 |
| TestFaultSubscriptionChannelDrop | Packet loss — back-pressure | REQ-FAULT-009 |
| TestFaultConcurrentClosePublish | Session loss under concurrent access | REQ-FAULT-010 |

Wire-protocol fault coverage (v3/v5 client):

| Test | Failure Mode | Requirement |
|---|---|---|
| FuzzReadPropSet | Malformed property set | REQ-FAULT-001 |
| FuzzBuildPublish | Malformed PUBLISH construction | REQ-FAULT-001 |
| Packet decoding tests | Unknown packet types | REQ-FAULT-002 |

---

## 4. Release artifacts

Generated by `gofusa release` on each tagged release:

| Artifact | Description |
|---|---|
| `check-report.json` | Machine-readable safety check report (0 errors required) |
| `fmea.csv` / `fmea.json` | Differential Failure Mode and Effects Analysis |
| `safety-case.md` / `safety-case.json` | Safety argument (GSN) |
| `sbom.json` | Software Bill of Materials (SPDX 2.3) |
| `provenance.json` | Build provenance |
| `artifact-manifest.json` | Release manifest with checksums |

All artifacts are committed to the repository at each tagged release and
verified by the CI `go-FuSa safety check` gate.

---

## 5. Security controls

| Control | Requirement | Threat mitigated |
|---|---|---|
| TLS transport (`v3.WithTLS`, `v3.DialTLS`) | REQ-TLS-001 | Eavesdropping / tampering on the broker link |
| Mutual TLS (client certificates) | REQ-TLS-002 | Unauthorised client/broker (man-in-the-middle, impersonation) |
| TLS 1.2 minimum (DialTLS default) | REQ-TLS-003 | Downgrade to weak protocol versions |

TLS is a mitigating control for transport-level threats in the integrating
system's threat model. The integrator remains responsible for certificate
provisioning, rotation, and trust-anchor management.

---

## 6. RELAY conformance

go-mqtt is RELAY-conformant at spec v0.2 (see `mqtt.SpecVersion`).
Conformance requirements are tracked in the REQ-RELAY family (14 requirements).
See `optional.go` for HealthProvider, MetricsProvider, and Drainer implementations.
