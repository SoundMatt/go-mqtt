# Software Quality Assurance Plan
## go-mqtt — ISO 26262 ASIL-B / IEC 61508 SIL 2 / DO-178C DAL C

**Document ID:** SQAP-001
**Version:** 1.0
**Date:** 2026-06-19
**Status:** Active
**Author:** Matt Jones (matt@jellybaby.com)
**Standards:** ISO 26262:2018 Part 8 §6, IEC 61508-3:2010 §6, DO-178C §8, §11.5

---

## 1. Purpose and scope

This Software Quality Assurance Plan (SQAP) defines the quality assurance
activities that provide confidence that go-mqtt conforms to its plans and
standards. It applies to the whole lifecycle.

## 2. Quality objectives

| Objective | Control | Gate |
|---|---|---|
| Plans are followed | CI enforces the verification gates in [SVP.md](SVP.md) | Required-green CI |
| Coding standards met | `golangci-lint v2`, `gofmt`, `go vet` | `lint` CI job |
| Requirements traced and tested | 100% bidirectional traceability | `gofusa trace -req-coverage 100` |
| No undefined/unsafe behaviour | Static + cyber analysis | `gofusa check`, `gofusa cyber` |
| No known vulnerabilities | Dependency scan | `gofusa vuln` |
| Tools fit for purpose | Tool qualification | `gofusa qualify` |
| Standards conformance | Protocol conformance | `relay conform --strict`, `relay interop --strict` |
| Releases reproducible | SBOM + provenance | `gofusa release` |

## 3. Process assurance

- Every change is reviewed via pull request before merge (squash).
- All commits carry a DCO `Signed-off-by` trailer (enforced by the DCO CI check).
- CI runs the full test matrix (3 OSes × 2 Go versions, `-race`) plus the FuSa
  lifecycle, lint, conformance, and integration jobs on every push and PR.
- A release tag is applied only to a fully green commit ([SVP.md](SVP.md) §6).

## 4. Product assurance

- Structural coverage is measured (`gofusa coverage`, `go test -cover`) and
  reviewed; safety-relevant gaps are dispositioned or closed with new tests.
- The dFMEA (`fmea.json`) is regenerated from the exported surface each release.
- The HARA (`.fusa-hara.json`) and TARA (`gofusa tara`) bound the hazard and
  threat space; the safety case (`safety-case.md`) assembles the evidence.

## 5. Non-conformance and corrective action

- Findings are recorded by go-FuSa; accepted findings require a reviewed
  disposition in `.fusa-dispositions.json` with a written rationale.
- Defects are tracked as problem reports in `.fusa-problems.json`
  (`gofusa pr add/close`) and as GitHub issues.
- A corrective change re-enters the change-control and verification process.

## 6. Records and audit

All quality records (test evidence, check/cyber/vuln/qualify reports, gap
reports against ISO 26262 / IEC 61508 / DO-178C / ISO 21434, SBOM, provenance)
are produced in CI and bundled by `gofusa audit-pack` into a single archive for
audit. The committed evidence in the repository reflects the latest release
baseline.
