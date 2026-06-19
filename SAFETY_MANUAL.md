# Safety Manual (SEOOC Assumptions of Use)
## go-mqtt — ISO 26262 ASIL-B / IEC 61508 SIL 2

**Document ID:** SM-001
**Version:** 1.0
**Date:** 2026-06-19
**Status:** Active
**Author:** Matt Jones (matt@jellybaby.com)
**Standards:** ISO 26262:2018 Part 10 §9 (SEooC), Part 8 §6; IEC 61508-3:2010 Annex D

---

## 1. Purpose

go-mqtt is developed as a **Safety Element Out Of Context (SEOOC)**: a reusable
software element built against *assumed* requirements rather than a specific
item definition. This Safety Manual states the **assumptions of use (AoU)** and
**integration constraints** that an integrator MUST satisfy for go-mqtt to be
used in a safety-related system at ASIL-B / SIL 2. If an assumption does not hold
in the target system, the integrator MUST re-evaluate the affected safety
requirements.

## 2. Element description

go-mqtt is a pure-Go MQTT v3.1.1 / v5.0 client and in-process broker with
swappable transports (TCP, TLS/mTLS, WebSocket) and RELAY conformance. It
provides message publish/subscribe with QoS 0/1/2, topic-filter matching
(§4.7), retained messages, and last-will. It performs **no hardware access** and
holds **no global mutable state** beyond per-connection objects.

## 3. Assumed safety requirements

go-mqtt is verified against the 189 requirements in `.fusa-reqs.json`. The
integrator MUST confirm these assumed requirements are consistent with the
allocated system-level requirements, in particular:

- **Transport, not safety function.** go-mqtt provides a *communication
  transport*. End-to-end safety of the transmitted data (freshness,
  authenticity, integrity beyond TCP/TLS, sequence counting) is the
  responsibility of a higher layer (e.g. an E2E/AUTOSAR-style protection profile
  or the RELAY `safety` concern). go-mqtt does not implement E2E protection.
- **QoS semantics.** QoS 1 is at-least-once (duplicates possible); QoS 2 is
  exactly-once for the v3 client. The integrator MUST select a QoS appropriate
  to the data's safety requirement and tolerate duplicates at QoS 1.

## 4. Assumptions of use (the integrator MUST ensure)

1. **Trusted toolchain.** Built with Go 1.25+ and the pinned go-FuSa `v0.30.0`
   and RELAY `v1.10.0`; no `InsecureSkipVerify` is introduced downstream.
2. **TLS for untrusted networks.** When the broker link crosses a trust
   boundary, TLS (≥1.2) is used; certificate verification is **not** disabled,
   and client authentication (mTLS) is configured where the threat model
   requires it. See REQ-SEC-001/002/003.
3. **Bounded inputs.** The integrator configures payload-size limits
   (e.g. REST `WithMaxBody`) and subscription channel depths appropriate to the
   platform's memory budget. go-mqtt rejects oversized REST bodies and applies
   non-blocking delivery (REQ-SEC-006, REQ-SAFETY-008).
4. **Back-pressure policy.** The integrator selects a `BackPressurePolicy`
   (DropNewest/DropOldest/Block) consistent with the safety requirement; the
   default drops on a full channel and never blocks delivery.
5. **No reconnect assumption.** go-mqtt does **not** auto-reconnect. The
   integrator detects connection loss (subscription channels close —
   REQ-SAFETY-006) and re-establishes the session if required.
6. **Single broker trust.** The federation bridge does not validate cross-broker
   identity beyond the configured transports; the integrator establishes broker
   trust out of band.
7. **Clean sessions.** The broker provides clean sessions only (no persistence);
   the integrator MUST not rely on broker-side session or message persistence.
8. **Concurrency contract.** All exported `Client` methods are safe for
   concurrent use (REQ-CONC-001..003). The integrator MUST still not use a
   `Subscription` channel after `Close`.

## 5. Faults handled by go-mqtt

The following fault modes are detected/contained by go-mqtt and verified by the
REQ-FAULT, REQ-SAFETY, and REQ-SEC families (see `fmea.json`):

- Operations after close return `ErrClosed`; empty topics return `ErrTopicEmpty`.
- Malformed remaining-length / unknown packet types are rejected/ignored without
  crashing (REQ-SEC-004/005).
- A full subscription channel drops rather than blocks (REQ-SAFETY-008).
- Connection loss closes all subscription channels (REQ-SAFETY-006).
- Malformed payloads are dropped without stalling the stream (REQ-SEC-008).
- Unknown MQTT v5 topic aliases are dropped, not injected (REQ-SEC-009).

## 6. Faults the integrator MUST handle

- **Communication failures** above the transport (lost/duplicated/late
  messages) — apply an E2E protection profile.
- **Resource exhaustion** from unbounded subscription rates — apply rate limits.
- **Time/clock** — go-mqtt does not provide message freshness; the integrator
  supplies timestamps/sequence numbers where freshness is safety-relevant.
- **Availability** — go-mqtt does not provide redundancy or failover.

## 7. Verification evidence

Conformance to the assumed requirements is evidenced by the artefacts in this
repository: the dFMEA (`fmea.json`), HARA (`.fusa-hara.json`), TARA
(`gofusa tara`), safety case (`safety-case.md`), traceability matrix
(`gofusa trace`), and the ISO 26262 / IEC 61508 / DO-178C / ISO 21434 gap
reports. The integrator SHOULD include this evidence in the item-level safety
case and confirm each assumption of use in §4 against the target item.
