# go-mqtt Roadmap

## Vision

go-mqtt is a modern, Go-native MQTT client library built for safety-critical
vehicle-signal and IoT applications.

The project focuses on:

- Clean, swappable transport interface (mock → v3 → v5)
- COVESA VISSR / W3C VISSv2 compatibility
- Safety-oriented development with go-FuSa annotations
- Broker-agnostic design (Mosquitto, HiveMQ, EMQX, AWS IoT, …)
- Zero CGo, zero mandatory external dependencies

go-mqtt is not a broker — it is a client library. Embedded broker support is
a future milestone.

---

## Guiding Principles

1. Pure Go first
2. Interface-driven — swap transport without changing application code
3. MQTT §4.7 wildcard semantics enforced in all implementations
4. Safety as a first-class concern (go-FuSa, ASIL-B / SIL 2)
5. COVESA VISSR topic conventions by default
6. Testability by default — mock broker, no network required for unit tests
7. Observability-ready — metrics and OTel hooks planned

---

## Release Plan

| Version | Theme |
|---|---|
| **v0.1** | Foundation: interfaces, mock broker, v3.1.1 client, CI, Docker quickstart ✅ |
| v0.2 | MQTT v5.0 client (`v5/`) — user properties, response topic, correlation data |
| v0.3 | TLS / mTLS transport support (`v3.WithTLS`, `v5.WithTLS`) |
| v0.4 | WebSocket transport — MQTT-over-WS for browser and VISSR compatibility |
| v0.5 | QoS 2 (ExactlyOnce) in v3 and v5 clients — v3 done ✅ |
| v0.6 | Retained message support in mock broker |
| v0.7 | Will message support (`LWT`) |
| v0.8 | COVESA VISSR bridge (`bridge/vissr/`) — map VSS signal paths to MQTT topics ✅ |
| v0.9 | Embedded broker (`broker/`) — minimal in-process MQTT broker for integration tests |
| v0.10 | OpenTelemetry adapter (`otel/`) — spans, metrics for publish/subscribe operations |
| v0.11 | go-FuSa safety case, FMEA table, SBOM, provenance |
| v1.0 | Stable API, safety certification artefacts |
| v1.1 | DDS bridge (`bridge/dds/`) — bidirectional MQTT ↔ DDS topic routing via go-DDS |
| v1.2 | SOME-IP bridge (`bridge/someip/`) — SOME-IP service ↔ MQTT topic translation |
| v1.3 | gRPC bridge (`bridge/grpc/`) — gRPC bidirectional streaming ↔ MQTT topics |
| v1.4 | REST bridge (`bridge/rest/`) — HTTP pub/sub gateway over MQTT |
| v1.5 | MQTT federation bridge (`bridge/mqtt/`) — broker-to-broker topic forwarding |

---

## Milestone Detail

### v0.2 — MQTT v5.0 client

MQTT v5.0 adds properties essential for request/response patterns used in
COVESA VISSR: `Response-Topic`, `Correlation-Data`, `Message-Expiry-Interval`,
`User-Property`. The `v5/` package will implement the full v5.0 CONNECT flow
and expose these as typed options.

### v0.4 — WebSocket transport

W3C VISSv2 defines MQTT-over-WebSocket as the primary browser transport.
`v3.DialWS` / `v5.DialWS` will accept a `ws://` or `wss://` URL.

### v0.8 — COVESA VISSR bridge

[COVESA VISSR](https://github.com/covesa/vissr) (Vehicle Information Service
Specification Reference) implements W3C VISSv2 for vehicle data access.
`bridge/vissr` will:

- Map VSS signal paths (`Vehicle.Speed`) to MQTT topic paths (`Vehicle/Speed`)
- Handle the VISSR subscription protocol over MQTT v5.0
- Support both VISSv1 and VISSv2 topic conventions
- Provide a `VISSRClient` that subscribes to any VSS signal by path

This package will make go-mqtt a first-class COVESA/VISSR transport.

### v1.1 — DDS bridge (`bridge/dds/`)

Bidirectional routing between MQTT topics and DDS topics using
[go-DDS](https://github.com/SoundMatt/go-DDS). Topic names are mapped
using a configurable translation table (e.g. `Vehicle/Speed` →
`Vehicle_Speed` DDS topic). QoS policies are translated: MQTT AtLeastOnce
→ DDS RELIABLE, AtMostOnce → DDS BEST_EFFORT.

### v1.2 — SOME-IP bridge (`bridge/someip/`)

Bridges SOME-IP service events and methods (AUTOSAR AP / Classic) to MQTT
topics. Incoming SOME-IP event notifications are published as MQTT messages;
outgoing MQTT messages trigger SOME-IP method calls or field updates. Intended
for automotive ECU environments where SOME-IP is the on-vehicle bus and MQTT
is the cloud or off-board transport.

### v1.3 — gRPC bridge (`bridge/grpc/`)

Exposes MQTT topics as a gRPC bidirectional streaming service. Clients can
subscribe to topic filters and publish messages over a single gRPC stream,
enabling gRPC-native applications to interact with an MQTT broker without
a direct broker connection. Useful for microservice architectures where gRPC
is the internal RPC layer.

### v1.4 — REST bridge (`bridge/rest/`)

HTTP gateway that maps REST endpoints to MQTT operations:

- `POST /publish/{topic}` → MQTT PUBLISH
- `GET  /subscribe/{filter}` → SSE (Server-Sent Events) stream of matching messages
- `GET  /retain/{topic}` → fetch last retained message

Suitable for web clients and tools that cannot speak MQTT directly.

### v1.5 — MQTT federation bridge (`bridge/mqtt/`)

Broker-to-broker topic forwarding. Subscribes to a local broker on
configurable filters and republishes matching messages to a remote broker
(and vice versa). Handles reconnect, QoS downgrade policies, and topic
prefix remapping. Equivalent to Mosquitto's built-in bridge feature but
implemented as a portable Go library.
