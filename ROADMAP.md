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
7. Observability-ready — runtime metrics via the RELAY §9 `MetricsProvider`
   interface (implemented by the mock and embedded brokers); a standalone
   OpenTelemetry adapter is intentionally out of scope to keep the core
   dependency-free (downstream code can bridge `MetricsProvider` to OTel)

---

## Release Plan

| Version | Theme |
|---|---|
| **v0.1** | Foundation: interfaces, mock broker, v3.1.1 client, CI, Docker quickstart ✅ |
| v0.2 | MQTT v5.0 client (`v5/`) — user properties, response topic, correlation data |
| v0.3 | TLS / mTLS transport support (`v3.WithTLS`, `v3.DialTLS`) — v3 done ✅ |
| v0.4 | WebSocket transport — MQTT-over-WS for browser and VISSR compatibility — v3 done ✅ |
| v0.5 | QoS 2 (ExactlyOnce) in v3 and v5 clients — v3 done ✅ |
| v0.6 | Retained message support in mock broker |
| v0.7 | Will message support (`LWT`) |
| v0.8 | COVESA VISSR bridge (`bridge/vissr/`) — map VSS signal paths to MQTT topics ✅ |
| v0.9 | Embedded broker (`broker/`) — minimal in-process MQTT broker for integration tests ✅ |
| v0.10 | Observability — metrics via RELAY §9 `MetricsProvider` (mock + broker) ✅ |
| v0.11 | go-FuSa safety case, FMEA table, SBOM, provenance |
| v1.0 | Stable API, safety certification artefacts |
| v1.4 | REST bridge (`bridge/rest/`) — HTTP pub/sub gateway over MQTT ✅ |
| v1.5 | MQTT federation bridge (`bridge/mqtt/`) — broker-to-broker topic forwarding ✅ |

> **Cross-protocol bridges (DDS, SOME-IP, gRPC) are not on the roadmap.** With
> [RELAY](https://github.com/SoundMatt/RELAY), every protocol implementation
> exposes `Adapt() → relay.Node`, so MQTT↔DDS / MQTT↔SOME-IP routing is done
> generically at the relay layer (forwarding `relay.Message` between two
> adapted nodes) rather than as protocol-specific packages inside go-mqtt.
> This keeps go-mqtt free of cross-protocol dependencies (go-DDS, go-SOMEIP,
> gRPC). The remaining `bridge/*` packages (VISSR, REST, federation) are MQTT-
> or HTTP-native and are not superseded by RELAY.

---

## Milestone Detail

### v0.2 — MQTT v5.0 client

MQTT v5.0 adds properties essential for request/response patterns used in
COVESA VISSR: `Response-Topic`, `Correlation-Data`, `Message-Expiry-Interval`,
`User-Property`. The `v5/` package will implement the full v5.0 CONNECT flow
and expose these as typed options.

### v0.4 — WebSocket transport

W3C VISSv2 defines MQTT-over-WebSocket as the primary browser transport.
`v3.DialWS` accepts a `ws://` or `wss://` URL and carries MQTT control packets
in WebSocket binary frames using the `mqtt` subprotocol (RFC 6455, pure stdlib).
The v5 equivalent is future work.

### v0.8 — COVESA VISSR bridge

[COVESA VISSR](https://github.com/covesa/vissr) (Vehicle Information Service
Specification Reference) implements W3C VISSv2 for vehicle data access.
`bridge/vissr` will:

- Map VSS signal paths (`Vehicle.Speed`) to MQTT topic paths (`Vehicle/Speed`)
- Handle the VISSR subscription protocol over MQTT v5.0
- Support both VISSv1 and VISSv2 topic conventions
- Provide a `VISSRClient` that subscribes to any VSS signal by path

This package will make go-mqtt a first-class COVESA/VISSR transport.

### Cross-protocol integration via RELAY (not a go-mqtt concern)

MQTT↔DDS, MQTT↔SOME-IP, and MQTT↔gRPC routing are handled at the RELAY layer,
not by packages inside go-mqtt. go-mqtt provides `Adapt(Client) → relay.Node`;
go-DDS and go-SOMEIP provide their own adapters. A generic relay-level bridge
forwards `relay.Message` between any two adapted nodes, so no go-mqtt package
needs to import another protocol's library. This keeps go-mqtt dependency-free
and avoids duplicating per-protocol topic-mapping logic in every implementation.

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
