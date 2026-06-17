# go-mqtt

A pure-Go MQTT client library — safety-oriented, broker-agnostic, and ready for vehicle-signal transport with [COVESA VISSR](https://github.com/covesa/vissr).

[![CI](https://github.com/SoundMatt/go-mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/SoundMatt/go-mqtt/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SoundMatt/go-mqtt.svg)](https://pkg.go.dev/github.com/SoundMatt/go-mqtt)

## Packages

| Package | Description | Requires |
|---|---|---|
| `.` | `mqtt` — core interfaces, QoS, Message, MatchTopic | Nothing |
| `mock` | In-process broker. Zero dependencies. Default for testing. | Nothing |
| `v3` | Pure-Go MQTT v3.1.1 TCP client. Connects to any broker. | Nothing |

## Install

```bash
go get github.com/SoundMatt/go-mqtt
```

## Quick start

```go
import (
    mqtt "github.com/SoundMatt/go-mqtt"
    "github.com/SoundMatt/go-mqtt/mock"
)

broker := mock.New()
client := broker.Dial()
defer client.Close()

sub, _ := client.Subscribe("Vehicle/#", mqtt.AtMostOnce)
client.Publish(ctx, "Vehicle/Speed", mqtt.AtMostOnce, []byte(`{"speed":60}`))

msg := <-sub.C()
fmt.Println(string(msg.Payload)) // {"speed":60}
```

## Switching implementations

Application code only ever imports the root `mqtt` package for types. Swap the transport at the call site:

```go
// Development / tests — no network needed:
import "github.com/SoundMatt/go-mqtt/mock"
client := mock.New().Dial()

// Production — connects to Mosquitto, HiveMQ, EMQX, etc.:
import "github.com/SoundMatt/go-mqtt/v3"
client, err := v3.Dial("broker:1883")
```

### TLS / mTLS

The v3 client speaks MQTTS (conventionally port 8883). Use `DialTLS` for a
sensible default config, or `WithTLS` for full control including client
certificates (mutual TLS):

```go
// Server-authenticated TLS (system roots, ServerName from the address):
client, err := v3.DialTLS("broker:8883")

// Mutual TLS with an explicit config:
cfg := &tls.Config{
    RootCAs:      caPool,
    Certificates: []tls.Certificate{clientCert},
    MinVersion:   tls.VersionTLS12,
}
client, err := v3.Dial("broker:8883", v3.WithTLS(cfg))
```

### WebSocket (MQTT-over-WS)

For browsers and W3C VISSv2, the v3 client speaks MQTT over WebSocket (RFC 6455,
`mqtt` subprotocol) — pure stdlib, no external dependency:

```go
client, err := v3.DialWS("ws://broker:9001/")        // plaintext
client, err := v3.DialWS("wss://broker:9001/mqtt")   // TLS; honours WithTLS
```

## MQTT wildcard subscriptions

Both `mock` and `v3` implement MQTT §4.7 topic matching:

```go
// '+' — single topic level
sub, _ := client.Subscribe("sensors/+/temperature", mqtt.AtMostOnce)
// matches: sensors/room1/temperature, sensors/lab/temperature
// not:     sensors/a/b/temperature

// '#' — zero or more levels (must be last)
sub, _ := client.Subscribe("Vehicle/#", mqtt.AtMostOnce)
// matches: Vehicle/Speed, Vehicle/Cabin/HVAC/Temperature, ...
```

## QoS

```go
// Fire-and-forget — lowest overhead (default)
client.Publish(ctx, "sensors/temperature", mqtt.AtMostOnce, payload)

// Acknowledged — at least once delivery
client.Publish(ctx, "actuators/brake", mqtt.AtLeastOnce, payload)
```

## Docker quickstart

```bash
docker compose -f docker/docker-compose.yml up --build
```

Spins up an Eclipse Mosquitto broker, a publisher sending `Vehicle/Speed` readings every second, and a subscriber logging them. No configuration needed.

## COVESA VISSR

go-mqtt uses VSS-style topic paths by default (`Vehicle/Speed`, `Vehicle/Cabin/HVAC/Temperature`). The roadmap includes a `bridge/vissr` package that maps COVESA VISSR WebSocket/MQTT signals to go-mqtt subscriptions. See `ROADMAP.md`.

## Safety

go-mqtt is developed as a Safety Element Out Of Context (SEOOC) targeting ASIL-B / SIL 2. Requirements are traced with [go-FuSa](https://github.com/SoundMatt/go-FuSa) annotations (`//fusa:req`). See `SAFETY_PLAN.md`.

## License

Mozilla Public License v2.0. See [LICENSE](LICENSE).
