// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqtt

//fusa:req REQ-RELAY-010
//fusa:req REQ-RELAY-011
//fusa:req REQ-RELAY-012
//fusa:req REQ-RELAY-013
//fusa:req REQ-RELAY-014

import relay "github.com/SoundMatt/RELAY"

// ── §9 Optional interfaces ────────────────────────────────────────────────────
//
// These are optional per RELAY spec §9. As of RELAY v0.3 (module v0.9.x) the
// canonical types are exported from github.com/SoundMatt/RELAY, so go-mqtt
// aliases them rather than redefining them (the local definitions used through
// RELAY v0.2 are gone). A value of type mqtt.Health is therefore identical to
// relay.Health, and an mqtt.HealthProvider is a relay.HealthProvider — so a
// broker that implements these satisfies the RELAY interfaces directly.
//
// Presence is declared in the capabilities document (§12.2) under
// "optional_interfaces".

// HealthStatus is the health state of a node or broker (RELAY spec §9).
//
//fusa:req REQ-RELAY-010
type HealthStatus = relay.HealthStatus

// HealthStatus values, re-exported from RELAY.
const (
	HealthOK       = relay.HealthOK       // node is operating normally
	HealthDegraded = relay.HealthDegraded // node is operating with reduced capability
	HealthDown     = relay.HealthDown     // node is not operational
)

// Health is the health report returned by HealthProvider (RELAY spec §9).
//
//fusa:req REQ-RELAY-010
type Health = relay.Health

// HealthProvider exposes node health (RELAY spec §9).
//
//fusa:req REQ-RELAY-011
type HealthProvider = relay.HealthProvider

// Metrics holds runtime counters for a node or broker (RELAY spec §9.1).
//
//fusa:req REQ-RELAY-012
type Metrics = relay.Metrics

// MetricsProvider exposes runtime counters (RELAY spec §9).
//
//fusa:req REQ-RELAY-013
type MetricsProvider = relay.MetricsProvider

// Drainer extends a node with graceful shutdown (RELAY spec §9.2).
//
//fusa:req REQ-RELAY-014
type Drainer = relay.Drainer
