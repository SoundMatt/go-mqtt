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

import "context"

// ── §9 Optional interfaces ────────────────────────────────────────────────────
//
// These are optional per RELAY spec §9. When implemented they MUST conform to
// these exact signatures. Presence is declared in the capabilities document
// (§12.2) under "optional_interfaces".
//
// Local definitions are used until github.com/SoundMatt/RELAY exports them
// (tracked in SoundMatt/RELAY#11).

// HealthStatus is the health state of a node or broker.
//
//fusa:req REQ-RELAY-010
type HealthStatus int

const (
	HealthOK       HealthStatus = 0 // node is operating normally
	HealthDegraded HealthStatus = 1 // node is operating with reduced capability
	HealthDown     HealthStatus = 2 // node is not operational
)

// Health is the health report returned by HealthProvider.
//
//fusa:req REQ-RELAY-010
type Health struct {
	Status  HealthStatus `json:"status"`
	Details string       `json:"details,omitempty"`
}

// HealthProvider exposes node health (RELAY spec §9).
// Implementations returning HealthDown MUST also return errors from operations.
//
//fusa:req REQ-RELAY-011
type HealthProvider interface {
	Health() Health
}

// Metrics holds runtime counters for a node or broker.
//
//fusa:req REQ-RELAY-012
type Metrics struct {
	WriteCount     uint64 `json:"write_count"`
	DeliverCount   uint64 `json:"deliver_count"`
	DropCount      uint64 `json:"drop_count"`
	BytesWritten   uint64 `json:"bytes_written"`
	BytesDelivered uint64 `json:"bytes_delivered"`
	ErrorCount     uint64 `json:"error_count"`
}

// MetricsProvider exposes runtime counters (RELAY spec §9).
//
//fusa:req REQ-RELAY-013
type MetricsProvider interface {
	Metrics() Metrics
}

// Drainer extends a node with graceful shutdown (RELAY spec §9).
// CloseWithDrain blocks until all in-flight messages are delivered or ctx expires,
// then closes the node. It MUST be idempotent.
//
//fusa:req REQ-RELAY-014
type Drainer interface {
	CloseWithDrain(ctx context.Context) error
}
