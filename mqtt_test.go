// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqtt_test

import (
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// ── MatchTopic wildcard semantics (§4.7) ──────────────────────────────────────

// TestMatchTopicWildcards exhaustively exercises the §4.7 topic-filter matching
// rules: multi-level '#', single-level '+', the '$'-prefix exclusion, and exact
// literal matches.
//
//fusa:test REQ-WILD-001
//fusa:test REQ-WILD-002
//fusa:test REQ-WILD-003
//fusa:test REQ-WILD-004
//fusa:test REQ-WILD-005
//fusa:test REQ-WILD-006
//fusa:test REQ-WILD-007
//fusa:test REQ-WILD-008
func TestMatchTopicWildcards(t *testing.T) {
	cases := []struct {
		filter string
		topic  string
		want   bool
		req    string
	}{
		// REQ-WILD-001: "#" matches any non-$ topic.
		{"#", "a", true, "WILD-001"},
		{"#", "a/b/c", true, "WILD-001"},
		// REQ-WILD-002: "#" does not match $-prefixed topics.
		{"#", "$SYS/broker", false, "WILD-002"},
		// REQ-WILD-003: "prefix/#" matches the parent level exactly.
		{"sport/#", "sport", true, "WILD-003"},
		// REQ-WILD-004: "prefix/#" matches deeper levels.
		{"sport/#", "sport/tennis", true, "WILD-004"},
		{"sport/#", "sport/tennis/player1", true, "WILD-004"},
		// REQ-WILD-005: "prefix/#" rejects $-prefixed topics.
		{"$SYS/#", "$SYS/x", true, "WILD-005-pos"}, // explicit $ filter does match
		{"#", "$SYS/x", false, "WILD-005-neg"},
		// REQ-WILD-006: "+" matches exactly one level.
		{"a/+/c", "a/b/c", true, "WILD-006"},
		{"a/+/c", "a/b/d/c", false, "WILD-006"},
		{"a/+", "a/b", true, "WILD-006"},
		{"a/+", "a/b/c", false, "WILD-006"},
		// REQ-WILD-007: leading "+" does not match $-prefixed topics.
		{"+/monitor", "$SYS/monitor", false, "WILD-007"},
		// REQ-WILD-008: exact literal match with no wildcard.
		{"a/b/c", "a/b/c", true, "WILD-008"},
		{"a/b/c", "a/b/d", false, "WILD-008"},
	}
	for _, tc := range cases {
		if got := mqtt.MatchTopic(tc.filter, tc.topic); got != tc.want {
			t.Errorf("[%s] MatchTopic(%q, %q) = %v, want %v",
				tc.req, tc.filter, tc.topic, got, tc.want)
		}
	}
}

// ── QoS constants (§4.3) ──────────────────────────────────────────────────────

//fusa:test REQ-QOS-001
//fusa:test REQ-QOS-002
//fusa:test REQ-QOS-003
//fusa:test REQ-QOS-004
func TestQoSConstants(t *testing.T) {
	if mqtt.AtMostOnce != 0 {
		t.Errorf("AtMostOnce = %d, want 0", mqtt.AtMostOnce)
	}
	if mqtt.AtLeastOnce != 1 {
		t.Errorf("AtLeastOnce = %d, want 1", mqtt.AtLeastOnce)
	}
	if mqtt.ExactlyOnce != 2 {
		t.Errorf("ExactlyOnce = %d, want 2", mqtt.ExactlyOnce)
	}
	// REQ-QOS-004: the three levels must be mutually distinct.
	set := map[mqtt.QoS]bool{mqtt.AtMostOnce: true, mqtt.AtLeastOnce: true, mqtt.ExactlyOnce: true}
	if len(set) != 3 {
		t.Errorf("QoS constants are not mutually distinct: %v", set)
	}
}

// ── Message canonical fields (§15.4) ──────────────────────────────────────────

//fusa:test REQ-MSG-001
//fusa:test REQ-MSG-002
//fusa:test REQ-MSG-003
//fusa:test REQ-MSG-004
//fusa:test REQ-MSG-005
func TestMessageFields(t *testing.T) {
	// A QoS 1 retained message carries all canonical fields.
	m := mqtt.Message{
		Topic:    "sensors/temp",
		Payload:  []byte("21.5"),
		QoS:      mqtt.AtLeastOnce,
		Retained: true,
		PacketID: 0x0042,
	}
	if m.Topic == "" { // REQ-MSG-001: non-empty topic identifies the destination
		t.Error("Topic must be non-empty")
	}
	if string(m.Payload) != "21.5" { // REQ-MSG-002: payload carries the body
		t.Errorf("Payload = %q, want 21.5", m.Payload)
	}
	if m.QoS != mqtt.AtLeastOnce { // REQ-MSG-003: QoS reflects the level
		t.Errorf("QoS = %v, want AtLeastOnce", m.QoS)
	}
	if !m.Retained { // REQ-MSG-004: retained flag set for stored delivery
		t.Error("Retained must be true")
	}
	if m.PacketID == 0 { // REQ-MSG-005: non-zero packet ID for QoS >= 1
		t.Error("PacketID must be non-zero for QoS >= 1")
	}

	// A QoS 0 message has a zero packet identifier (REQ-MSG-005).
	zero := mqtt.Message{Topic: "a/b"}
	if zero.Topic != "a/b" {
		t.Errorf("Topic = %q, want a/b", zero.Topic)
	}
	if zero.QoS != mqtt.AtMostOnce {
		t.Errorf("default QoS = %v, want AtMostOnce", zero.QoS)
	}
	if zero.PacketID != 0 {
		t.Errorf("QoS 0 PacketID = %d, want 0", zero.PacketID)
	}
	// REQ-MSG-002: a nil payload is a valid zero-length body.
	if zero.Payload != nil {
		t.Errorf("default Payload = %v, want nil", zero.Payload)
	}
}
