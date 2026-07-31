// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqtt_test

import (
	"errors"
	"strings"
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
		// REQ-WILD-004/006: combined single- and multi-level wildcards
		// ("+" earlier levels must still expand under a trailing "/#").
		{"a/+/#", "a/b/c", true, "WILD-004+006"},
		{"a/+/#", "a/b", true, "WILD-004+006"},
		{"a/+/#", "a/b/c/d", true, "WILD-004+006"},
		{"a/+/#", "x/b/c", false, "WILD-004+006"},
		{"sport/+/#", "sport/tennis/player1/ranking", true, "WILD-004+006"},
		{"sport/+/#", "sport", false, "WILD-004+006"},
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

// ── FitsRemainingLength boundary (MQTT §2.2.3) ────────────────────────────────

// TestFitsRemainingLength verifies that the combined topic + optional
// packet-ID + payload size is checked against the MQTT §2.2.3 4-byte
// Remaining Length maximum (268,435,455), not the payload alone. A payload
// right at mqtt.MaxPayloadSize combined with a non-empty topic must be
// rejected, since the topic and packet-ID bytes also count toward the wire
// Remaining Length that encodeVarLen has to represent.
//
//fusa:test REQ-RELAY-001
func TestFitsRemainingLength(t *testing.T) {
	// Empty topic ("" — not valid in practice, ErrTopicEmpty rejects it
	// earlier, but this isolates the arithmetic), no packet ID: payload may
	// use the entire budget.
	if !mqtt.FitsRemainingLength("", mqtt.MaxPayloadSize-2, false) {
		t.Error("FitsRemainingLength(\"\", MaxPayloadSize-2, false) = false, want true (exactly at the boundary)")
	}
	if mqtt.FitsRemainingLength("", mqtt.MaxPayloadSize-1, false) {
		t.Error("FitsRemainingLength(\"\", MaxPayloadSize-1, false) = true, want false (one byte over)")
	}

	// A payload of exactly MaxPayloadSize with ANY non-empty topic must be
	// rejected: the topic's 2-byte length prefix plus its bytes push the
	// total Remaining Length past 268,435,455 even though the payload alone
	// is within MaxPayloadSize.
	if mqtt.FitsRemainingLength("a", mqtt.MaxPayloadSize, false) {
		t.Error("FitsRemainingLength(\"a\", MaxPayloadSize, false) = true, want false (topic overhead overflows the wire limit)")
	}

	// QoS>0 adds a 2-byte packet identifier; a payload that fits at QoS 0
	// with a given topic may no longer fit at QoS 1/2.
	topic := "sensors/temp"
	overhead := 2 + len(topic)
	if !mqtt.FitsRemainingLength(topic, mqtt.MaxPayloadSize-overhead, false) {
		t.Errorf("FitsRemainingLength(%q, MaxPayloadSize-%d, false) = false, want true", topic, overhead)
	}
	if mqtt.FitsRemainingLength(topic, mqtt.MaxPayloadSize-overhead+1, false) {
		t.Errorf("FitsRemainingLength(%q, MaxPayloadSize-%d+1, false) = true, want false", topic, overhead)
	}
	if !mqtt.FitsRemainingLength(topic, mqtt.MaxPayloadSize-overhead-2, true) {
		t.Errorf("FitsRemainingLength(%q, ..., true) with packet ID = false, want true", topic)
	}
	if mqtt.FitsRemainingLength(topic, mqtt.MaxPayloadSize-overhead-1, true) {
		t.Errorf("FitsRemainingLength(%q, ..., true) with packet ID = true, want false (packet ID overhead not accounted)", topic)
	}
}

// ── CheckStringLen / CheckBinLen (§1.5.4, §1.5.6) ──────────────────────────

// TestCheckStringLen is a regression test for go-mqtt-01: a UTF-8 string
// field above the 2-byte length-prefix bound (65,535 bytes) must be rejected
// with ErrPayloadTooLarge, not silently truncated (mod 65,536) on encode.
//
//fusa:test REQ-RELAY-002
func TestCheckStringLen(t *testing.T) {
	if err := mqtt.CheckStringLen(strings.Repeat("a", mqtt.MaxStringLen)); err != nil {
		t.Errorf("CheckStringLen(%d bytes) = %v, want nil (exactly at the boundary)", mqtt.MaxStringLen, err)
	}
	err := mqtt.CheckStringLen(strings.Repeat("a", mqtt.MaxStringLen+1))
	if !errors.Is(err, mqtt.ErrPayloadTooLarge) {
		t.Errorf("CheckStringLen(%d bytes) = %v, want ErrPayloadTooLarge (one byte over)", mqtt.MaxStringLen+1, err)
	}
}

// TestCheckBinLen mirrors TestCheckStringLen for MQTT Binary Data fields
// (§1.5.6), which share the same 2-byte length-prefix bound.
//
//fusa:test REQ-RELAY-002
func TestCheckBinLen(t *testing.T) {
	if err := mqtt.CheckBinLen(make([]byte, mqtt.MaxStringLen)); err != nil {
		t.Errorf("CheckBinLen(%d bytes) = %v, want nil (exactly at the boundary)", mqtt.MaxStringLen, err)
	}
	err := mqtt.CheckBinLen(make([]byte, mqtt.MaxStringLen+1))
	if !errors.Is(err, mqtt.ErrPayloadTooLarge) {
		t.Errorf("CheckBinLen(%d bytes) = %v, want ErrPayloadTooLarge (one byte over)", mqtt.MaxStringLen+1, err)
	}
}
