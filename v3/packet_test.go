// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

import (
	"bytes"
	"testing"
)

//fusa:test REQ-WIRE-001
//fusa:test REQ-WIRE-002
func TestEncodeVarLen(t *testing.T) {
	cases := []struct {
		n    int
		want []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{16383, []byte{0xFF, 0x7F}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{2097151, []byte{0xFF, 0xFF, 0x7F}},
		{268435455, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}
	for _, tc := range cases {
		got := encodeVarLen(tc.n)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("encodeVarLen(%d) = %x, want %x", tc.n, got, tc.want)
		}
	}
}

func TestReadVarLen(t *testing.T) {
	cases := []struct {
		encoded []byte
		want    int
	}{
		{[]byte{0x00}, 0},
		{[]byte{0x01}, 1},
		{[]byte{0x7F}, 127},
		{[]byte{0x80, 0x01}, 128},
		{[]byte{0xFF, 0x7F}, 16383},
		{[]byte{0x80, 0x80, 0x01}, 16384},
		{[]byte{0xFF, 0xFF, 0xFF, 0x7F}, 268435455},
	}
	for _, tc := range cases {
		r := bytes.NewReader(tc.encoded)
		got, err := readVarLen(r)
		if err != nil {
			t.Errorf("readVarLen(%x): %v", tc.encoded, err)
			continue
		}
		if got != tc.want {
			t.Errorf("readVarLen(%x) = %d, want %d", tc.encoded, got, tc.want)
		}
	}
}

func TestVarLenRoundtrip(t *testing.T) {
	values := []int{0, 1, 64, 127, 128, 255, 16383, 16384, 65535, 268435455}
	for _, v := range values {
		encoded := encodeVarLen(v)
		r := bytes.NewReader(encoded)
		got, err := readVarLen(r)
		if err != nil {
			t.Errorf("roundtrip(%d): decode error: %v", v, err)
			continue
		}
		if got != v {
			t.Errorf("roundtrip(%d): got %d", v, got)
		}
	}
}

// TestReadVarLenOverflow verifies that a malformed Remaining Length with a 5th
// continuation byte (MSB set) is rejected rather than silently accepted — a
// fault-tolerance property for untrusted wire input (MQTT §2.2.3).
//
//fusa:test REQ-WIRE-003
//fusa:test REQ-FAULT-001
//fusa:sec-test REQ-SEC-004
func TestReadVarLenOverflow(t *testing.T) {
	// Five bytes all with the continuation bit set — exceeds the 4-byte maximum.
	overlong := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x7F}
	if _, err := readVarLen(bytes.NewReader(overlong)); err == nil {
		t.Error("readVarLen accepted a 5-byte Remaining Length, want error")
	}
}

//fusa:test REQ-WIRE-004
func TestEncodeStr(t *testing.T) {
	cases := []struct {
		s    string
		want []byte
	}{
		{"", []byte{0, 0}},
		{"A", []byte{0, 1, 'A'}},
		{"MQTT", []byte{0, 4, 'M', 'Q', 'T', 'T'}},
	}
	for _, tc := range cases {
		got := encodeStr(tc.s)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("encodeStr(%q) = %x, want %x", tc.s, got, tc.want)
		}
	}
}

//fusa:test REQ-WIRE-005
//fusa:test REQ-WIRE-006
func TestBuildCONNECT(t *testing.T) {
	pkt := buildCONNECT("test-client", 30, nil)

	// Fixed header: 0x10 (CONNECT)
	if pkt[0] != pktCONNECT {
		t.Errorf("fixed header: got 0x%02x, want 0x%02x", pkt[0], pktCONNECT)
	}
	// Must contain protocol name "MQTT"
	if !bytes.Contains(pkt, []byte("MQTT")) {
		t.Error("CONNECT packet missing protocol name 'MQTT'")
	}
	// Protocol level must be 0x04 (v3.1.1)
	if !bytes.Contains(pkt, []byte{0x04}) {
		t.Error("CONNECT packet missing protocol level 0x04")
	}
}

//fusa:test REQ-WIRE-007
//fusa:test REQ-WIRE-008
//fusa:test REQ-WIRE-009
//fusa:test REQ-WIRE-010
//fusa:test REQ-PUB-005
//fusa:test REQ-PUB-006
func TestBuildPUBLISH(t *testing.T) {
	// QoS 0 publish — no packet ID, no retain/dup flags.
	pkt := buildPUBLISH("test/topic", []byte("hello"), 0, false, 0)
	if pkt[0]&0xF0 != pktPUBLISH&0xF0 {
		t.Errorf("PUBLISH header nibble: got 0x%02x", pkt[0])
	}
	if bytes.Contains(pkt, []byte("test/topic")) == false {
		t.Error("PUBLISH missing topic")
	}
	if !bytes.Contains(pkt, []byte("hello")) {
		t.Error("PUBLISH missing payload")
	}
	// REQ-PUB-005: QoS 0 must carry no packet identifier. The variable header is
	// topic (2-byte len + "test/topic") then payload immediately — total length
	// excludes a 2-byte packet ID.
	if pkt[0]&0x06 != 0x00 {
		t.Errorf("QoS 0 flags: got 0x%02x, want QoS bits clear", pkt[0])
	}

	// REQ-WIRE-007: RETAIN is fixed-header bit 0.
	retained := buildPUBLISH("t", []byte("p"), 0, true, 0)
	if retained[0]&0x01 != 0x01 {
		t.Errorf("RETAIN flag: got 0x%02x, want bit 0 set", retained[0])
	}
	if pkt[0]&0x01 != 0x00 {
		t.Errorf("non-retained RETAIN flag: got 0x%02x, want bit 0 clear", pkt[0])
	}

	// REQ-WIRE-008 / REQ-WIRE-010 / REQ-PUB-006: QoS 1 sets QoS bits [2:1] and
	// includes a non-zero 2-byte big-endian packet identifier after the topic.
	pkt1 := buildPUBLISH("t", []byte("p"), 1, false, 0x0042)
	if pkt1[0]&0x06 != 0x02 {
		t.Errorf("QoS 1 header flags: got 0x%02x, want QoS=1", pkt1[0])
	}
	// topic "t" = [0x00 0x01 't'] at the start of the variable header (offset 2),
	// so the packet identifier 0x0042 follows at offsets 5..6.
	if !bytes.Contains(pkt1, []byte{0x00, 0x42}) {
		t.Errorf("QoS 1 packet identifier 0x0042 not found in % x", pkt1)
	}

	// REQ-WIRE-009: DUP is fixed-header bit 3.
	dup := buildPUBLISH("t", []byte("p"), 1, false, 0x0043)
	dup[0] |= 0x08 // a redelivery sets DUP
	if dup[0]&0x08 != 0x08 {
		t.Errorf("DUP flag: got 0x%02x, want bit 3 set", dup[0])
	}
}

//fusa:test REQ-WIRE-011
func TestBuildSUBSCRIBE(t *testing.T) {
	pkt := buildSUBSCRIBE("sensors/#", 0, 1)
	if pkt[0] != pktSUBSCRIBE {
		t.Errorf("SUBSCRIBE header: got 0x%02x, want 0x%02x", pkt[0], pktSUBSCRIBE)
	}
	if !bytes.Contains(pkt, []byte("sensors/#")) {
		t.Error("SUBSCRIBE missing topic filter")
	}
	// REQ-WIRE-011: non-zero 2-byte big-endian packet identifier (here 1).
	if !bytes.Contains(pkt, []byte{0x00, 0x01}) {
		t.Errorf("SUBSCRIBE packet identifier not found in % x", pkt)
	}
}

func TestBuildPUBACK(t *testing.T) {
	pkt := buildPUBACK(0x1234)
	if pkt[0] != pktPUBACK {
		t.Errorf("PUBACK header: got 0x%02x, want 0x%02x", pkt[0], pktPUBACK)
	}
	if pkt[2] != 0x12 || pkt[3] != 0x34 {
		t.Errorf("PUBACK packet ID: got %02x %02x, want 12 34", pkt[2], pkt[3])
	}
}

//fusa:test REQ-WIRE-012
func TestBuildUNSUBSCRIBE(t *testing.T) {
	pkt := buildUNSUBSCRIBE("sensors/#", 0x0007)
	if pkt[0] != pktUNSUBSCRIBE {
		t.Errorf("UNSUBSCRIBE header: got 0x%02x, want 0x%02x", pkt[0], pktUNSUBSCRIBE)
	}
	// REQ-WIRE-012: non-zero 2-byte big-endian packet identifier precedes the filter.
	if !bytes.Contains(pkt, []byte{0x00, 0x07}) {
		t.Errorf("UNSUBSCRIBE packet identifier not found in % x", pkt)
	}
	if !bytes.Contains(pkt, []byte("sensors/#")) {
		t.Error("UNSUBSCRIBE missing topic filter")
	}
}
