// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

//fusa:req REQ-CONN-011

import (
	"bytes"
	"encoding/binary"
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// parseConnect parses a CONNECT packet body (after fixed header + remaining length).
// Returns connectFlags, keepalive, clientID, willTopic, willPayload.
func parseConnect(t *testing.T, body []byte) (connectFlags byte, keepalive uint16, clientID, willTopic string, willPayload []byte) {
	t.Helper()
	// Skip protocol name (6 bytes: 0x00 0x04 M Q T T) + protocol level (1) = 7
	if len(body) < 10 {
		t.Fatalf("CONNECT body too short: %d bytes", len(body))
	}
	connectFlags = body[7]
	keepalive = binary.BigEndian.Uint16(body[8:10])
	off := 10
	// Client ID
	clen := int(binary.BigEndian.Uint16(body[off : off+2]))
	off += 2
	clientID = string(body[off : off+clen])
	off += clen
	// Will topic (only if will flag set)
	if connectFlags&0x04 != 0 {
		tlen := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		willTopic = string(body[off : off+tlen])
		off += tlen
		// Will message
		plen := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		willPayload = body[off : off+plen]
	}
	return
}

// stripFixedHeader strips the fixed header byte + remaining-length varint.
func stripFixedHeader(t *testing.T, pkt []byte) []byte {
	t.Helper()
	if len(pkt) < 2 {
		t.Fatalf("packet too short")
	}
	off := 1
	for pkt[off]&0x80 != 0 {
		off++
	}
	return pkt[off+1:]
}

// TestBuildConnectNoWill verifies that buildCONNECT without a will sets
// CleanSession and no will flag.
//
//fusa:req REQ-CONN-011
func TestBuildConnectNoWill(t *testing.T) {
	pkt := buildCONNECT("test-id", 30, nil)
	body := stripFixedHeader(t, pkt)
	flags, keepalive, clientID, willTopic, _ := parseConnect(t, body)

	if flags != 0x02 {
		t.Errorf("connectFlags = 0x%02x, want 0x02 (CleanSession only)", flags)
	}
	if keepalive != 30 {
		t.Errorf("keepalive = %d, want 30", keepalive)
	}
	if clientID != "test-id" {
		t.Errorf("clientID = %q, want %q", clientID, "test-id")
	}
	if willTopic != "" {
		t.Errorf("willTopic = %q, want empty (no will)", willTopic)
	}
}

// TestBuildConnectWithWillQoS0 verifies that a QoS-0 non-retained will is
// encoded with the will flag set and QoS bits = 0.
//
//fusa:req REQ-CONN-011
func TestBuildConnectWithWillQoS0(t *testing.T) {
	w := &will{topic: "status/dead", payload: []byte("offline"), qos: mqtt.AtMostOnce, retain: false}
	pkt := buildCONNECT("sensor-1", 60, w)
	body := stripFixedHeader(t, pkt)
	flags, _, _, willTopic, willPayload := parseConnect(t, body)

	if flags&0x04 == 0 {
		t.Errorf("will flag (bit 2) not set in connectFlags 0x%02x", flags)
	}
	if flags&0x18 != 0 {
		t.Errorf("will QoS bits = 0x%02x, want 0 for AtMostOnce", (flags>>3)&0x03)
	}
	if flags&0x20 != 0 {
		t.Errorf("will retain flag set, want unset")
	}
	if willTopic != "status/dead" {
		t.Errorf("willTopic = %q, want %q", willTopic, "status/dead")
	}
	if !bytes.Equal(willPayload, []byte("offline")) {
		t.Errorf("willPayload = %q, want %q", willPayload, "offline")
	}
}

// TestBuildConnectWithWillQoS1Retain verifies QoS-1 retained will encoding.
//
//fusa:req REQ-CONN-011
func TestBuildConnectWithWillQoS1Retain(t *testing.T) {
	w := &will{topic: "lwt/sensor", payload: []byte("dead"), qos: mqtt.AtLeastOnce, retain: true}
	pkt := buildCONNECT("sensor-2", 30, w)
	body := stripFixedHeader(t, pkt)
	flags, _, _, willTopic, willPayload := parseConnect(t, body)

	if flags&0x04 == 0 {
		t.Errorf("will flag not set")
	}
	if (flags>>3)&0x03 != 1 {
		t.Errorf("will QoS = %d, want 1", (flags>>3)&0x03)
	}
	if flags&0x20 == 0 {
		t.Errorf("will retain flag not set")
	}
	if willTopic != "lwt/sensor" {
		t.Errorf("willTopic = %q, want %q", willTopic, "lwt/sensor")
	}
	if !bytes.Equal(willPayload, []byte("dead")) {
		t.Errorf("willPayload = %q, want %q", willPayload, "dead")
	}
}

// TestBuildConnectWithWillQoS2 verifies QoS-2 will encoding.
//
//fusa:req REQ-CONN-011
func TestBuildConnectWithWillQoS2(t *testing.T) {
	w := &will{topic: "lwt/ctrl", payload: []byte("gone"), qos: mqtt.ExactlyOnce, retain: false}
	pkt := buildCONNECT("ctrl-1", 30, w)
	body := stripFixedHeader(t, pkt)
	flags, _, _, _, _ := parseConnect(t, body)

	if (flags>>3)&0x03 != 2 {
		t.Errorf("will QoS = %d, want 2", (flags>>3)&0x03)
	}
}

// TestWithWillOption verifies that the WithWill option correctly populates
// the will struct in options.
//
//fusa:req REQ-CONN-011
func TestWithWillOption(t *testing.T) {
	o := &options{}
	WithWill("topic", []byte("msg"), mqtt.AtLeastOnce, true)(o)

	if o.will == nil {
		t.Fatal("will is nil after WithWill")
	}
	if o.will.topic != "topic" {
		t.Errorf("will.topic = %q, want %q", o.will.topic, "topic")
	}
	if !bytes.Equal(o.will.payload, []byte("msg")) {
		t.Errorf("will.payload = %q, want %q", o.will.payload, "msg")
	}
	if o.will.qos != mqtt.AtLeastOnce {
		t.Errorf("will.qos = %v, want AtLeastOnce", o.will.qos)
	}
	if !o.will.retain {
		t.Error("will.retain = false, want true")
	}
}
