// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v5

import (
	"bytes"
	"encoding/binary"
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// ── encodeVarLen / decodeVarLen ────────────────────────────────────────────

func TestEncodeVarLen(t *testing.T) {
	cases := []struct {
		n    int
		want []byte
	}{
		{0, []byte{0}},
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
			t.Errorf("encodeVarLen(%d) = %v, want %v", tc.n, got, tc.want)
		}
	}
}

func TestDecodeVarLen(t *testing.T) {
	cases := []struct {
		data []byte
		val  int
		n    int
	}{
		{[]byte{0}, 0, 1},
		{[]byte{0x7F}, 127, 1},
		{[]byte{0x80, 0x01}, 128, 2},
		{[]byte{0xFF, 0x7F}, 16383, 2},
		{[]byte{0x80, 0x80, 0x01}, 16384, 3},
	}
	for _, tc := range cases {
		val, n, err := decodeVarLen(tc.data)
		if err != nil {
			t.Errorf("decodeVarLen(%v) error: %v", tc.data, err)
			continue
		}
		if val != tc.val || n != tc.n {
			t.Errorf("decodeVarLen(%v) = (%d, %d), want (%d, %d)", tc.data, val, n, tc.val, tc.n)
		}
	}
}

func TestVarLenRoundtrip(t *testing.T) {
	values := []int{0, 1, 127, 128, 255, 16383, 16384, 268435455}
	for _, v := range values {
		encoded := encodeVarLen(v)
		decoded, _, err := decodeVarLen(encoded)
		if err != nil {
			t.Errorf("roundtrip(%d): decode error: %v", v, err)
			continue
		}
		if decoded != v {
			t.Errorf("roundtrip(%d): got %d", v, decoded)
		}
	}
}

func TestDecodeVarLenError(t *testing.T) {
	_, _, err := decodeVarLen([]byte{0x80, 0x80, 0x80, 0x80, 0x01}) // 5 bytes — too long
	if err == nil {
		t.Error("expected error for overlong varint, got nil")
	}
}

// ── String / binary encoding ───────────────────────────────────────────────

func TestEncodeStr(t *testing.T) {
	b := encodeStr("MQTT")
	if !bytes.Equal(b[:2], []byte{0, 4}) {
		t.Errorf("expected length prefix {0,4}, got %v", b[:2])
	}
	if string(b[2:]) != "MQTT" {
		t.Errorf("expected 'MQTT', got %q", string(b[2:]))
	}
}

func TestEncodeStrEmpty(t *testing.T) {
	b := encodeStr("")
	if !bytes.Equal(b, []byte{0, 0}) {
		t.Errorf("empty string: want {0,0}, got %v", b)
	}
}

func TestEncodeBin(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	b := encodeBin(data)
	if !bytes.Equal(b[:2], []byte{0, 4}) {
		t.Errorf("expected length prefix {0,4}, got %v", b[:2])
	}
	if !bytes.Equal(b[2:], data) {
		t.Errorf("encoded binary mismatch")
	}
}

// ── Property encoding ──────────────────────────────────────────────────────

func TestEncodePropsEmpty(t *testing.T) {
	b := encodeProps()
	if !bytes.Equal(b, []byte{0}) {
		t.Errorf("empty props: want {0}, got %v", b)
	}
}

func TestPropU16(t *testing.T) {
	b := propU16(propReceiveMax, 1000)
	if b[0] != propReceiveMax {
		t.Errorf("wrong property ID: got 0x%02x", b[0])
	}
	v := binary.BigEndian.Uint16(b[1:])
	if v != 1000 {
		t.Errorf("wrong value: got %d", v)
	}
}

func TestPropU32(t *testing.T) {
	b := propU32(propSessionExpiry, 300)
	if b[0] != propSessionExpiry {
		t.Errorf("wrong property ID")
	}
	v := binary.BigEndian.Uint32(b[1:])
	if v != 300 {
		t.Errorf("wrong value: got %d", v)
	}
}

func TestPropStr(t *testing.T) {
	b := propStr(propResponseTopic, "reply/topic")
	if b[0] != propResponseTopic {
		t.Errorf("wrong property ID")
	}
	s, _, err := readStr(b[1:])
	if err != nil || s != "reply/topic" {
		t.Errorf("readStr after propStr: got %q, err %v", s, err)
	}
}

func TestPropBin(t *testing.T) {
	data := []byte("corr-id")
	b := propBin(propCorrelationData, data)
	if b[0] != propCorrelationData {
		t.Errorf("wrong property ID")
	}
	d, _, err := readBin(b[1:])
	if err != nil || !bytes.Equal(d, data) {
		t.Errorf("readBin after propBin: got %v, err %v", d, err)
	}
}

func TestPropUserProp(t *testing.T) {
	b := propUserProp("unit", "km/h")
	if b[0] != propUserProperty {
		t.Errorf("wrong property ID: 0x%02x", b[0])
	}
	k, rest, err := readStr(b[1:])
	if err != nil || k != "unit" {
		t.Errorf("key: got %q, err %v", k, err)
	}
	v, _, err := readStr(rest)
	if err != nil || v != "km/h" {
		t.Errorf("value: got %q, err %v", v, err)
	}
}

// ── readPropSet ────────────────────────────────────────────────────────────

func TestReadPropSetEmpty(t *testing.T) {
	pp, remaining, err := readPropSet([]byte{0}) // props length = 0
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("expected no remaining bytes, got %d", len(remaining))
	}
	if pp.receiveMax != nil || pp.topicAliasMax != nil {
		t.Error("expected nil negotiated values for empty props")
	}
}

func TestReadPropSetSessionExpiry(t *testing.T) {
	props := encodeProps(propU32(propSessionExpiry, 3600))
	pp, _, err := readPropSet(props)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pp.sessionExpiry == nil || *pp.sessionExpiry != 3600 {
		t.Errorf("session expiry: got %v", pp.sessionExpiry)
	}
}

func TestReadPropSetReceiveMax(t *testing.T) {
	props := encodeProps(propU16(propReceiveMax, 500))
	pp, _, err := readPropSet(props)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pp.receiveMax == nil || *pp.receiveMax != 500 {
		t.Errorf("receive max: got %v", pp.receiveMax)
	}
}

func TestReadPropSetTopicAlias(t *testing.T) {
	props := encodeProps(propU16(propTopicAlias, 7))
	pp, _, err := readPropSet(props)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pp.topicAlias == nil || *pp.topicAlias != 7 {
		t.Errorf("topic alias: got %v", pp.topicAlias)
	}
}

func TestReadPropSetUserProperties(t *testing.T) {
	props := encodeProps(
		propUserProp("key1", "val1"),
		propUserProp("key2", "val2"),
	)
	pp, _, err := readPropSet(props)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pp.userProps) != 2 {
		t.Fatalf("expected 2 user props, got %d", len(pp.userProps))
	}
	if pp.userProps[0].Key != "key1" || pp.userProps[0].Value != "val1" {
		t.Errorf("user prop[0]: got %+v", pp.userProps[0])
	}
	if pp.userProps[1].Key != "key2" || pp.userProps[1].Value != "val2" {
		t.Errorf("user prop[1]: got %+v", pp.userProps[1])
	}
}

func TestReadPropSetResponseTopic(t *testing.T) {
	props := encodeProps(propStr(propResponseTopic, "reply/here"))
	pp, _, err := readPropSet(props)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pp.responseTopic != "reply/here" {
		t.Errorf("response topic: got %q", pp.responseTopic)
	}
}

func TestReadPropSetCorrelationData(t *testing.T) {
	props := encodeProps(propBin(propCorrelationData, []byte("req-123")))
	pp, _, err := readPropSet(props)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(pp.correlationData, []byte("req-123")) {
		t.Errorf("correlation data: got %v", pp.correlationData)
	}
}

func TestReadPropSetSkipsUnknownKnownID(t *testing.T) {
	// propMaxQoS (0x24) is a byte property we parse via skipPropValue.
	props := encodeProps(propByte(propMaxQoS, 0x01))
	_, _, err := readPropSet(props)
	if err != nil {
		t.Errorf("unexpected error skipping known property: %v", err)
	}
}

func TestReadPropSetRemainingBytes(t *testing.T) {
	props := encodeProps(propU16(propReceiveMax, 100))
	extra := []byte{0xAA, 0xBB}
	pp, remaining, err := readPropSet(append(props, extra...))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pp.receiveMax == nil || *pp.receiveMax != 100 {
		t.Errorf("receive max: got %v", pp.receiveMax)
	}
	if !bytes.Equal(remaining, extra) {
		t.Errorf("remaining bytes: got %v, want %v", remaining, extra)
	}
}

// ── buildCONNECT ───────────────────────────────────────────────────────────

func TestBuildCONNECT_Basic(t *testing.T) {
	p := buildCONNECT("test-client", 30, 0, 0)
	if p[0] != pktCONNECT {
		t.Errorf("first byte: got 0x%02x, want 0x%02x", p[0], pktCONNECT)
	}
	// Parse remaining length to find body.
	_, n, err := decodeVarLen(p[1:])
	if err != nil {
		t.Fatalf("decode remaining length: %v", err)
	}
	body := p[1+n:]
	// Protocol name: {0,4,'M','Q','T','T'}
	if !bytes.Equal(body[:6], []byte{0, 4, 'M', 'Q', 'T', 'T'}) {
		t.Errorf("protocol name: got %v", body[:6])
	}
	if body[6] != 0x05 {
		t.Errorf("protocol level: got 0x%02x, want 0x05", body[6])
	}
	if body[7] != 0x02 {
		t.Errorf("connect flags: got 0x%02x, want 0x02 (CleanStart)", body[7])
	}
	keepalive := binary.BigEndian.Uint16(body[8:10])
	if keepalive != 30 {
		t.Errorf("keepalive: got %d, want 30", keepalive)
	}
}

func TestBuildCONNECT_SessionExpiry(t *testing.T) {
	p := buildCONNECT("sess-client", 30, 300, 0)
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	// Props start at body[10].
	props, _, err := readPropSet(body[10:])
	if err != nil {
		t.Fatalf("readPropSet: %v", err)
	}
	if props.sessionExpiry == nil || *props.sessionExpiry != 300 {
		t.Errorf("session expiry: got %v", props.sessionExpiry)
	}
}

func TestBuildCONNECT_ReceiveMax(t *testing.T) {
	p := buildCONNECT("rm-client", 30, 0, 100)
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	props, _, err := readPropSet(body[10:])
	if err != nil {
		t.Fatalf("readPropSet: %v", err)
	}
	if props.receiveMax == nil || *props.receiveMax != 100 {
		t.Errorf("receive max: got %v", props.receiveMax)
	}
}

// ── buildPUBLISH ───────────────────────────────────────────────────────────

func TestBuildPUBLISH_NoProps(t *testing.T) {
	p := buildPUBLISH("Vehicle/Speed", []byte(`{"speed":60}`), 0, false, 0, PublishProps{})
	if p[0] != pktPUBLISH {
		t.Errorf("header: got 0x%02x, want 0x%02x", p[0], pktPUBLISH)
	}
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	topic := string(body[2 : 2+topicLen])
	if topic != "Vehicle/Speed" {
		t.Errorf("topic: got %q", topic)
	}
}

func TestBuildPUBLISH_QoS1HasPacketID(t *testing.T) {
	p := buildPUBLISH("t", []byte("payload"), 1, false, 42, PublishProps{})
	if p[0] != pktPUBLISH|0x02 {
		t.Errorf("header: got 0x%02x, want 0x%02x", p[0], pktPUBLISH|0x02)
	}
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	after := body[2+topicLen:]
	pid := binary.BigEndian.Uint16(after[:2])
	if pid != 42 {
		t.Errorf("packet ID: got %d, want 42", pid)
	}
}

func TestBuildPUBLISH_RetainFlag(t *testing.T) {
	p := buildPUBLISH("t", nil, 0, true, 0, PublishProps{})
	if p[0]&0x01 == 0 {
		t.Error("retain flag not set in header")
	}
}

func TestBuildPUBLISH_WithResponseTopic(t *testing.T) {
	props := PublishProps{
		ResponseTopic:   "reply/here",
		CorrelationData: []byte("corr-42"),
	}
	p := buildPUBLISH("req/topic", []byte("body"), 0, false, 0, props)
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	// Skip topic → at props
	propsData := body[2+topicLen:]
	pp, _, err := readPropSet(propsData)
	if err != nil {
		t.Fatalf("readPropSet: %v", err)
	}
	if pp.responseTopic != "reply/here" {
		t.Errorf("response topic: got %q", pp.responseTopic)
	}
	if !bytes.Equal(pp.correlationData, []byte("corr-42")) {
		t.Errorf("correlation data: got %v", pp.correlationData)
	}
}

func TestBuildPUBLISH_WithUserProperties(t *testing.T) {
	props := PublishProps{
		UserProperties: []mqtt.UserProperty{
			{Key: "unit", Value: "km/h"},
			{Key: "source", Value: "gps"},
		},
	}
	p := buildPUBLISH("Vehicle/Speed", []byte("60"), 0, false, 0, props)
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	propsData := body[2+topicLen:]
	pp, _, err := readPropSet(propsData)
	if err != nil {
		t.Fatalf("readPropSet: %v", err)
	}
	if len(pp.userProps) != 2 {
		t.Fatalf("expected 2 user props, got %d", len(pp.userProps))
	}
	if pp.userProps[0].Key != "unit" || pp.userProps[0].Value != "km/h" {
		t.Errorf("user prop[0]: %+v", pp.userProps[0])
	}
}

func TestBuildPUBLISH_ExpiryInterval(t *testing.T) {
	props := PublishProps{ExpiryInterval: 60}
	p := buildPUBLISH("t", nil, 0, false, 0, props)
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	pp, _, err := readPropSet(body[2+topicLen:])
	if err != nil {
		t.Fatalf("readPropSet: %v", err)
	}
	if pp.expiryInterval == nil || *pp.expiryInterval != 60 {
		t.Errorf("expiry interval: got %v", pp.expiryInterval)
	}
}

// ── buildSUBSCRIBE ─────────────────────────────────────────────────────────

func TestBuildSUBSCRIBE_Basic(t *testing.T) {
	p := buildSUBSCRIBE("Vehicle/#", 0, 1, SubscribeOpts{})
	if p[0] != pktSUBSCRIBE {
		t.Errorf("header: got 0x%02x, want 0x%02x", p[0], pktSUBSCRIBE)
	}
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	pid := binary.BigEndian.Uint16(body[:2])
	if pid != 1 {
		t.Errorf("packet ID: got %d", pid)
	}
}

func TestBuildSUBSCRIBE_NoLocal(t *testing.T) {
	p := buildSUBSCRIBE("t", 0, 1, SubscribeOpts{NoLocal: true})
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	// body: [packetID(2)] [propsLen(1)] [topic(2+len)] [subOpts(1)]
	propsLen := int(body[2]) // first byte after packet ID is props length (0)
	topicStart := 3 + propsLen
	topicLen := int(binary.BigEndian.Uint16(body[topicStart:]))
	subOpts := body[topicStart+2+topicLen]
	if subOpts&0x04 == 0 {
		t.Error("NoLocal flag not set in subscription options byte")
	}
}

func TestBuildSUBSCRIBE_RetainAsPublished(t *testing.T) {
	p := buildSUBSCRIBE("t", 0, 1, SubscribeOpts{RetainAsPublished: true})
	_, n, _ := decodeVarLen(p[1:])
	body := p[1+n:]
	propsLen := int(body[2])
	topicStart := 3 + propsLen
	topicLen := int(binary.BigEndian.Uint16(body[topicStart:]))
	subOpts := body[topicStart+2+topicLen]
	if subOpts&0x08 == 0 {
		t.Error("RetainAsPublished flag not set in subscription options byte")
	}
}

// ── buildDISCONNECT ────────────────────────────────────────────────────────

func TestBuildDISCONNECT(t *testing.T) {
	p := buildDISCONNECT()
	if p[0] != pktDISCONNECT {
		t.Errorf("header: got 0x%02x", p[0])
	}
	if p[2] != 0x00 {
		t.Errorf("reason code: got 0x%02x, want 0x00 (Normal disconnect)", p[2])
	}
}
