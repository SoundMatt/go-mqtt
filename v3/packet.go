// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

//fusa:req REQ-WIRE-001
//fusa:req REQ-WIRE-002
//fusa:req REQ-WIRE-003
//fusa:req REQ-WIRE-004
//fusa:req REQ-WIRE-005
//fusa:req REQ-WIRE-006
//fusa:req REQ-WIRE-007
//fusa:req REQ-WIRE-008
//fusa:req REQ-WIRE-009
//fusa:req REQ-WIRE-010
//fusa:req REQ-WIRE-011
//fusa:req REQ-WIRE-012
//fusa:req REQ-WIRE-013
//fusa:req REQ-WIRE-014
//fusa:req REQ-WIRE-015

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MQTT v3.1.1 packet type constants (fixed-header first nibble).
const (
	pktCONNECT    byte = 0x10
	pktCONNACK    byte = 0x20
	pktPUBLISH    byte = 0x30
	pktPUBACK     byte = 0x40
	pktPUBREC     byte = 0x50 // QoS 2: publish received
	pktPUBREL     byte = 0x62 // QoS 2: publish release (type 6 + reserved flags 0b0010)
	pktPUBCOMP    byte = 0x70 // QoS 2: publish complete
	pktSUBSCRIBE  byte = 0x82 // type 8 + reserved flags 0b0010
	pktSUBACK     byte = 0x90
	pktUNSUBSCRIBE byte = 0xA2 // type 10 + reserved flags 0b0010
	pktUNSUBACK   byte = 0xB0
	pktPINGREQ    byte = 0xC0
	pktPINGRESP   byte = 0xD0
	pktDISCONNECT byte = 0xE0
)

//fusa:req REQ-WIRE-001
//fusa:req REQ-WIRE-002
func encodeVarLen(n int) []byte {
	if n == 0 {
		return []byte{0}
	}
	var buf []byte
	for n > 0 {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		buf = append(buf, b)
	}
	return buf
}

//fusa:req REQ-WIRE-001
//fusa:req REQ-WIRE-003
//fusa:req REQ-FAULT-001
func readVarLen(r io.Reader) (int, error) {
	multiplier := 1
	n := 0
	for i := range 4 {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, fmt.Errorf("mqtt/v3: remaining length byte %d: %w", i, err)
		}
		n += int(b[0]&0x7F) * multiplier
		if b[0]&0x80 == 0 {
			return n, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("mqtt/v3: malformed remaining length")
}

//fusa:req REQ-WIRE-004
func encodeStr(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

// encodeBytes encodes a raw byte slice with a 2-byte big-endian length prefix,
// as used for MQTT binary data fields (will payload, etc.).
func encodeBytes(b []byte) []byte {
	out := make([]byte, 2+len(b))
	binary.BigEndian.PutUint16(out, uint16(len(b)))
	copy(out[2:], b)
	return out
}

// encodeU16 encodes n as a 2-byte big-endian integer.
func encodeU16(n uint16) []byte {
	return []byte{byte(n >> 8), byte(n)}
}

// packet assembles a complete MQTT packet from a fixed-header byte and a body.
func packet(header byte, body []byte) []byte {
	pkt := append([]byte{header}, encodeVarLen(len(body))...)
	return append(pkt, body...)
}

//fusa:req REQ-WIRE-005
//fusa:req REQ-WIRE-006
//fusa:req REQ-CONN-011
func buildCONNECT(clientID string, keepaliveSecs uint16, w *will) []byte {
	// Connect flags: CleanSession=1 (bit 1).
	// Will flag (bit 2), will QoS (bits 4-3), will retain (bit 5) are set
	// when a will is provided per MQTT §3.1.2.5–3.1.2.7.
	connectFlags := byte(0x02) // CleanSession=1
	if w != nil {
		connectFlags |= 0x04 // will flag
		connectFlags |= byte(w.qos) << 3
		if w.retain {
			connectFlags |= 0x20
		}
	}

	body := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T', // protocol name
		0x04,                             // protocol level = 3.1.1
		connectFlags,
		byte(keepaliveSecs >> 8), byte(keepaliveSecs),
	}
	// Payload: client ID, then (if will set) will topic + will message.
	body = append(body, encodeStr(clientID)...)
	if w != nil {
		body = append(body, encodeStr(w.topic)...)
		body = append(body, encodeBytes(w.payload)...)
	}
	return packet(pktCONNECT, body)
}

//fusa:req REQ-WIRE-007
//fusa:req REQ-WIRE-008
//fusa:req REQ-WIRE-009
//fusa:req REQ-WIRE-010
func buildPUBLISH(topic string, payload []byte, qos byte, retain bool, packetID uint16) []byte {
	header := pktPUBLISH | (qos << 1)
	if retain {
		header |= 0x01
	}
	body := encodeStr(topic)
	if qos > 0 {
		body = append(body, encodeU16(packetID)...)
	}
	body = append(body, payload...)
	return packet(header, body)
}

//fusa:req REQ-WIRE-011
func buildSUBSCRIBE(filter string, qos byte, packetID uint16) []byte {
	body := encodeU16(packetID)
	body = append(body, encodeStr(filter)...)
	body = append(body, qos)
	return packet(pktSUBSCRIBE, body)
}

//fusa:req REQ-WIRE-012
func buildUNSUBSCRIBE(filter string, packetID uint16) []byte {
	body := encodeU16(packetID)
	body = append(body, encodeStr(filter)...)
	return packet(pktUNSUBSCRIBE, body)
}

// buildPUBACK builds a PUBACK packet for the given packet ID.
func buildPUBACK(packetID uint16) []byte {
	return packet(pktPUBACK, encodeU16(packetID))
}

// buildPUBREC builds a PUBREC packet (QoS 2, step 2) for the given packet ID.
//
//fusa:req REQ-WIRE-013
func buildPUBREC(packetID uint16) []byte {
	return packet(pktPUBREC, encodeU16(packetID))
}

// buildPUBREL builds a PUBREL packet (QoS 2, step 3) for the given packet ID.
// PUBREL carries reserved flags 0b0010 in its fixed header (pktPUBREL).
//
//fusa:req REQ-WIRE-014
func buildPUBREL(packetID uint16) []byte {
	return packet(pktPUBREL, encodeU16(packetID))
}

// buildPUBCOMP builds a PUBCOMP packet (QoS 2, step 4) for the given packet ID.
//
//fusa:req REQ-WIRE-015
func buildPUBCOMP(packetID uint16) []byte {
	return packet(pktPUBCOMP, encodeU16(packetID))
}

var pingReq = []byte{pktPINGREQ, 0x00}
var disconnect = []byte{pktDISCONNECT, 0x00}
