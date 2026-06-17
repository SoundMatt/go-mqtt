// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package broker

//fusa:req REQ-BROKER-WIRE-001
//fusa:req REQ-BROKER-WIRE-002

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MQTT v3.1.1 packet type constants (fixed-header first nibble).
const (
	pktCONNECT     byte = 0x10
	pktCONNACK     byte = 0x20
	pktPUBLISH     byte = 0x30
	pktPUBACK      byte = 0x40
	pktPUBREC      byte = 0x50
	pktPUBREL      byte = 0x62 // type 6 + reserved flags 0b0010
	pktPUBCOMP     byte = 0x70
	pktSUBSCRIBE   byte = 0x82 // type 8 + reserved flags 0b0010
	pktSUBACK      byte = 0x90
	pktUNSUBSCRIBE byte = 0xA2 // type 10 + reserved flags 0b0010
	pktUNSUBACK    byte = 0xB0
	pktPINGREQ     byte = 0xC0
	pktPINGRESP    byte = 0xD0
	pktDISCONNECT  byte = 0xE0
)

//fusa:req REQ-BROKER-WIRE-001
func readVarLen(r io.Reader) (int, error) {
	multiplier := 1
	n := 0
	for i := 0; i < 4; i++ {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		n += int(b[0]&0x7F) * multiplier
		if b[0]&0x80 == 0 {
			return n, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("broker: malformed remaining length")
}

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

func encodeU16(n uint16) []byte { return []byte{byte(n >> 8), byte(n)} }

func encodeStr(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

// packet assembles a fixed-header byte plus body into a complete packet.
func packet(header byte, body []byte) []byte {
	pkt := append([]byte{header}, encodeVarLen(len(body))...)
	return append(pkt, body...)
}

// readPacket reads one full MQTT packet, returning the fixed-header byte and body.
//
//fusa:req REQ-BROKER-WIRE-001
func readPacket(r io.Reader) (byte, []byte, error) {
	var hdr [1]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	remLen, err := readVarLen(r)
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, remLen)
	if remLen > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, nil, err
		}
	}
	return hdr[0], body, nil
}

// ── server-side packet builders ───────────────────────────────────────────────

//fusa:req REQ-BROKER-WIRE-002
func buildCONNACK(code byte) []byte { return packet(pktCONNACK, []byte{0x00, code}) }

func buildSUBACK(packetID uint16, codes []byte) []byte {
	return packet(pktSUBACK, append(encodeU16(packetID), codes...))
}

func buildUNSUBACK(packetID uint16) []byte { return packet(pktUNSUBACK, encodeU16(packetID)) }

func buildPUBACK(packetID uint16) []byte  { return packet(pktPUBACK, encodeU16(packetID)) }
func buildPUBREC(packetID uint16) []byte  { return packet(pktPUBREC, encodeU16(packetID)) }
func buildPUBCOMP(packetID uint16) []byte { return packet(pktPUBCOMP, encodeU16(packetID)) }

//fusa:req REQ-BROKER-WIRE-002
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

var pingRespPacket = []byte{pktPINGRESP, 0x00}
