// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v5

//fusa:req REQ-WIRE-001
//fusa:req REQ-WIRE-002
//fusa:req REQ-WIRE-003
//fusa:req REQ-WIRE-004
//fusa:req REQ-WIRE-005
//fusa:req REQ-V5-PUB-001
//fusa:req REQ-V5-PUB-002
//fusa:req REQ-V5-PUB-003
//fusa:req REQ-V5-SUB-001
//fusa:req REQ-V5-SUB-002
//fusa:req REQ-V5-SUB-003
//fusa:req REQ-V5-ALIAS-001
//fusa:req REQ-V5-SESSION-001

import (
	"encoding/binary"
	"fmt"
	"io"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// MQTT v5.0 packet type constants (fixed-header first nibble).
const (
	pktCONNECT     byte = 0x10
	pktCONNACK     byte = 0x20
	pktPUBLISH     byte = 0x30
	pktPUBACK      byte = 0x40
	pktSUBSCRIBE   byte = 0x82 // type 8 + reserved flags 0b0010
	pktSUBACK      byte = 0x90
	pktUNSUBSCRIBE byte = 0xA2 // type 10 + reserved flags 0b0010
	pktUNSUBACK    byte = 0xB0
	pktPINGREQ     byte = 0xC0
	pktPINGRESP    byte = 0xD0
	pktDISCONNECT  byte = 0xE0
)

// MQTT v5.0 property identifiers (§2.2.2).
const (
	propPayloadFormat     byte = 0x01
	propMsgExpiry         byte = 0x02
	propContentType       byte = 0x03
	propResponseTopic     byte = 0x08
	propCorrelationData   byte = 0x09
	propSubID             byte = 0x0B
	propSessionExpiry     byte = 0x11
	propAssignedClientID  byte = 0x12
	propServerKeepalive   byte = 0x13
	propAuthMethod        byte = 0x15
	propAuthData          byte = 0x16
	propReqProblemInfo    byte = 0x17
	propWillDelayInterval byte = 0x18
	propReqResponseInfo   byte = 0x19
	propResponseInfo      byte = 0x1A
	propServerRef         byte = 0x1C
	propReasonString      byte = 0x1F
	propReceiveMax        byte = 0x21
	propTopicAliasMax     byte = 0x22
	propTopicAlias        byte = 0x23
	propMaxQoS            byte = 0x24
	propRetainAvailable   byte = 0x25
	propUserProperty      byte = 0x26
	propMaxPacketSize     byte = 0x27
	propWildcardSubAvail  byte = 0x28
	propSubIDAvail        byte = 0x29
	propSharedSubAvail    byte = 0x2A
)

// propKind classifies how many bytes a property value occupies on the wire.
type propKind int

const (
	propKindByte    propKind = iota
	propKindU16
	propKindU32
	propKindVarInt
	propKindStr
	propKindBin
	propKindStrPair
)

// propKinds maps every standard MQTT v5 property ID to its wire encoding kind,
// enabling the parser to skip properties it does not explicitly handle.
var propKinds = map[byte]propKind{
	0x01: propKindByte,
	0x02: propKindU32,
	0x03: propKindStr,
	0x08: propKindStr,
	0x09: propKindBin,
	0x0B: propKindVarInt,
	0x11: propKindU32,
	0x12: propKindStr,
	0x13: propKindU16,
	0x15: propKindStr,
	0x16: propKindBin,
	0x17: propKindByte,
	0x18: propKindU32,
	0x19: propKindByte,
	0x1A: propKindStr,
	0x1C: propKindStr,
	0x1F: propKindStr,
	0x21: propKindU16,
	0x22: propKindU16,
	0x23: propKindU16,
	0x24: propKindByte,
	0x25: propKindByte,
	0x26: propKindStrPair,
	0x27: propKindU32,
	0x28: propKindByte,
	0x29: propKindByte,
	0x2A: propKindByte,
}

// ── Wire encoding helpers ──────────────────────────────────────────────────

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

func readVarLen(r io.Reader) (int, error) {
	multiplier := 1
	n := 0
	for i := range 4 {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, fmt.Errorf("mqtt/v5: remaining length byte %d: %w", i, err)
		}
		n += int(b[0]&0x7F) * multiplier
		if b[0]&0x80 == 0 {
			return n, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("mqtt/v5: malformed remaining length")
}

func decodeVarLen(data []byte) (val int, n int, err error) {
	multiplier := 1
	for i, b := range data {
		if i >= 4 {
			return 0, 0, fmt.Errorf("mqtt/v5: malformed variable length integer")
		}
		val += int(b&0x7F) * multiplier
		n++
		if b&0x80 == 0 {
			return val, n, nil
		}
		multiplier *= 128
	}
	return 0, 0, fmt.Errorf("mqtt/v5: truncated variable length integer")
}

func encodeStr(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

func encodeBin(data []byte) []byte {
	b := make([]byte, 2+len(data))
	binary.BigEndian.PutUint16(b, uint16(len(data)))
	copy(b[2:], data)
	return b
}

func encodeU16(n uint16) []byte { return []byte{byte(n >> 8), byte(n)} }
func encodeU32(n uint32) []byte {
	return []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

func pkt(header byte, body []byte) []byte {
	out := append([]byte{header}, encodeVarLen(len(body))...)
	return append(out, body...)
}

// ── Property encoding ──────────────────────────────────────────────────────

// encodeProps wraps property byte slices with a variable-length length prefix.
func encodeProps(parts ...[]byte) []byte {
	var body []byte
	for _, p := range parts {
		body = append(body, p...)
	}
	return append(encodeVarLen(len(body)), body...)
}

func propByte(id, v byte) []byte       { return []byte{id, v} }
func propU16(id byte, v uint16) []byte { return append([]byte{id}, encodeU16(v)...) }
func propU32(id byte, v uint32) []byte { return append([]byte{id}, encodeU32(v)...) }
func propStr(id byte, s string) []byte { return append([]byte{id}, encodeStr(s)...) }
func propBin(id byte, d []byte) []byte { return append([]byte{id}, encodeBin(d)...) }

func propUserProp(key, val string) []byte {
	b := []byte{propUserProperty}
	b = append(b, encodeStr(key)...)
	b = append(b, encodeStr(val)...)
	return b
}

// ── Property parsing ───────────────────────────────────────────────────────

// parsedProps holds decoded MQTT v5 properties extracted from an incoming packet.
type parsedProps struct {
	sessionExpiry    *uint32
	receiveMax       *uint16
	topicAliasMax    *uint16
	topicAlias       *uint16
	serverKeepalive  *uint16
	assignedClientID string
	responseTopic    string
	correlationData  []byte
	contentType      string
	expiryInterval   *uint32
	userProps        []mqtt.UserProperty
}

// readPropSet parses a property set from data and returns parsed props plus remaining bytes.
func readPropSet(data []byte) (parsedProps, []byte, error) {
	var pp parsedProps
	if len(data) == 0 {
		return pp, data, nil
	}

	propLen, n, err := decodeVarLen(data)
	if err != nil {
		return pp, nil, fmt.Errorf("mqtt/v5: property length: %w", err)
	}
	data = data[n:]
	if len(data) < propLen {
		return pp, nil, fmt.Errorf("mqtt/v5: truncated properties: need %d have %d", propLen, len(data))
	}

	body := data[:propLen]
	remaining := data[propLen:]

	for len(body) > 0 {
		id := body[0]
		body = body[1:]

		var e error
		switch id {
		case propSessionExpiry:
			if len(body) < 4 {
				return pp, nil, fmt.Errorf("mqtt/v5: short session expiry")
			}
			v := binary.BigEndian.Uint32(body[:4])
			pp.sessionExpiry = &v
			body = body[4:]
		case propReceiveMax:
			if len(body) < 2 {
				return pp, nil, fmt.Errorf("mqtt/v5: short receive max")
			}
			v := binary.BigEndian.Uint16(body[:2])
			pp.receiveMax = &v
			body = body[2:]
		case propTopicAliasMax:
			if len(body) < 2 {
				return pp, nil, fmt.Errorf("mqtt/v5: short topic alias max")
			}
			v := binary.BigEndian.Uint16(body[:2])
			pp.topicAliasMax = &v
			body = body[2:]
		case propTopicAlias:
			if len(body) < 2 {
				return pp, nil, fmt.Errorf("mqtt/v5: short topic alias")
			}
			v := binary.BigEndian.Uint16(body[:2])
			pp.topicAlias = &v
			body = body[2:]
		case propServerKeepalive:
			if len(body) < 2 {
				return pp, nil, fmt.Errorf("mqtt/v5: short server keepalive")
			}
			v := binary.BigEndian.Uint16(body[:2])
			pp.serverKeepalive = &v
			body = body[2:]
		case propAssignedClientID:
			var s string
			s, body, e = readStr(body)
			if e != nil {
				return pp, nil, fmt.Errorf("mqtt/v5: assigned client id: %w", e)
			}
			pp.assignedClientID = s
		case propResponseTopic:
			var s string
			s, body, e = readStr(body)
			if e != nil {
				return pp, nil, fmt.Errorf("mqtt/v5: response topic: %w", e)
			}
			pp.responseTopic = s
		case propCorrelationData:
			var d []byte
			d, body, e = readBin(body)
			if e != nil {
				return pp, nil, fmt.Errorf("mqtt/v5: correlation data: %w", e)
			}
			pp.correlationData = d
		case propContentType:
			var s string
			s, body, e = readStr(body)
			if e != nil {
				return pp, nil, fmt.Errorf("mqtt/v5: content type: %w", e)
			}
			pp.contentType = s
		case propMsgExpiry:
			if len(body) < 4 {
				return pp, nil, fmt.Errorf("mqtt/v5: short msg expiry")
			}
			v := binary.BigEndian.Uint32(body[:4])
			pp.expiryInterval = &v
			body = body[4:]
		case propUserProperty:
			var k, v string
			k, body, e = readStr(body)
			if e != nil {
				return pp, nil, fmt.Errorf("mqtt/v5: user property key: %w", e)
			}
			v, body, e = readStr(body)
			if e != nil {
				return pp, nil, fmt.Errorf("mqtt/v5: user property value: %w", e)
			}
			pp.userProps = append(pp.userProps, mqtt.UserProperty{Key: k, Value: v})
		default:
			body, e = skipPropValue(id, body)
			if e != nil {
				return pp, nil, fmt.Errorf("mqtt/v5: property 0x%02x: %w", id, e)
			}
		}
	}
	return pp, remaining, nil
}

func readStr(data []byte) (string, []byte, error) {
	if len(data) < 2 {
		return "", nil, fmt.Errorf("truncated string length")
	}
	n := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	if len(data) < n {
		return "", nil, fmt.Errorf("truncated string value")
	}
	return string(data[:n]), data[n:], nil
}

func readBin(data []byte) ([]byte, []byte, error) {
	if len(data) < 2 {
		return nil, nil, fmt.Errorf("truncated binary length")
	}
	n := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	if len(data) < n {
		return nil, nil, fmt.Errorf("truncated binary value")
	}
	result := make([]byte, n)
	copy(result, data[:n])
	return result, data[n:], nil
}

func skipPropValue(id byte, data []byte) ([]byte, error) {
	kind, ok := propKinds[id]
	if !ok {
		return nil, fmt.Errorf("unknown property ID 0x%02x", id)
	}
	switch kind {
	case propKindByte:
		if len(data) < 1 {
			return nil, fmt.Errorf("short byte property")
		}
		return data[1:], nil
	case propKindU16:
		if len(data) < 2 {
			return nil, fmt.Errorf("short u16 property")
		}
		return data[2:], nil
	case propKindU32:
		if len(data) < 4 {
			return nil, fmt.Errorf("short u32 property")
		}
		return data[4:], nil
	case propKindStr, propKindBin:
		if len(data) < 2 {
			return nil, fmt.Errorf("short length-prefixed property")
		}
		n := int(binary.BigEndian.Uint16(data[:2]))
		data = data[2:]
		if len(data) < n {
			return nil, fmt.Errorf("truncated length-prefixed property")
		}
		return data[n:], nil
	case propKindStrPair:
		for range 2 {
			if len(data) < 2 {
				return nil, fmt.Errorf("short string-pair property")
			}
			n := int(binary.BigEndian.Uint16(data[:2]))
			data = data[2:]
			if len(data) < n {
				return nil, fmt.Errorf("truncated string-pair property")
			}
			data = data[n:]
		}
		return data, nil
	case propKindVarInt:
		for i := range 4 {
			if len(data) < 1 {
				return nil, fmt.Errorf("short varint property")
			}
			b := data[0]
			data = data[1:]
			if b&0x80 == 0 {
				break
			}
			if i == 3 {
				return nil, fmt.Errorf("malformed varint property")
			}
		}
		return data, nil
	}
	return data, nil
}

// ── Exported option types ──────────────────────────────────────────────────

// PublishProps holds MQTT v5 per-message properties for PublishV5.
//
//fusa:req REQ-V5-PUB-001
//fusa:req REQ-V5-PUB-002
//fusa:req REQ-V5-PUB-003
type PublishProps struct {
	ResponseTopic   string             // for request/response correlation
	CorrelationData []byte             // opaque identifier echoed in the response
	UserProperties  []mqtt.UserProperty // arbitrary key/value metadata
	ContentType     string             // MIME type of the payload
	ExpiryInterval  uint32             // seconds; 0 = no expiry
	TopicAlias      uint16             // 0 = no alias; must be ≤ server TopicAliasMax
}

// SubscribeOpts holds MQTT v5 subscription options for SubscribeV5.
//
//fusa:req REQ-V5-SUB-001
//fusa:req REQ-V5-SUB-002
//fusa:req REQ-V5-SUB-003
type SubscribeOpts struct {
	NoLocal           bool // do not deliver own publishes back to this client
	RetainAsPublished bool // preserve the RETAIN flag as set by the publisher
	RetainHandling    byte // 0=send retained on subscribe, 1=only if new, 2=never
}

// ── Packet builders ────────────────────────────────────────────────────────

// buildCONNECT builds an MQTT v5.0 CONNECT packet with CleanStart=true.
func buildCONNECT(clientID string, keepaliveSecs uint16, sessionExpiry uint32, receiveMax uint16) []byte {
	var connProps [][]byte
	if sessionExpiry > 0 {
		connProps = append(connProps, propU32(propSessionExpiry, sessionExpiry))
	}
	if receiveMax > 0 {
		connProps = append(connProps, propU16(propReceiveMax, receiveMax))
	}

	body := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T', // protocol name
		0x05,                             // protocol level = 5
		0x02,                             // connect flags: CleanStart=1
		byte(keepaliveSecs >> 8), byte(keepaliveSecs),
	}
	body = append(body, encodeProps(connProps...)...)
	body = append(body, encodeStr(clientID)...)
	return pkt(pktCONNECT, body)
}

// buildPUBLISH builds an MQTT v5 PUBLISH packet with optional properties.
func buildPUBLISH(topic string, payload []byte, qos byte, retain bool, packetID uint16, props PublishProps) []byte {
	header := pktPUBLISH | (qos << 1)
	if retain {
		header |= 0x01
	}
	body := encodeStr(topic)
	if qos > 0 {
		body = append(body, encodeU16(packetID)...)
	}

	var pubProps [][]byte
	if props.TopicAlias != 0 {
		pubProps = append(pubProps, propU16(propTopicAlias, props.TopicAlias))
	}
	if props.ExpiryInterval != 0 {
		pubProps = append(pubProps, propU32(propMsgExpiry, props.ExpiryInterval))
	}
	if props.ResponseTopic != "" {
		pubProps = append(pubProps, propStr(propResponseTopic, props.ResponseTopic))
	}
	if len(props.CorrelationData) > 0 {
		pubProps = append(pubProps, propBin(propCorrelationData, props.CorrelationData))
	}
	if props.ContentType != "" {
		pubProps = append(pubProps, propStr(propContentType, props.ContentType))
	}
	for _, up := range props.UserProperties {
		pubProps = append(pubProps, propUserProp(up.Key, up.Value))
	}
	body = append(body, encodeProps(pubProps...)...)
	body = append(body, payload...)
	return pkt(header, body)
}

// buildSUBSCRIBE builds an MQTT v5 SUBSCRIBE packet with subscription options.
func buildSUBSCRIBE(filter string, qos byte, packetID uint16, opts SubscribeOpts) []byte {
	body := encodeU16(packetID)
	body = append(body, encodeProps()...) // empty properties
	body = append(body, encodeStr(filter)...)
	subOpts := qos & 0x03
	if opts.NoLocal {
		subOpts |= 0x04
	}
	if opts.RetainAsPublished {
		subOpts |= 0x08
	}
	subOpts |= (opts.RetainHandling & 0x03) << 4
	body = append(body, subOpts)
	return pkt(pktSUBSCRIBE, body)
}

// buildUNSUBSCRIBE builds an MQTT v5 UNSUBSCRIBE packet.
func buildUNSUBSCRIBE(filter string, packetID uint16) []byte {
	body := encodeU16(packetID)
	body = append(body, encodeProps()...) // empty properties
	body = append(body, encodeStr(filter)...)
	return pkt(pktUNSUBSCRIBE, body)
}

// buildPUBACK builds an MQTT v5 PUBACK packet (reason code 0x00 = Success).
func buildPUBACK(packetID uint16) []byte {
	body := append(encodeU16(packetID), 0x00, 0x00) // reason + empty props
	return pkt(pktPUBACK, body)
}

// buildDISCONNECT builds an MQTT v5 DISCONNECT with reason 0x00 (Normal disconnect).
func buildDISCONNECT() []byte {
	return []byte{pktDISCONNECT, 0x02, 0x00, 0x00} // remaining=2, reason=0, propsLen=0
}

var pingReq = []byte{pktPINGREQ, 0x00}
