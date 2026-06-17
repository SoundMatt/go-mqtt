// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package broker

//fusa:req REQ-BROKER-004
//fusa:req REQ-BROKER-005
//fusa:req REQ-BROKER-006
//fusa:req REQ-BROKER-008

import (
	"net"
	"sync"
	"sync/atomic"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// session is one connected client.
type session struct {
	conn   net.Conn
	server *Server

	mu       sync.Mutex
	subs     map[string]byte        // filter → granted QoS
	qos2In   map[uint16]qos2Pending // inbound QoS 2 packet ID → message awaiting PUBREL
	writeMu  sync.Mutex
	pktID    atomic.Uint32
	clientID string

	will         *willMsg
	disconnected bool
}

// qos2Pending is an inbound QoS 2 PUBLISH held between PUBREC and PUBREL.
type qos2Pending struct {
	topic   string
	payload []byte
	retain  bool
}

type willMsg struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

func (s *session) nextID() uint16 {
	id := s.pktID.Add(1) & 0xFFFF
	if id == 0 {
		id = s.pktID.Add(1) & 0xFFFF
	}
	return uint16(id)
}

func (s *session) send(pkt []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.conn.Write(pkt)
	return err
}

// serve runs the session protocol: CONNECT first, then a packet loop.
//
//fusa:req REQ-BROKER-004
func (s *session) serve() {
	defer s.cleanup()

	hdr, body, err := readPacket(s.conn)
	if err != nil || hdr&0xF0 != pktCONNECT&0xF0 {
		return
	}
	if !s.handleConnect(body) {
		return
	}
	s.server.register(s)

	for {
		hdr, body, err := readPacket(s.conn)
		if err != nil {
			return
		}
		switch hdr & 0xF0 {
		case pktPUBLISH & 0xF0:
			s.handlePublish(hdr, body)
		case pktPUBREL & 0xF0:
			s.handlePubrel(body)
		case pktPUBACK & 0xF0, pktPUBREC & 0xF0, pktPUBCOMP & 0xF0:
			// Outbound QoS 1/2 acks from the subscriber. The MVP delivers once
			// and does not retransmit, so these are consumed without action.
		case pktSUBSCRIBE & 0xF0:
			s.handleSubscribe(body)
		case pktUNSUBSCRIBE & 0xF0:
			s.handleUnsubscribe(body)
		case pktPINGREQ & 0xF0:
			_ = s.send(pingRespPacket)
		case pktDISCONNECT & 0xF0:
			s.disconnected = true
			return
		}
	}
}

// handleConnect parses the CONNECT payload (client ID + optional will) and
// answers CONNACK. It returns false if the packet is malformed.
//
//fusa:req REQ-BROKER-004
//fusa:req REQ-BROKER-008
func (s *session) handleConnect(body []byte) bool {
	// Variable header: protocol name + level + flags + keepalive (10 bytes for "MQTT").
	if len(body) < 10 {
		return false
	}
	connectFlags := body[7]
	off := 10

	clientID, off, ok := readString(body, off)
	if !ok {
		return false
	}
	s.clientID = clientID

	if connectFlags&0x04 != 0 { // will flag
		willTopic, noff, ok := readString(body, off)
		if !ok {
			return false
		}
		willPayload, noff2, ok := readBytes(body, noff)
		if !ok {
			return false
		}
		off = noff2
		s.will = &willMsg{
			topic:   willTopic,
			payload: willPayload,
			qos:     (connectFlags >> 3) & 0x03,
			retain:  connectFlags&0x20 != 0,
		}
	}
	_ = off

	return s.send(buildCONNACK(0x00)) == nil
}

// handlePublish processes an inbound PUBLISH, performing the QoS acknowledgement
// and routing (QoS 0/1 deliver immediately; QoS 2 defers until PUBREL).
//
//fusa:req REQ-BROKER-005
//fusa:req REQ-BROKER-006
func (s *session) handlePublish(hdr byte, body []byte) {
	qos := (hdr >> 1) & 0x03
	retain := hdr&0x01 != 0

	topic, off, ok := readString(body, 0)
	if !ok {
		return
	}
	var packetID uint16
	if qos > 0 {
		if len(body) < off+2 {
			return
		}
		packetID = uint16(body[off])<<8 | uint16(body[off+1])
		off += 2
	}
	if off > len(body) {
		return
	}
	payload := make([]byte, len(body)-off)
	copy(payload, body[off:])

	switch qos {
	case 0:
		s.server.publish(topic, payload, 0, retain)
	case 1:
		s.server.publish(topic, payload, 1, retain)
		_ = s.send(buildPUBACK(packetID))
	case 2:
		// Store and acknowledge with PUBREC; route on PUBREL (exactly once).
		s.mu.Lock()
		if _, dup := s.qos2In[packetID]; !dup {
			s.qos2In[packetID] = qos2Pending{topic: topic, payload: payload, retain: retain}
		}
		s.mu.Unlock()
		_ = s.send(buildPUBREC(packetID))
	}
}

// handlePubrel completes an inbound QoS 2 exchange: route the stored message and
// answer PUBCOMP.
//
//fusa:req REQ-BROKER-006
func (s *session) handlePubrel(body []byte) {
	if len(body) < 2 {
		return
	}
	packetID := uint16(body[0])<<8 | uint16(body[1])
	s.mu.Lock()
	pending, ok := s.qos2In[packetID]
	delete(s.qos2In, packetID)
	s.mu.Unlock()

	if ok {
		s.server.publish(pending.topic, pending.payload, 2, pending.retain)
	}
	_ = s.send(buildPUBCOMP(packetID))
}

// handleSubscribe registers filters, answers SUBACK, and replays retained
// messages matching each filter.
//
//fusa:req REQ-BROKER-007
func (s *session) handleSubscribe(body []byte) {
	if len(body) < 2 {
		return
	}
	packetID := uint16(body[0])<<8 | uint16(body[1])
	off := 2

	var codes []byte
	type retained struct {
		topic   string
		payload []byte
		qos     byte
	}
	var toReplay []retained

	for off < len(body) {
		filter, noff, ok := readString(body, off)
		if !ok || noff >= len(body) {
			break
		}
		reqQoS := body[noff] & 0x03
		off = noff + 1

		s.mu.Lock()
		s.subs[filter] = reqQoS
		s.mu.Unlock()
		codes = append(codes, reqQoS)

		topics, payloads, qoss := s.server.retainedFor(filter)
		for i := range topics {
			dq := qoss[i]
			if reqQoS < dq {
				dq = reqQoS
			}
			toReplay = append(toReplay, retained{topics[i], payloads[i], dq})
		}
	}

	_ = s.send(buildSUBACK(packetID, codes))

	for _, r := range toReplay {
		s.deliver(r.topic, r.payload, r.qos, true)
	}
}

// handleUnsubscribe removes filters and answers UNSUBACK.
//
//fusa:req REQ-BROKER-007
func (s *session) handleUnsubscribe(body []byte) {
	if len(body) < 2 {
		return
	}
	packetID := uint16(body[0])<<8 | uint16(body[1])
	off := 2
	for off < len(body) {
		filter, noff, ok := readString(body, off)
		if !ok {
			break
		}
		off = noff
		s.mu.Lock()
		delete(s.subs, filter)
		s.mu.Unlock()
	}
	_ = s.send(buildUNSUBACK(packetID))
}

// matchQoS returns the granted QoS for the best filter matching topic, and
// whether any filter matched.
//
//fusa:req REQ-BROKER-006
func (s *session) matchQoS(topic string) (byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	best := byte(0)
	found := false
	for filter, qos := range s.subs {
		if mqtt.MatchTopic(filter, topic) {
			found = true
			if qos > best {
				best = qos
			}
		}
	}
	return best, found
}

// deliver sends a PUBLISH to this session's client at the given QoS. It returns
// false only if the write fails.
//
//fusa:req REQ-BROKER-006
func (s *session) deliver(topic string, payload []byte, qos byte, retain bool) bool {
	var pid uint16
	if qos > 0 {
		pid = s.nextID()
	}
	return s.send(buildPUBLISH(topic, payload, qos, retain, pid)) == nil
}

// cleanup removes the session and, if the disconnect was not clean, publishes
// the last-will message.
//
//fusa:req REQ-BROKER-008
func (s *session) cleanup() {
	_ = s.conn.Close()
	s.server.unregister(s)
	if !s.disconnected && s.will != nil {
		s.server.publish(s.will.topic, s.will.payload, s.will.qos, s.will.retain)
	}
}

// ── byte helpers ──────────────────────────────────────────────────────────────

// readString reads a 2-byte length-prefixed UTF-8 string at off.
func readString(b []byte, off int) (string, int, bool) {
	if off+2 > len(b) {
		return "", off, false
	}
	n := int(b[off])<<8 | int(b[off+1])
	off += 2
	if off+n > len(b) {
		return "", off, false
	}
	return string(b[off : off+n]), off + n, true
}

// readBytes reads a 2-byte length-prefixed binary field at off.
func readBytes(b []byte, off int) ([]byte, int, bool) {
	if off+2 > len(b) {
		return nil, off, false
	}
	n := int(b[off])<<8 | int(b[off+1])
	off += 2
	if off+n > len(b) {
		return nil, off, false
	}
	out := make([]byte, n)
	copy(out, b[off:off+n])
	return out, off + n, true
}
