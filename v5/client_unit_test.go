// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v5

// In-process fake-broker tests for the MQTT v5.0 client runtime: CONNACK
// handling, TopicAliasMax negotiation, inbound topic-alias register/resolve/drop,
// and QoS-2 rejection. These exercise client.go without a live broker.

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// fakeBrokerV5 is a minimal in-process TCP server speaking just enough MQTT v5.0
// to drive the client: it completes CONNECT/CONNACK (with caller-supplied CONNACK
// properties), drains client→broker packets, and lets the test inject frames.
type fakeBrokerV5 struct {
	ln   net.Listener
	conn net.Conn
}

func newFakeBrokerV5(t *testing.T) *fakeBrokerV5 {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fb := &fakeBrokerV5{ln: ln}
	t.Cleanup(fb.close)
	return fb
}

func (fb *fakeBrokerV5) addr() string { return fb.ln.Addr().String() }

func (fb *fakeBrokerV5) close() {
	if fb.conn != nil {
		_ = fb.conn.Close()
	}
	_ = fb.ln.Close()
}

// connack builds a v5 CONNACK with the given reason code and property bytes.
func connack(reason byte, props []byte) []byte {
	body := []byte{0x00, reason} // session-present = 0
	body = append(body, encodeVarLen(len(props))...)
	body = append(body, props...)
	pkt := []byte{pktCONNACK}
	pkt = append(pkt, encodeVarLen(len(body))...)
	return append(pkt, body...)
}

// accept waits for one client, completes the handshake with the given CONNACK,
// then starts draining client→broker packets so client writes never block.
func (fb *fakeBrokerV5) accept(t *testing.T, ca []byte) {
	t.Helper()
	conn, err := fb.ln.Accept()
	if err != nil {
		t.Errorf("accept: %v", err)
		return
	}
	fb.conn = conn
	// Read the CONNECT packet and discard it.
	readOnePacket(conn)
	if _, err := conn.Write(ca); err != nil {
		return
	}
	go func() {
		for {
			if _, _, ok := readOnePacketBody(conn); !ok {
				return
			}
		}
	}()
}

// readOnePacket reads and discards a single MQTT packet.
func readOnePacket(conn net.Conn) { readOnePacketBody(conn) }

func readOnePacketBody(conn net.Conn) (byte, []byte, bool) {
	var hdr [1]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return 0, nil, false
	}
	remLen, err := readVarLen(conn)
	if err != nil {
		return 0, nil, false
	}
	body := make([]byte, remLen)
	if remLen > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			return 0, nil, false
		}
	}
	return hdr[0], body, true
}

// TestV5DialCONNACK verifies Dial completes once a success CONNACK is received.
//
//fusa:test REQ-V5-CONN-002
func TestV5DialCONNACK(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = c.Close()
}

// TestV5DialBadReason verifies Dial returns an error when the CONNACK reason
// code is non-zero.
//
//fusa:test REQ-V5-CONN-003
func TestV5DialBadReason(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x85, nil)) // 0x85 = Client Identifier not valid

	if _, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0)); err == nil {
		t.Fatal("Dial succeeded on non-zero CONNACK reason code, want error")
	}
}

// TestV5DialTopicAliasMax verifies the client parses and applies the
// TopicAliasMax property from the CONNACK.
//
//fusa:test REQ-V5-CONN-004
func TestV5DialTopicAliasMax(t *testing.T) {
	fb := newFakeBrokerV5(t)
	props := propU16(propTopicAliasMax, 10)
	go fb.accept(t, connack(0x00, props))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if c.serverTopicAliasMax != 10 {
		t.Errorf("serverTopicAliasMax = %d, want 10", c.serverTopicAliasMax)
	}
}

// TestV5TopicAliasRegisterResolve verifies inbound topic-alias handling: a
// PUBLISH carrying both topic and alias registers the mapping (REQ-V5-ALIAS-001),
// and a later PUBLISH with an empty topic + the same alias resolves to it
// (REQ-V5-ALIAS-002).
//
//fusa:test REQ-V5-ALIAS-001
//fusa:test REQ-V5-ALIAS-002
func TestV5TopicAliasRegisterResolve(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("sensors/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let the readLoop settle

	// Register alias 5 -> sensors/temp.
	if _, err := fb.conn.Write(buildPUBLISH("sensors/temp", []byte("first"), 0, false, 0, PublishProps{TopicAlias: 5})); err != nil {
		t.Fatalf("write publish 1: %v", err)
	}
	if m := recvV5(t, sub); string(m.Payload) != "first" || m.Topic != "sensors/temp" {
		t.Errorf("msg 1 = %q@%q, want first@sensors/temp", m.Payload, m.Topic)
	}

	// Resolve alias 5 with an empty topic.
	if _, err := fb.conn.Write(buildPUBLISH("", []byte("second"), 0, false, 0, PublishProps{TopicAlias: 5})); err != nil {
		t.Fatalf("write publish 2: %v", err)
	}
	if m := recvV5(t, sub); string(m.Payload) != "second" || m.Topic != "sensors/temp" {
		t.Errorf("msg 2 = %q@%q, want second@sensors/temp (alias resolved)", m.Payload, m.Topic)
	}
}

// TestV5TopicAliasUnknownDropped verifies a PUBLISH with an empty topic and an
// unregistered alias is dropped rather than delivered with an empty topic.
//
//fusa:test REQ-V5-ALIAS-003
//fusa:sec-test REQ-SEC-009
func TestV5TopicAliasUnknownDropped(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("sensors/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// Unknown alias 9 with empty topic — must be dropped.
	if _, err := fb.conn.Write(buildPUBLISH("", []byte("ghost"), 0, false, 0, PublishProps{TopicAlias: 9})); err != nil {
		t.Fatalf("write publish: %v", err)
	}
	// A valid follow-up message proves the stream is still live and the ghost
	// was dropped (not delivered).
	if _, err := fb.conn.Write(buildPUBLISH("sensors/ok", []byte("real"), 0, false, 0, PublishProps{})); err != nil {
		t.Fatalf("write publish 2: %v", err)
	}
	if m := recvV5(t, sub); string(m.Payload) != "real" {
		t.Errorf("delivered %q, want real (the unknown-alias ghost should have been dropped)", m.Payload)
	}
}

// TestV5QoS2Unsupported verifies that QoS 2 publish and subscribe are rejected
// with ErrQoSUnsupported (v5 supports QoS 0/1 only).
//
//fusa:test REQ-PUB-002
func TestV5QoS2Unsupported(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Publish(context.Background(), "t", mqtt.ExactlyOnce, []byte("x")); err != mqtt.ErrQoSUnsupported {
		t.Errorf("Publish QoS2 err = %v, want ErrQoSUnsupported", err)
	}
	if _, err := c.Subscribe("t", mqtt.ExactlyOnce); err != mqtt.ErrQoSUnsupported {
		t.Errorf("Subscribe QoS2 err = %v, want ErrQoSUnsupported", err)
	}
}

func recvV5(t *testing.T, sub mqtt.Subscription) mqtt.Message {
	t.Helper()
	select {
	case m := <-sub.C():
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for v5 message")
		return mqtt.Message{}
	}
}
