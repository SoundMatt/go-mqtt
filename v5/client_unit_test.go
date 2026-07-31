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
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// fakeBrokerV5 is a minimal in-process TCP server speaking just enough MQTT v5.0
// to drive the client: it completes CONNECT/CONNACK (with caller-supplied CONNACK
// properties), drains client→broker packets, and lets the test inject frames.
//
// By default it answers every client SUBSCRIBE with a success (0x00) SUBACK
// and every QoS 1 client PUBLISH with a success (0x00) PUBACK, so tests that
// don't care about ack handling aren't affected by SubscribeV5/PublishV5(QoS1)
// now blocking on the ack (see TestV5Subscribe*ReasonCode below for tests
// that do care, via subAckReason/pubAckReason).
type fakeBrokerV5 struct {
	ln   net.Listener
	conn net.Conn

	// subAckReason/pubAckReason are the Reason Code the drain goroutine
	// echoes back for every SUBSCRIBE/QoS-1-PUBLISH it sees. Set before
	// calling accept (or acceptNoAck); zero value 0x00 = Success.
	subAckReason byte
	pubAckReason byte
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
// then drains client→broker packets, answering SUBSCRIBE with a SUBACK
// (reason fb.subAckReason) and QoS 1 PUBLISH with a PUBACK (reason
// fb.pubAckReason) so client writes never block and SubscribeV5/PublishV5
// don't hang waiting for an ack that never arrives.
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
			hdr, body, ok := readOnePacketBody(conn)
			if !ok {
				return
			}
			switch hdr & 0xF0 {
			case pktSUBSCRIBE & 0xF0:
				if len(body) >= 2 {
					packetID := uint16(body[0])<<8 | uint16(body[1])
					_, _ = conn.Write(testSUBACK(packetID, fb.subAckReason))
				}
			case pktPUBLISH & 0xF0:
				qos := (hdr >> 1) & 0x03
				if qos == 1 && len(body) >= 2 {
					topicLen := int(body[0])<<8 | int(body[1])
					off := 2 + topicLen
					if len(body) >= off+2 {
						packetID := uint16(body[off])<<8 | uint16(body[off+1])
						_, _ = conn.Write(testPUBACK(packetID, fb.pubAckReason))
					}
				}
			}
		}
	}()
}

// acceptNoAck is like accept but never answers SUBSCRIBE or QoS 1 PUBLISH —
// it only drains them — for tests exercising the SubscribeV5/PublishV5 ack
// timeout path.
func (fb *fakeBrokerV5) acceptNoAck(t *testing.T, ca []byte) {
	t.Helper()
	conn, err := fb.ln.Accept()
	if err != nil {
		t.Errorf("accept: %v", err)
		return
	}
	fb.conn = conn
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

// testSUBACK builds a minimal single-reason-code SUBACK for the fake broker
// to answer a client SUBSCRIBE with (this client always requests exactly one
// Topic Filter per SUBSCRIBE, so one reason code suffices, §3.9).
func testSUBACK(packetID uint16, reason byte) []byte {
	body := []byte{byte(packetID >> 8), byte(packetID), 0x00, reason} // propsLen=0, then the one reason code
	pkt := []byte{pktSUBACK}
	pkt = append(pkt, encodeVarLen(len(body))...)
	return append(pkt, body...)
}

// testPUBACK builds a PUBACK with an explicit Reason Code for the fake broker
// to answer a client QoS 1 PUBLISH with (§3.4.2).
func testPUBACK(packetID uint16, reason byte) []byte {
	body := []byte{byte(packetID >> 8), byte(packetID), reason}
	pkt := []byte{pktPUBACK}
	pkt = append(pkt, encodeVarLen(len(body))...)
	return append(pkt, body...)
}

// testPublishWithTopicAlias builds a QoS 0 PUBLISH carrying an explicit Topic
// Alias property, including alias 0. buildPUBLISH can't be used for this: it
// intentionally omits the Topic Alias property when TopicAlias == 0 (correct
// for the client's own send path), so a test simulating a broker that sends
// an out-of-spec alias-0 property needs to construct the packet directly.
func testPublishWithTopicAlias(t *testing.T, topic, payload string, alias uint16) []byte {
	t.Helper()
	body := encodeStr(topic)
	body = append(body, encodeProps(propU16(propTopicAlias, alias))...)
	body = append(body, []byte(payload)...)
	return pkt(pktPUBLISH, body)
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

// TestV5New verifies New (the RELAY spec §7 Constructor Contract entry point)
// performs the same handshake as Dial and returns an mqtt.Client.
//
//fusa:test REQ-V5-CONN-002
func TestV5New(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := New(context.Background(), fb.addr(), WithClientID("n"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()
}

// TestV5NewRespectsContext verifies New aborts connection establishment
// promptly when the supplied ctx is already canceled, per RELAY spec §7 rule
// 3 (New MUST NOT block indefinitely).
//
//fusa:test REQ-V5-CONN-002
func TestV5NewRespectsContext(t *testing.T) {
	fb := newFakeBrokerV5(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := New(ctx, fb.addr(), WithClientID("n2"), WithKeepalive(0)); err == nil {
		t.Fatal("New with an already-canceled ctx succeeded, want error")
	}
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

// TestV5SubscribeReasonCodeFailure verifies SubscribeV5 surfaces a SUBACK
// failure Reason Code (§3.9, e.g. 0x87 Not authorized) as an error instead of
// silently returning a Subscription whose channel would never deliver — a
// regression test for go-mqtt-02 (SUBACK reason code was never inspected).
//
//fusa:test REQ-V5-SUB-001
func TestV5SubscribeReasonCodeFailure(t *testing.T) {
	fb := newFakeBrokerV5(t)
	fb.subAckReason = 0x87 // Not authorized
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("forbidden/topic", mqtt.AtMostOnce)
	if err == nil {
		t.Fatal("Subscribe with a refused SUBACK returned nil error, want an error")
	}
	if sub != nil {
		t.Errorf("Subscribe with a refused SUBACK returned non-nil Subscription: %v", sub)
	}
	if !errors.Is(err, mqtt.ErrSubscribeRefused) {
		t.Errorf("err = %v, want it to wrap mqtt.ErrSubscribeRefused", err)
	}

	// The refused subscription must not be left dangling in the client's
	// filter table — previously it stayed registered forever, its channel
	// never delivering and never closing until the whole client closed.
	c.mu.RLock()
	leftover := len(c.subs["forbidden/topic"])
	c.mu.RUnlock()
	if leftover != 0 {
		t.Errorf(`subs["forbidden/topic"] has %d entries after refusal, want 0`, leftover)
	}
}

// TestV5SubscribeSuccessReasonCode verifies SubscribeV5 accepts a granted
// SUBACK reason code (0x01 = Granted QoS 1, still < 0x80 so not a failure).
//
//fusa:test REQ-V5-SUB-001
func TestV5SubscribeSuccessReasonCode(t *testing.T) {
	fb := newFakeBrokerV5(t)
	fb.subAckReason = 0x01 // Granted QoS 1
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("granted/topic", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if sub == nil {
		t.Fatal("Subscribe returned a nil Subscription on a granted SUBACK")
	}
}

// TestV5SubscribeAckTimeout verifies SubscribeV5 fails with ErrTimeout, and
// cleans up its filter-table entry, when no SUBACK ever arrives.
//
//fusa:test REQ-V5-SUB-001
//fusa:test REQ-FAULT-001
func TestV5SubscribeAckTimeout(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.acceptNoAck(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0), WithAckTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("never/acked", mqtt.AtMostOnce)
	if !errors.Is(err, mqtt.ErrTimeout) {
		t.Errorf("err = %v, want it to wrap mqtt.ErrTimeout", err)
	}
	if sub != nil {
		t.Errorf("Subscribe with no SUBACK returned non-nil Subscription: %v", sub)
	}
	c.mu.RLock()
	leftover := len(c.subs["never/acked"])
	c.mu.RUnlock()
	if leftover != 0 {
		t.Errorf(`subs["never/acked"] has %d entries after timeout, want 0`, leftover)
	}
}

// TestV5PublishQoS1ReasonCodeFailure verifies PublishV5 (QoS 1) surfaces a
// PUBACK failure Reason Code (§3.4.2, e.g. 0x97 Quota exceeded) as an error
// instead of silently reporting success — a regression test for go-mqtt-02
// on the publish side.
//
//fusa:test REQ-V5-PUB-001
func TestV5PublishQoS1ReasonCodeFailure(t *testing.T) {
	fb := newFakeBrokerV5(t)
	fb.pubAckReason = 0x97 // Quota exceeded
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Publish(context.Background(), "t", mqtt.AtLeastOnce, []byte("x"))
	if !errors.Is(err, mqtt.ErrPublishRefused) {
		t.Errorf("err = %v, want it to wrap mqtt.ErrPublishRefused", err)
	}
}

// TestV5PublishQoS1AckTimeout verifies PublishV5 (QoS 1) fails with
// ErrTimeout when no PUBACK ever arrives.
//
//fusa:test REQ-V5-PUB-001
//fusa:test REQ-FAULT-001
func TestV5PublishQoS1AckTimeout(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.acceptNoAck(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0), WithAckTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Publish(context.Background(), "t", mqtt.AtLeastOnce, []byte("x"))
	if !errors.Is(err, mqtt.ErrTimeout) {
		t.Errorf("err = %v, want it to wrap mqtt.ErrTimeout", err)
	}
}

// TestV5TopicAliasZeroProtocolError verifies a PUBLISH carrying Topic Alias 0
// is treated as a Protocol Error (§3.3.2.3.4: 0 is never a valid alias) and
// closes the connection rather than being silently accepted or resolved — a
// regression test for go-mqtt-03.
//
//fusa:test REQ-V5-ALIAS-001
//fusa:sec-test REQ-SEC-009
func TestV5TopicAliasZeroProtocolError(t *testing.T) {
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

	// buildPUBLISH omits the Topic Alias property entirely when TopicAlias
	// == 0 (correct for the send path: 0 means "no alias"), so an explicit
	// alias-0 property — which a misbehaving/malicious broker could still
	// send — has to be constructed by hand here to exercise the parser.
	if _, err := fb.conn.Write(testPublishWithTopicAlias(t, "sensors/temp", "x", 0)); err != nil {
		t.Fatalf("write publish: %v", err)
	}

	select {
	case m, ok := <-sub.C():
		if ok {
			t.Errorf("expected the subscription channel to close on protocol error, got message %v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the connection to close after a Topic Alias 0 protocol error")
	}
}

// TestV5TopicAliasExceedsMaxProtocolError verifies a PUBLISH carrying a Topic
// Alias greater than what the client advertised via Topic Alias Maximum in
// CONNECT is a Protocol Error, not silently accepted — a regression test for
// go-mqtt-03.
//
//fusa:test REQ-V5-ALIAS-001
//fusa:sec-test REQ-SEC-009
func TestV5TopicAliasExceedsMaxProtocolError(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0), WithTopicAliasMax(4))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("sensors/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// alias 5 > advertised max of 4.
	if _, err := fb.conn.Write(buildPUBLISH("sensors/temp", []byte("x"), 0, false, 0, PublishProps{TopicAlias: 5})); err != nil {
		t.Fatalf("write publish: %v", err)
	}

	select {
	case m, ok := <-sub.C():
		if ok {
			t.Errorf("expected the subscription channel to close on protocol error, got message %v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the connection to close after an out-of-bound Topic Alias protocol error")
	}
}

// TestV5PublishTopicTooLarge is a regression test for go-mqtt-01: a topic
// longer than mqtt.MaxStringLen (65,535 bytes, the 2-byte length-prefix
// bound, §1.5.4) must be rejected with ErrPayloadTooLarge before it reaches
// encodeStr, not silently truncated (mod 65,536) into a misframed packet.
//
//fusa:test REQ-V5-WIRE-004
func TestV5PublishTopicTooLarge(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("big"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	hugeTopic := strings.Repeat("a", mqtt.MaxStringLen+1)
	err = c.Publish(context.Background(), hugeTopic, mqtt.AtMostOnce, []byte("x"))
	if !errors.Is(err, mqtt.ErrPayloadTooLarge) {
		t.Errorf("Publish with a %d-byte topic: err = %v, want ErrPayloadTooLarge", len(hugeTopic), err)
	}
}

// TestV5SubscribeTopicTooLarge mirrors TestV5PublishTopicTooLarge for the
// SUBSCRIBE topic filter, which is encoded the same way.
//
//fusa:test REQ-V5-WIRE-004
func TestV5SubscribeTopicTooLarge(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("big"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	hugeFilter := strings.Repeat("a", mqtt.MaxStringLen+1)
	if _, err := c.Subscribe(hugeFilter, mqtt.AtMostOnce); !errors.Is(err, mqtt.ErrPayloadTooLarge) {
		t.Errorf("Subscribe with a %d-byte filter: err = %v, want ErrPayloadTooLarge", len(hugeFilter), err)
	}
}

// TestV5PublishPropsFieldTooLarge is a regression test for the go-mqtt-01
// follow-up: PublishV5's ResponseTopic/ContentType/CorrelationData/
// UserProperties fields were left unguarded even after the topic itself was
// fixed. Each is encoded the same length-prefixed way (§1.5.4, §1.5.6) and
// must be rejected before buildPUBLISH truncates its length prefix.
//
//fusa:test REQ-V5-PUB-001
//fusa:test REQ-V5-PUB-002
func TestV5PublishPropsFieldTooLarge(t *testing.T) {
	huge := strings.Repeat("a", mqtt.MaxStringLen+1)
	hugeBin := make([]byte, mqtt.MaxStringLen+1)

	cases := []struct {
		name  string
		props PublishProps
	}{
		{"ResponseTopic", PublishProps{ResponseTopic: huge}},
		{"ContentType", PublishProps{ContentType: huge}},
		{"CorrelationData", PublishProps{CorrelationData: hugeBin}},
		{"UserProperty key", PublishProps{UserProperties: []mqtt.UserProperty{{Key: huge, Value: "v"}}}},
		{"UserProperty value", PublishProps{UserProperties: []mqtt.UserProperty{{Key: "k", Value: huge}}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fb := newFakeBrokerV5(t)
			go fb.accept(t, connack(0x00, nil))

			c, err := Dial(fb.addr(), WithClientID("props"), WithKeepalive(0))
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer func() { _ = c.Close() }()

			err = c.PublishV5(context.Background(), "t", mqtt.AtMostOnce, []byte("x"), tc.props)
			if !errors.Is(err, mqtt.ErrPayloadTooLarge) {
				t.Errorf("PublishV5 with oversized %s: err = %v, want ErrPayloadTooLarge", tc.name, err)
			}
		})
	}
}

// TestV5DialClientIDTooLarge verifies Dial rejects an oversized client ID
// before ever attempting a TCP connection, since the client ID is encoded
// the same length-prefixed way (§1.5.4).
//
//fusa:test REQ-V5-WIRE-004
//fusa:test REQ-V5-CONN-001
func TestV5DialClientIDTooLarge(t *testing.T) {
	hugeID := strings.Repeat("a", mqtt.MaxStringLen+1)
	if _, err := Dial("127.0.0.1:0", WithClientID(hugeID)); !errors.Is(err, mqtt.ErrPayloadTooLarge) {
		t.Errorf("Dial with a %d-byte client ID: err = %v, want ErrPayloadTooLarge", len(hugeID), err)
	}
}

// TestV5ReadLoopRejectsOversizedInboundFrame is a regression test for
// go-mqtt-04: readVarLen only enforces the wire-format ceiling
// (268,435,455, §2.2.3), which is not a resource-safety bound. Before this
// fix, the client allocated `make([]byte, remLen)` directly from an
// untrusted broker-declared length; a single crafted frame could force a
// ~268MB allocation. The client must now reject any inbound Remaining Length
// above its configured cap before allocating a body buffer at all — this
// test declares a length far above the default 1 MiB cap and confirms the
// client disconnects (and does not hang or panic) rather than allocating.
//
//fusa:test REQ-FAULT-001
//fusa:sec-test REQ-SEC-004
func TestV5ReadLoopRejectsOversizedInboundFrame(t *testing.T) {
	fb := newFakeBrokerV5(t)
	go fb.accept(t, connack(0x00, nil))

	c, err := Dial(fb.addr(), WithClientID("cap"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("t", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let the SUBACK settle

	// Declare a PUBLISH with Remaining Length just over the default 1 MiB
	// cap, but never write that many body bytes — if the client allocated
	// make([]byte, remLen) unconditionally and then blocked in
	// io.ReadFull, this test would hang instead of observing the
	// connection close below.
	if _, err := fb.conn.Write(append([]byte{pktPUBLISH}, encodeVarLen(2_000_000)...)); err != nil {
		t.Fatalf("write oversized frame header: %v", err)
	}

	select {
	case _, ok := <-sub.C():
		if ok {
			t.Error("expected the subscription channel to close after an oversized inbound frame, got a message instead")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the subscription channel to close after an oversized inbound frame")
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
