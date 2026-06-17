// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

//fusa:req REQ-QOS2-001
//fusa:req REQ-QOS2-002
//fusa:req REQ-QOS2-003
//fusa:req REQ-QOS2-004

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// ── Packet builder unit tests ─────────────────────────────────────────────────

func TestBuildPUBREC(t *testing.T) {
	pkt := buildPUBREC(0x1234)
	want := []byte{pktPUBREC, 0x02, 0x12, 0x34}
	if !bytes.Equal(pkt, want) {
		t.Errorf("buildPUBREC = %x, want %x", pkt, want)
	}
}

func TestBuildPUBREL(t *testing.T) {
	pkt := buildPUBREL(0x1234)
	// PUBREL fixed header MUST carry reserved flags 0b0010 → 0x62.
	want := []byte{pktPUBREL, 0x02, 0x12, 0x34}
	if !bytes.Equal(pkt, want) {
		t.Errorf("buildPUBREL = %x, want %x", pkt, want)
	}
	if pkt[0] != 0x62 {
		t.Errorf("PUBREL header = 0x%02x, want 0x62 (flags 0b0010)", pkt[0])
	}
}

func TestBuildPUBCOMP(t *testing.T) {
	pkt := buildPUBCOMP(0x1234)
	want := []byte{pktPUBCOMP, 0x02, 0x12, 0x34}
	if !bytes.Equal(pkt, want) {
		t.Errorf("buildPUBCOMP = %x, want %x", pkt, want)
	}
}

// ── Fake-broker handshake tests ───────────────────────────────────────────────

// fakeBroker is a minimal in-process TCP server that speaks just enough of the
// MQTT v3.1.1 wire protocol to exercise the client's QoS 2 handshake.
type fakeBroker struct {
	ln   net.Listener
	conn net.Conn
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &fakeBroker{ln: ln}
}

func (fb *fakeBroker) addr() string { return fb.ln.Addr().String() }

// serve accepts one client, completes the CONNECT/CONNACK handshake, and then
// invokes handler on the same goroutine so the connection has a single owner.
func (fb *fakeBroker) serve(t *testing.T, handler func()) {
	t.Helper()
	go func() {
		conn, err := fb.ln.Accept()
		if err != nil {
			return
		}
		fb.conn = conn
		fb.readPacket(t) // CONNECT
		if _, err := conn.Write([]byte{pktCONNACK, 0x02, 0x00, 0x00}); err != nil {
			return
		}
		handler()
	}()
}

// readPacket reads one full MQTT packet and returns its fixed-header byte and body.
func (fb *fakeBroker) readPacket(t *testing.T) (byte, []byte) {
	t.Helper()
	var hdr [1]byte
	if _, err := io.ReadFull(fb.conn, hdr[:]); err != nil {
		return 0, nil
	}
	remLen, err := readVarLen(fb.conn)
	if err != nil {
		return 0, nil
	}
	body := make([]byte, remLen)
	if remLen > 0 {
		if _, err := io.ReadFull(fb.conn, body); err != nil {
			return 0, nil
		}
	}
	return hdr[0], body
}

func (fb *fakeBroker) close() {
	if fb.conn != nil {
		_ = fb.conn.Close()
	}
	_ = fb.ln.Close()
}

// packetID extracts the trailing 2-byte packet ID from an ack-style packet
// body (PUBREC, PUBREL, PUBCOMP, PUBACK), where the body is exactly the ID.
func packetID(body []byte) uint16 {
	if len(body) < 2 {
		return 0
	}
	n := len(body)
	return uint16(body[n-2])<<8 | uint16(body[n-1])
}

// publishPacketID extracts the packet ID from a QoS>0 PUBLISH body, which lies
// immediately after the topic (2-byte length prefix + topic bytes).
func publishPacketID(body []byte) uint16 {
	if len(body) < 2 {
		return 0
	}
	topicLen := int(body[0])<<8 | int(body[1])
	off := 2 + topicLen
	if len(body) < off+2 {
		return 0
	}
	return uint16(body[off])<<8 | uint16(body[off+1])
}

// TestQoS2PublishHandshake verifies the outbound QoS 2 flow:
// PUBLISH → PUBREC → PUBREL → PUBCOMP, with Publish returning nil on success.
//
//fusa:req REQ-QOS2-001
//fusa:req REQ-QOS2-002
func TestQoS2PublishHandshake(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	brokerDone := make(chan error, 1)
	fb.serve(t, func() {
		// Expect PUBLISH (QoS 2).
		hdr, body := fb.readPacket(t)
		if hdr&0xF0 != pktPUBLISH&0xF0 {
			brokerDone <- errExpected("PUBLISH", hdr)
			return
		}
		if (hdr>>1)&0x03 != 2 {
			brokerDone <- errors.New("PUBLISH QoS != 2")
			return
		}
		id := publishPacketID(body)
		// Send PUBREC.
		_, _ = fb.conn.Write(buildPUBREC(id))
		// Expect PUBREL.
		hdr2, body2 := fb.readPacket(t)
		if hdr2 != pktPUBREL {
			brokerDone <- errExpected("PUBREL", hdr2)
			return
		}
		if packetID(body2) != id {
			brokerDone <- errors.New("PUBREL packet ID mismatch")
			return
		}
		// Send PUBCOMP.
		_, _ = fb.conn.Write(buildPUBCOMP(id))
		brokerDone <- nil
	})

	c, err := Dial(fb.addr(), WithClientID("qos2-pub"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if err := c.Publish(ctx, "a/b", mqtt.ExactlyOnce, []byte("exactly-once")); err != nil {
		t.Fatalf("Publish QoS 2: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker handshake: %v", err)
	}
}

// TestQoS2PublishTimeout verifies that Publish returns ErrTimeout if the broker
// never sends PUBREC.
//
//fusa:req REQ-QOS2-004
func TestQoS2PublishTimeout(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	// Broker reads the PUBLISH but never answers.
	fb.serve(t, func() { fb.readPacket(t) })

	c, err := Dial(fb.addr(), WithClientID("qos2-timeout"),
		WithKeepalive(0), WithQoS2Timeout(150*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Publish(context.Background(), "a/b", mqtt.ExactlyOnce, []byte("x"))
	if !errors.Is(err, mqtt.ErrTimeout) {
		t.Errorf("Publish QoS 2 with no PUBREC: got %v, want ErrTimeout", err)
	}
}

// TestQoS2PublishContextCancel verifies that a cancelled context aborts the
// QoS 2 handshake.
//
//fusa:req REQ-QOS2-004
func TestQoS2PublishContextCancel(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	fb.serve(t, func() { fb.readPacket(t) })

	c, err := Dial(fb.addr(), WithClientID("qos2-cancel"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err = c.Publish(ctx, "a/b", mqtt.ExactlyOnce, []byte("x"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Publish QoS 2 with cancelled ctx: got %v, want context.Canceled", err)
	}
}

// TestQoS2InboundDelivery verifies the inbound QoS 2 flow: the client receives a
// QoS 2 PUBLISH, answers PUBREC, and delivers the message exactly once on PUBREL.
//
//fusa:req REQ-QOS2-003
func TestQoS2InboundDelivery(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	// Broker drives an inbound QoS 2 PUBLISH. It reports its outcome on
	// brokerDone so the test can wait for PUBCOMP before tearing down.
	const id uint16 = 0x0007
	brokerDone := make(chan error, 1)
	fb.serve(t, func() {
		// Discard the SUBSCRIBE packet.
		fb.readPacket(t)
		// Send a QoS 2 PUBLISH.
		_, _ = fb.conn.Write(buildPUBLISH("a/b", []byte("inbound"), 2, false, id))
		// Expect PUBREC.
		hdr, body := fb.readPacket(t)
		if hdr&0xF0 != pktPUBREC&0xF0 || packetID(body) != id {
			brokerDone <- errExpected("PUBREC", hdr)
			return
		}
		// Send PUBREL → client should deliver and answer PUBCOMP.
		_, _ = fb.conn.Write(buildPUBREL(id))
		hdr2, body2 := fb.readPacket(t)
		if hdr2&0xF0 != pktPUBCOMP&0xF0 || packetID(body2) != id {
			brokerDone <- errExpected("PUBCOMP", hdr2)
			return
		}
		brokerDone <- nil
	})

	c, err := Dial(fb.addr(), WithClientID("qos2-inbound"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("a/#", mqtt.ExactlyOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	select {
	case msg := <-sub.C():
		if string(msg.Payload) != "inbound" {
			t.Errorf("payload = %q, want %q", msg.Payload, "inbound")
		}
		if msg.QoS != mqtt.ExactlyOnce {
			t.Errorf("QoS = %v, want ExactlyOnce", msg.QoS)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for QoS 2 inbound delivery")
	}

	// Wait for the broker to confirm it received PUBCOMP before teardown.
	select {
	case err := <-brokerDone:
		if err != nil {
			t.Fatalf("broker handshake: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broker PUBCOMP")
	}
}

// TestQoS2InboundDedup verifies that a retransmitted QoS 2 PUBLISH (same packet
// ID) is delivered only once.
//
//fusa:req REQ-QOS2-003
func TestQoS2InboundDedup(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	const id uint16 = 0x0009
	brokerDone := make(chan struct{})
	fb.serve(t, func() {
		defer close(brokerDone)
		fb.readPacket(t) // discard SUBSCRIBE
		// Send the same QoS 2 PUBLISH twice before PUBREL (duplicate delivery).
		_, _ = fb.conn.Write(buildPUBLISH("a/b", []byte("once"), 2, false, id))
		fb.readPacket(t) // PUBREC #1
		_, _ = fb.conn.Write(buildPUBLISH("a/b", []byte("once"), 2, false, id))
		fb.readPacket(t) // PUBREC #2
		// Single PUBREL releases the (deduped) message.
		_, _ = fb.conn.Write(buildPUBREL(id))
		fb.readPacket(t) // PUBCOMP
	})

	c, err := Dial(fb.addr(), WithClientID("qos2-dedup"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("a/#", mqtt.ExactlyOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Expect exactly one delivery.
	select {
	case msg := <-sub.C():
		if string(msg.Payload) != "once" {
			t.Errorf("payload = %q, want %q", msg.Payload, "once")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	select {
	case msg := <-sub.C():
		t.Errorf("unexpected second delivery: %q", msg.Payload)
	case <-time.After(200 * time.Millisecond):
		// Good — no duplicate.
	}

	// Drain the broker goroutine before teardown.
	<-brokerDone
}

func errExpected(want string, got byte) error {
	return errors.New("expected " + want + ", got header 0x" + string(hexByte(got)))
}

func hexByte(b byte) []byte {
	const hexdigits = "0123456789abcdef"
	return []byte{hexdigits[b>>4], hexdigits[b&0x0F]}
}
