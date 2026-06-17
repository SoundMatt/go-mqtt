// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package broker

// Internal tests that drive the broker over a raw connection to exercise
// features the v3 client cannot request: retained publishes and last-will.

import (
	"context"
	"net"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/v3"
)

func rawStartBroker(t *testing.T) (*Server, string) {
	t.Helper()
	srv := New()
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("broker did not start")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, srv.Addr()
}

// rawConnect opens a TCP connection and sends a CONNECT, optionally with a will,
// then reads the CONNACK.
func rawConnect(t *testing.T, addr, clientID string, w *willMsg) net.Conn {
	t.Helper()
	var d net.Dialer
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}

	flags := byte(0x02) // CleanSession
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04}
	if w != nil {
		flags |= 0x04 | (w.qos << 3)
		if w.retain {
			flags |= 0x20
		}
	}
	body = append(body, flags, 0x00, 0x00) // flags + keepalive 0
	body = append(body, encodeStr(clientID)...)
	if w != nil {
		body = append(body, encodeStr(w.topic)...)
		body = append(body, encodeU16(uint16(len(w.payload)))...)
		body = append(body, w.payload...)
	}
	if _, werr := conn.Write(packet(pktCONNECT, body)); werr != nil {
		t.Fatalf("raw CONNECT: %v", werr)
	}
	hdr, _, err := readPacket(conn)
	if err != nil || hdr&0xF0 != pktCONNACK&0xF0 {
		t.Fatalf("raw CONNACK: hdr=0x%02x err=%v", hdr, err)
	}
	return conn
}

func TestRetainedReplayedOnSubscribe(t *testing.T) {
	_, addr := rawStartBroker(t)

	// Raw publisher sets the retain flag on a QoS 0 PUBLISH.
	pub := rawConnect(t, addr, "raw-pub", nil)
	if _, err := pub.Write(buildPUBLISH("r/temp", []byte("21.5"), 0, true, 0)); err != nil {
		t.Fatalf("raw PUBLISH retain: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	_ = pub.Close()

	// A later subscriber (via the v3 client) receives the retained message.
	sub, err := v3.Dial(addr, v3.WithClientID("ret-sub"), v3.WithKeepalive(0))
	if err != nil {
		t.Fatalf("dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	s, err := sub.Subscribe("r/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-s.C():
		if string(msg.Payload) != "21.5" {
			t.Errorf("payload = %q, want 21.5", msg.Payload)
		}
		if !msg.Retained {
			t.Error("Retained = false, want true for a replayed retained message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for retained message")
	}
}

func TestWillPublishedOnAbruptDisconnect(t *testing.T) {
	_, addr := rawStartBroker(t)

	// Subscriber for the will topic.
	sub, err := v3.Dial(addr, v3.WithClientID("will-sub"), v3.WithKeepalive(0))
	if err != nil {
		t.Fatalf("dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	s, err := sub.Subscribe("clients/+/status", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// A client connects with a will, then drops its TCP connection abruptly
	// (no DISCONNECT) — the broker must publish the will.
	w := &willMsg{topic: "clients/sensor1/status", payload: []byte("offline"), qos: 0}
	victim := rawConnect(t, addr, "victim", w)
	_ = victim.Close() // abrupt: no DISCONNECT packet sent

	select {
	case msg := <-s.C():
		if msg.Topic != "clients/sensor1/status" {
			t.Errorf("topic = %q, want clients/sensor1/status", msg.Topic)
		}
		if string(msg.Payload) != "offline" {
			t.Errorf("payload = %q, want offline", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for will message")
	}
}

func TestCleanDisconnectSuppressesWill(t *testing.T) {
	_, addr := rawStartBroker(t)

	sub, err := v3.Dial(addr, v3.WithClientID("will-sub2"), v3.WithKeepalive(0))
	if err != nil {
		t.Fatalf("dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	s, err := sub.Subscribe("clients/+/status", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// Connect with a will, then disconnect cleanly: the will must NOT fire.
	w := &willMsg{topic: "clients/sensor2/status", payload: []byte("offline"), qos: 0}
	c := rawConnect(t, addr, "clean", w)
	if _, err := c.Write([]byte{pktDISCONNECT, 0x00}); err != nil {
		t.Fatalf("DISCONNECT: %v", err)
	}
	_ = c.Close()

	select {
	case msg := <-s.C():
		t.Errorf("will fired on clean disconnect: %q", msg.Payload)
	case <-time.After(300 * time.Millisecond):
		// Good: clean disconnect suppressed the will.
	}
}
