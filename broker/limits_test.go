// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package broker

// Regression tests for the pre-auth memory-exhaustion DoS fixed by bounding
// readPacket's remaining-length allocation, and for the accompanying
// defense-in-depth idle-timeout and connection-cap options.

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// TestReadPacketRejectsOversizedRemainingLength proves readPacket rejects a
// remaining length beyond maxRemaining *before* allocating (or attempting to
// read) the body: the fake reader below supplies only the fixed header and
// the remaining-length varint, never the 300 declared body bytes, so a
// successful read here would block or fail with io.ErrUnexpectedEOF/EOF
// rather than the expected ErrPayloadTooLarge.
//
//fusa:test REQ-SEC-010
func TestReadPacketRejectsOversizedRemainingLength(t *testing.T) {
	// Fixed header (CONNECT) + remaining-length varint encoding 300, with no
	// body bytes at all.
	r := bytes.NewReader([]byte{pktCONNECT, 0xAC, 0x02})
	_, _, err := readPacket(r, 16)
	if err == nil {
		t.Fatal("readPacket: got nil error, want ErrPayloadTooLarge")
	}
	if !errors.Is(err, mqtt.ErrPayloadTooLarge) {
		t.Errorf("readPacket error = %v, want errors.Is(_, mqtt.ErrPayloadTooLarge)", err)
	}
}

// TestReadPacketZeroMaxIsUnbounded confirms maxRemaining <= 0 preserves the
// pre-fix unbounded behaviour (for callers that explicitly opt out, e.g. the
// raw test helper reading a CONNACK of unknown size).
func TestReadPacketZeroMaxIsUnbounded(t *testing.T) {
	r := bytes.NewReader([]byte{pktCONNACK, 0x02, 0x00, 0x00})
	hdr, body, err := readPacket(r, 0)
	if err != nil {
		t.Fatalf("readPacket: %v", err)
	}
	if hdr != pktCONNACK || len(body) != 2 {
		t.Errorf("readPacket = (0x%02x, %v), want (0x%02x, len 2)", hdr, body, pktCONNACK)
	}
}

// TestServerRejectsOversizedConnect drives the fix end-to-end over a real
// TCP connection: a broker configured with a small WithMaxMessageSize closes
// the connection (never answers CONNACK) when the CONNECT declares a
// remaining length beyond the limit — the pre-auth path from the reported
// DoS.
//
//fusa:test REQ-SEC-010
func TestServerRejectsOversizedConnect(t *testing.T) {
	srv := New(WithMaxMessageSize(16))
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("broker did not start")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Close() })

	var d net.Dialer
	conn, err := d.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// CONNECT fixed header, remaining length encoded as 300 (> the 16-byte
	// limit) — deliberately never sends the 300 declared body bytes.
	if _, werr := conn.Write([]byte{pktCONNECT, 0xAC, 0x02}); werr != nil {
		t.Fatalf("write: %v", werr)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("read: got %d bytes (%v), want the connection closed with no CONNACK", n, buf[:n])
	}
}

// TestServerIdleTimeoutClosesConnection confirms a connection that never
// completes a packet is closed once WithIdleTimeout elapses, rather than
// held open indefinitely.
//
//fusa:test REQ-BROKER-011
func TestServerIdleTimeoutClosesConnection(t *testing.T) {
	srv := New(WithIdleTimeout(50 * time.Millisecond))
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("broker did not start")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Close() })

	var d net.Dialer
	conn, err := d.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, rerr := conn.Read(buf); rerr == nil {
		t.Fatal("read: got data, want the idle connection closed by the broker")
	}
}

// TestServerMaxConnections confirms a connection accepted beyond
// WithMaxConnections is closed immediately, before any CONNECT is processed.
func TestServerMaxConnections(t *testing.T) {
	srv := New(WithMaxConnections(1))
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("broker did not start")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Close() })

	var d net.Dialer
	first, err := d.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}
	defer func() { _ = first.Close() }()
	// Hold the first connection open without completing CONNECT so it still
	// counts against the cap.
	time.Sleep(20 * time.Millisecond)

	second, err := d.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer func() { _ = second.Close() }()

	_ = second.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, rerr := second.Read(buf); rerr == nil {
		t.Fatal("read: got data on the over-cap connection, want it closed immediately")
	}
}
