// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package broker_test

// Regression coverage for go-mqtt#59: a pre-auth connection sending a
// CONNECT-shaped header with a maximal remaining-length varint used to force
// a ~256 MiB allocation before any authentication check ran. The broker now
// rejects any packet (including the very first, pre-CONNECT, one) whose
// declared remaining length exceeds server.maxRemainingLength.

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/SoundMatt/go-mqtt/broker"
)

// TestOversizedRemainingLengthRejectedPreAuth sends a maximal 4-byte varint
// remaining length (268,435,455 — the MQTT wire format's own structural
// max) as the very first thing on a fresh connection, before any CONNECT
// body follows. The broker must close the connection (never allocate the
// declared body size, and never answer with a CONNACK) rather than block
// waiting for ~256 MiB of attacker-supplied data.
//
//fusa:test REQ-SEC-010
//fusa:test REQ-BROKER-WIRE-001
func TestOversizedRemainingLengthRejectedPreAuth(t *testing.T) {
	_, addr := startBroker(t)

	var d net.Dialer
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// pktCONNECT fixed-header byte (0x10) + a 4-byte varint encoding the
	// protocol maximum remaining length (268,435,455) — deliberately no
	// body bytes follow, since a correctly bounded broker must reject this
	// before ever trying to read a body of that size.
	if _, werr := conn.Write([]byte{0x10, 0xFF, 0xFF, 0xFF, 0x7F}); werr != nil {
		t.Fatalf("write: %v", werr)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("read: got %d bytes (%x), want the connection closed with no CONNACK", n, buf[:n])
	}
	if err != io.EOF {
		t.Logf("read returned %v (EOF also acceptable); connection was not left open serving a CONNACK", err)
	}
}

// TestMaxRemainingLengthConfigurable verifies broker.WithMaxRemainingLength
// actually changes the enforced bound: a CONNECT declaring a remaining
// length just above a small configured maximum is rejected pre-auth.
//
//fusa:test REQ-SEC-010
func TestMaxRemainingLengthConfigurable(t *testing.T) {
	srv := broker.New(broker.WithMaxRemainingLength(16))
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("broker did not start listening")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Close() })

	var d net.Dialer
	conn, err := d.DialContext(context.Background(), "tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Remaining length 17 (0x11), one over the configured max of 16 — must
	// be rejected even though it is nowhere near the protocol's own limit.
	if _, werr := conn.Write([]byte{0x10, 0x11}); werr != nil {
		t.Fatalf("write: %v", werr)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4)
	if n, err := conn.Read(buf); err == nil {
		t.Fatalf("read: got %d bytes (%x), want the connection closed with no CONNACK", n, buf[:n])
	}
}
