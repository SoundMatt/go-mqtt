// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

// Regression coverage for go-mqtt#59: a malicious/compromised broker used to
// be able to force an unbounded (up to ~256 MiB) allocation in the client's
// readLoop by sending a packet header declaring a maximal remaining length.

import (
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// TestReadLoopRejectsOversizedRemainingLength has the fake broker send a
// PUBLISH-shaped header with a remaining length above the client's
// configured maximum, with no body following. A correctly bounded client
// must stop reading (closing every live subscription) rather than block
// trying to read a body of that declared size.
//
//fusa:test REQ-SEC-010
func TestReadLoopRejectsOversizedRemainingLength(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	done := make(chan struct{})
	fb.serve(t, func() {
		// pktPUBLISH (QoS 0) + a 4-byte varint remaining length of
		// 268,435,455 (the MQTT wire format's own structural maximum) —
		// deliberately no body bytes follow.
		_, _ = fb.conn.Write([]byte{pktPUBLISH, 0xFF, 0xFF, 0xFF, 0x7F})
		close(done)
	})

	c, err := Dial(fb.addr(), WithClientID("oversized"), WithKeepalive(0), WithMaxRemainingLength(1<<20))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("t/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fake broker never finished writing the oversized header")
	}

	select {
	case _, ok := <-sub.C():
		if ok {
			t.Fatal("received a message; want the subscription channel closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop did not close the subscription after an oversized remaining length")
	}
}
