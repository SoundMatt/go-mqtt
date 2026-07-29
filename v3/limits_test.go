// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

// Regression test for a malicious/compromised broker forcing an unbounded
// pre-parse allocation: a packet declaring a remaining length beyond
// WithMaxMessageSize must be rejected (dropping the connection) before the
// client allocates a body buffer of that size.

import (
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

//fusa:test REQ-SEC-010
func TestReadLoopRejectsOversizedRemainingLength(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	fb.serve(t, func() {
		// Fixed header + a remaining-length varint encoding 300 (greater
		// than the 16-byte limit below), deliberately never followed by the
		// 300 declared body bytes — a successful read of this packet would
		// require actually allocating and blocking on a huge/nonexistent
		// body.
		_, _ = fb.conn.Write([]byte{pktPUBLISH, 0xAC, 0x02})
	})

	c, err := Dial(fb.addr(), WithClientID("m"), WithKeepalive(0), WithMaxMessageSize(16))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case _, ok := <-sub.C():
		if ok {
			t.Error("subscription channel delivered a message, want closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscription channel not closed after an oversized remaining length")
	}
}
