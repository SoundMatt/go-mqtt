// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

// Guards against a "send on closed channel" panic when deliver() races
// closeOnce() (go-mqtt#60). Before the fix, deliver sent on sub.ch without
// holding sub.mu, so a concurrent Close/closeOnce could close the channel
// between deliver's liveness check (if any) and its send.

import (
	"sync"
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// TestSubscriptionDeliverCloseRace runs deliver concurrently with closeOnce
// many times under -race: it must never panic ("send on closed channel")
// and the race detector must report no data race on sub.ch/sub.closed.
//
//fusa:test REQ-CONC-003
func TestSubscriptionDeliverCloseRace(t *testing.T) {
	const iterations = 2000
	for i := 0; i < iterations; i++ {
		sub := &v3Subscription{filter: "t/#", ch: make(chan mqtt.Message, 1)}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			sub.deliver(mqtt.Message{Topic: "t/1"})
		}()
		go func() {
			defer wg.Done()
			sub.closeOnce()
		}()
		wg.Wait()
	}
}

// TestClientDeliverCloseRace exercises the same race through c.deliver and
// c.Close (the real caller paths), rather than calling v3Subscription's
// methods directly.
//
//fusa:test REQ-CONC-003
func TestClientDeliverCloseRace(t *testing.T) {
	const iterations = 500
	for i := 0; i < iterations; i++ {
		c := &v3Client{
			subs: map[string][]*v3Subscription{},
			done: make(chan struct{}),
		}
		sub := &v3Subscription{filter: "t/#", ch: make(chan mqtt.Message, 1), client: c}
		c.subs[sub.filter] = []*v3Subscription{sub}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.deliver("t/1", mqtt.Message{Topic: "t/1"})
		}()
		go func() {
			defer wg.Done()
			sub.closeOnce()
		}()
		wg.Wait()
	}
}
