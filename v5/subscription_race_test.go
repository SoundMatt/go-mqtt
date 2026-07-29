// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v5

// Regression test for a deliver() vs Close() data race: deliver() must never
// send on sub.ch after closeOnce() has closed it. Run with `go test -race`
// to catch the underlying data race as well as the send-on-closed-channel
// panic it can cause.

import (
	"sync"
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
)

//fusa:test REQ-CONC-003
func TestV5SubscriptionDeliverCloseRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		sub := &v5Subscription{ch: make(chan mqtt.Message, 1)}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			sub.deliver(mqtt.Message{Topic: "t", Payload: []byte("p")})
		}()
		go func() {
			defer wg.Done()
			sub.closeOnce()
		}()
		wg.Wait()
	}
}
