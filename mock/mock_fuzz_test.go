// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mock_test

import (
	"context"
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/mock"
)

// FuzzPublish verifies that Publish does not panic on arbitrary topic/payload.
func FuzzPublish(f *testing.F) {
	f.Add("sensors/temperature", []byte(`{"temp":21}`))
	f.Add("Vehicle/Speed", []byte(`{"speed":60}`))
	f.Add("", []byte{})
	f.Add("a/b/c", []byte("hello"))

	f.Fuzz(func(t *testing.T, topic string, payload []byte) {
		b := mock.New()
		c := b.Dial()
		defer c.Close()

		ctx := context.Background()
		// Must not panic — ErrTopicEmpty is expected for empty topic.
		_ = c.Publish(ctx, topic, mqtt.AtMostOnce, payload)
	})
}
