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

func BenchmarkPublish(b *testing.B) {
	broker := mock.New()
	c := broker.Dial()
	defer c.Close()

	sub, _ := c.Subscribe("bench/topic", mqtt.AtMostOnce, mqtt.WithChannelDepth(b.N+1))
	defer sub.Close()

	payload := []byte(`{"value":42}`)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_ = c.Publish(ctx, "bench/topic", mqtt.AtMostOnce, payload)
	}
}

func BenchmarkPublishParallel(b *testing.B) {
	broker := mock.New()
	c := broker.Dial()
	defer c.Close()

	payload := []byte(`{"value":42}`)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.Publish(ctx, "bench/parallel", mqtt.AtMostOnce, payload)
		}
	})
}

func BenchmarkMatchTopic(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = mqtt.MatchTopic("Vehicle/+/Speed", "Vehicle/Car1/Speed")
	}
}
