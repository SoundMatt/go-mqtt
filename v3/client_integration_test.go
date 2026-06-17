// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build integration

package v3_test

import (
	"context"
	"os"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/v3"
)

func brokerAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("MQTT_BROKER")
	if addr == "" {
		addr = "localhost:1883"
	}
	return addr
}

func TestIntegration_ConnectDisconnect(t *testing.T) {
	c, err := v3.Dial(brokerAddr(t), v3.WithClientID("go-mqtt-v3-integ-connect"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
}

func TestIntegration_PublishSubscribeQoS2(t *testing.T) {
	addr := brokerAddr(t)

	sub, err := v3.Dial(addr, v3.WithClientID("go-mqtt-v3-integ-sub2"))
	if err != nil {
		t.Fatalf("Dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := v3.Dial(addr, v3.WithClientID("go-mqtt-v3-integ-pub2"))
	if err != nil {
		t.Fatalf("Dial pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	topic := "go-mqtt/v3/integ/qos2"
	subscription, err := sub.Subscribe(topic, mqtt.ExactlyOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond) // allow SUBACK to arrive

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, topic, mqtt.ExactlyOnce, []byte("exactly-once")); err != nil {
		t.Fatalf("Publish QoS 2: %v", err)
	}

	select {
	case msg := <-subscription.C():
		if string(msg.Payload) != "exactly-once" {
			t.Errorf("payload: got %q, want %q", msg.Payload, "exactly-once")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for QoS 2 message")
	}

	// No duplicate must arrive.
	select {
	case msg := <-subscription.C():
		t.Errorf("unexpected duplicate QoS 2 delivery: %q", msg.Payload)
	case <-time.After(300 * time.Millisecond):
	}
}
