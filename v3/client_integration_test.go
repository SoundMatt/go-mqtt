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

func wsBrokerURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("MQTT_WS_BROKER")
	if u == "" {
		u = "ws://localhost:9001/"
	}
	return u
}

func TestIntegration_WebSocketPublishSubscribe(t *testing.T) {
	url := wsBrokerURL(t)

	sub, err := v3.DialWS(url, v3.WithClientID("go-mqtt-v3-integ-ws-sub"))
	if err != nil {
		t.Fatalf("DialWS sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := v3.DialWS(url, v3.WithClientID("go-mqtt-v3-integ-ws-pub"))
	if err != nil {
		t.Fatalf("DialWS pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	topic := "go-mqtt/v3/integ/ws"
	subscription, err := sub.Subscribe(topic, mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, topic, mqtt.AtMostOnce, []byte("over-websocket")); err != nil {
		t.Fatalf("Publish over WS: %v", err)
	}

	select {
	case msg := <-subscription.C():
		if string(msg.Payload) != "over-websocket" {
			t.Errorf("payload: got %q, want over-websocket", msg.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for WS message")
	}
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

// TestIntegration_FieldExactRoundTrip publishes via go-mqtt's own v3.Client
// and verifies the message the go-mqtt v3.Client subscriber receives is
// field-exact — Topic, Payload, and QoS all match what was published — over
// genuine MQTT wire traffic to a real Mosquitto broker (not the mock
// in-process broker used by the rest of this repo's unit tests). This is
// the "real-broker round-trip" half of interop testing; see
// client_thirdparty_interop_test.go for the third-party-peer half
// (mosquitto_pub/mosquitto_sub cross-checks).
func TestIntegration_FieldExactRoundTrip(t *testing.T) {
	addr := brokerAddr(t)

	sub, err := v3.Dial(addr, v3.WithClientID("go-mqtt-v3-integ-fieldexact-sub"))
	if err != nil {
		t.Fatalf("Dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := v3.Dial(addr, v3.WithClientID("go-mqtt-v3-integ-fieldexact-pub"))
	if err != nil {
		t.Fatalf("Dial pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	topic := "go-mqtt/v3/integ/fieldexact"
	subscription, err := sub.Subscribe(topic, mqtt.AtLeastOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond) // allow SUBACK to arrive

	wantPayload := []byte(`{"speed":123,"unit":"km/h"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, topic, mqtt.AtLeastOnce, wantPayload); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-subscription.C():
		if msg.Topic != topic {
			t.Errorf("Topic: got %q, want %q", msg.Topic, topic)
		}
		if string(msg.Payload) != string(wantPayload) {
			t.Errorf("Payload: got %q, want %q", msg.Payload, wantPayload)
		}
		if msg.QoS != mqtt.AtLeastOnce {
			t.Errorf("QoS: got %v, want %v", msg.QoS, mqtt.AtLeastOnce)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for field-exact round-trip message")
	}
}
