// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build integration

package v5_test

import (
	"context"
	"os"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/v5"
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
	c, err := v5.Dial(brokerAddr(t), v5.WithClientID("go-mqtt-v5-integ-connect"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
}

func TestIntegration_PublishSubscribeQoS0(t *testing.T) {
	addr := brokerAddr(t)
	sub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-sub0"))
	if err != nil {
		t.Fatalf("Dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-pub0"))
	if err != nil {
		t.Fatalf("Dial pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	topic := "go-mqtt/v5/integ/qos0"
	subscription, err := sub.Subscribe(topic, mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond) // allow SUBACK to arrive

	ctx := context.Background()
	if err := pub.Publish(ctx, topic, mqtt.AtMostOnce, []byte("hello-v5")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-subscription.C():
		if string(msg.Payload) != "hello-v5" {
			t.Errorf("payload: got %q, want %q", msg.Payload, "hello-v5")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestIntegration_PublishSubscribeQoS1(t *testing.T) {
	addr := brokerAddr(t)
	sub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-sub1"))
	if err != nil {
		t.Fatalf("Dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-pub1"))
	if err != nil {
		t.Fatalf("Dial pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	topic := "go-mqtt/v5/integ/qos1"
	subscription, err := sub.Subscribe(topic, mqtt.AtLeastOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond)

	if err := pub.Publish(context.Background(), topic, mqtt.AtLeastOnce, []byte("acked")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-subscription.C():
		if string(msg.Payload) != "acked" {
			t.Errorf("payload: got %q", msg.Payload)
		}
		if msg.QoS != mqtt.AtLeastOnce {
			t.Errorf("QoS: got %d, want 1", msg.QoS)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestIntegration_PublishV5_UserProperties(t *testing.T) {
	addr := brokerAddr(t)
	sub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-usub"))
	if err != nil {
		t.Fatalf("Dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-upub"))
	if err != nil {
		t.Fatalf("Dial pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	topic := "go-mqtt/v5/integ/userprops"
	subscription, err := sub.Subscribe(topic, mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond)

	props := v5.PublishProps{
		ResponseTopic:   "reply/" + topic,
		CorrelationData: []byte("req-001"),
		UserProperties: []mqtt.UserProperty{
			{Key: "unit", Value: "km/h"},
		},
		ContentType: "application/json",
	}
	if err := pub.PublishV5(context.Background(), topic, mqtt.AtMostOnce, []byte(`{"speed":80}`), props); err != nil {
		t.Fatalf("PublishV5: %v", err)
	}

	select {
	case msg := <-subscription.C():
		if msg.ResponseTopic != props.ResponseTopic {
			t.Errorf("response topic: got %q, want %q", msg.ResponseTopic, props.ResponseTopic)
		}
		if string(msg.CorrelationData) != "req-001" {
			t.Errorf("correlation data: got %q", msg.CorrelationData)
		}
		if len(msg.UserProperties) == 0 || msg.UserProperties[0].Key != "unit" {
			t.Errorf("user properties: got %v", msg.UserProperties)
		}
		if msg.ContentType != "application/json" {
			t.Errorf("content type: got %q", msg.ContentType)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestIntegration_WildcardSubscription(t *testing.T) {
	addr := brokerAddr(t)
	sub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-wsub"))
	if err != nil {
		t.Fatalf("Dial sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-wpub"))
	if err != nil {
		t.Fatalf("Dial pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	subscription, err := sub.Subscribe("go-mqtt/v5/integ/wild/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond)

	topics := []string{
		"go-mqtt/v5/integ/wild/a",
		"go-mqtt/v5/integ/wild/b/c",
	}
	for _, tpc := range topics {
		if err := pub.Publish(context.Background(), tpc, mqtt.AtMostOnce, []byte(tpc)); err != nil {
			t.Fatalf("Publish %s: %v", tpc, err)
		}
	}

	received := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for len(received) < len(topics) {
		select {
		case msg := <-subscription.C():
			received[msg.Topic] = true
		case <-timeout:
			t.Fatalf("timeout; received %d/%d topics: %v", len(received), len(topics), received)
		}
	}
}

func TestIntegration_SubscribeV5_NoLocal(t *testing.T) {
	addr := brokerAddr(t)
	// NoLocal: the publisher should not receive its own messages.
	c, err := v5.Dial(addr, v5.WithClientID("go-mqtt-v5-integ-nolocal"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	topic := "go-mqtt/v5/integ/nolocal"
	subscription, err := c.SubscribeV5(topic, mqtt.AtMostOnce, v5.SubscribeOpts{NoLocal: true})
	if err != nil {
		t.Fatalf("SubscribeV5: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	time.Sleep(50 * time.Millisecond)

	if err := c.Publish(context.Background(), topic, mqtt.AtMostOnce, []byte("self")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-subscription.C():
		t.Errorf("NoLocal: received own message: %q", msg.Payload)
	case <-time.After(500 * time.Millisecond):
		// Expected: no message delivered
	}
}

func TestIntegration_ClosedClientErrors(t *testing.T) {
	c, err := v5.Dial(brokerAddr(t), v5.WithClientID("go-mqtt-v5-integ-closed"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Publish(context.Background(), "t", mqtt.AtMostOnce, nil); err != mqtt.ErrClosed {
		t.Errorf("Publish after Close: got %v, want ErrClosed", err)
	}
	if _, err := c.Subscribe("t", mqtt.AtMostOnce); err != mqtt.ErrClosed {
		t.Errorf("Subscribe after Close: got %v, want ErrClosed", err)
	}
}
