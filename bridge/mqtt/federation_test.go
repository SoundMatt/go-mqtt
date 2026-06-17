// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqttbridge_test

import (
	"context"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	mqttbridge "github.com/SoundMatt/go-mqtt/bridge/mqtt"
	"github.com/SoundMatt/go-mqtt/mock"
)

// recvTimeout waits for one message on sub or fails after d.
func recvTimeout(t *testing.T, sub mqtt.Subscription, d time.Duration) mqtt.Message {
	t.Helper()
	select {
	case msg := <-sub.C():
		return msg
	case <-time.After(d):
		t.Fatal("timeout waiting for forwarded message")
		return mqtt.Message{}
	}
}

func TestForwardBasic(t *testing.T) {
	srcBroker := mock.New()
	dstBroker := mock.New()
	src := srcBroker.Dial()
	dst := dstBroker.Dial()

	b := mqttbridge.New(src, dst, mqttbridge.Route{
		Filters: []string{"Vehicle/#"},
		MaxQoS:  mqtt.ExactlyOnce,
	})
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	// A consumer on the destination broker.
	consumer := dstBroker.Dial()
	sub, err := consumer.Subscribe("Vehicle/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	// Publish on the source broker.
	if err := src.Publish(context.Background(), "Vehicle/Speed", mqtt.AtLeastOnce, []byte("60")); err != nil {
		t.Fatal(err)
	}

	msg := recvTimeout(t, sub, time.Second)
	if msg.Topic != "Vehicle/Speed" {
		t.Errorf("topic = %q, want Vehicle/Speed", msg.Topic)
	}
	if string(msg.Payload) != "60" {
		t.Errorf("payload = %q, want 60", msg.Payload)
	}
}

func TestForwardQoSDowngrade(t *testing.T) {
	srcBroker := mock.New()
	dstBroker := mock.New()

	b := mqttbridge.New(srcBroker.Dial(), dstBroker.Dial(), mqttbridge.Route{
		Filters: []string{"a/#"},
		MaxQoS:  mqtt.AtMostOnce, // cap everything to QoS 0
	})
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	consumer := dstBroker.Dial()
	sub, err := consumer.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub := srcBroker.Dial()
	if err := pub.Publish(context.Background(), "a/b", mqtt.ExactlyOnce, []byte("x")); err != nil {
		t.Fatal(err)
	}

	msg := recvTimeout(t, sub, time.Second)
	if msg.QoS != mqtt.AtMostOnce {
		t.Errorf("forwarded QoS = %v, want AtMostOnce (downgraded)", msg.QoS)
	}
}

func TestForwardPrefixRemap(t *testing.T) {
	srcBroker := mock.New()
	dstBroker := mock.New()

	b := mqttbridge.New(srcBroker.Dial(), dstBroker.Dial(), mqttbridge.Route{
		Filters:     []string{"local/#"},
		MaxQoS:      mqtt.ExactlyOnce,
		StripPrefix: "local",
		AddPrefix:   "remote",
	})
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	consumer := dstBroker.Dial()
	sub, err := consumer.Subscribe("remote/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub := srcBroker.Dial()
	if err := pub.Publish(context.Background(), "local/sensors/temp", mqtt.AtMostOnce, []byte("21")); err != nil {
		t.Fatal(err)
	}

	msg := recvTimeout(t, sub, time.Second)
	if msg.Topic != "remote/sensors/temp" {
		t.Errorf("remapped topic = %q, want remote/sensors/temp", msg.Topic)
	}
}

func TestForwardStats(t *testing.T) {
	srcBroker := mock.New()
	dstBroker := mock.New()

	b := mqttbridge.New(srcBroker.Dial(), dstBroker.Dial(), mqttbridge.Route{
		Filters: []string{"a/#"},
		MaxQoS:  mqtt.ExactlyOnce,
	})
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	consumer := dstBroker.Dial()
	sub, err := consumer.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub := srcBroker.Dial()
	for range 3 {
		if err := pub.Publish(context.Background(), "a/b", mqtt.AtMostOnce, []byte("x")); err != nil {
			t.Fatal(err)
		}
		recvTimeout(t, sub, time.Second)
	}

	// The Forwarded counter is incremented just after the downstream Publish
	// returns, which can lag the consumer's receipt of the message — poll until
	// it settles rather than reading it racily.
	deadline := time.Now().Add(time.Second)
	var got uint64
	for time.Now().Before(deadline) {
		if got = b.Stats().Forwarded; got == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got != 3 {
		t.Errorf("Forwarded = %d, want 3", got)
	}
}

func TestStartIdempotent(t *testing.T) {
	b := mqttbridge.New(mock.New().Dial(), mock.New().Dial(), mqttbridge.Route{
		Filters: []string{"a/#"},
		MaxQoS:  mqtt.ExactlyOnce,
	})
	if err := b.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	_ = b.Stop()
}

func TestStopIdempotent(t *testing.T) {
	b := mqttbridge.New(mock.New().Dial(), mock.New().Dial(), mqttbridge.Route{
		Filters: []string{"a/#"},
		MaxQoS:  mqtt.ExactlyOnce,
	})
	_ = b.Start()
	if err := b.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestStartSubscribeError(t *testing.T) {
	srcBroker := mock.New()
	src := srcBroker.Dial()
	_ = src.Close() // closed client rejects Subscribe

	b := mqttbridge.New(src, mock.New().Dial(), mqttbridge.Route{
		Filters: []string{"a/#"},
		MaxQoS:  mqtt.ExactlyOnce,
	})
	if err := b.Start(); err == nil {
		t.Error("Start with closed source: expected error, got nil")
		_ = b.Stop()
	}
}

func TestPairBidirectional(t *testing.T) {
	brokerA := mock.New()
	brokerB := mock.New()

	// Non-overlapping topic spaces per direction to avoid forwarding loops.
	aToB, bToA := mqttbridge.Pair(brokerA.Dial(), brokerB.Dial(),
		[]mqttbridge.Route{{Filters: []string{"fromA/#"}, MaxQoS: mqtt.ExactlyOnce}},
		[]mqttbridge.Route{{Filters: []string{"fromB/#"}, MaxQoS: mqtt.ExactlyOnce}},
	)
	if err := aToB.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = aToB.Stop() })
	if err := bToA.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bToA.Stop() })

	// A→B: a message published on broker A under fromA/ reaches broker B.
	consumerB := brokerB.Dial()
	subB, err := consumerB.Subscribe("fromA/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subB.Close() })

	// B→A: a message published on broker B under fromB/ reaches broker A.
	consumerA := brokerA.Dial()
	subA, err := consumerA.Subscribe("fromB/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subA.Close() })

	pubA := brokerA.Dial()
	if err := pubA.Publish(context.Background(), "fromA/ping", mqtt.AtMostOnce, []byte("from-a")); err != nil {
		t.Fatal(err)
	}
	if msg := recvTimeout(t, subB, time.Second); string(msg.Payload) != "from-a" {
		t.Errorf("A→B payload = %q, want from-a", msg.Payload)
	}

	pubB := brokerB.Dial()
	if err := pubB.Publish(context.Background(), "fromB/pong", mqtt.AtMostOnce, []byte("from-b")); err != nil {
		t.Fatal(err)
	}
	if msg := recvTimeout(t, subA, time.Second); string(msg.Payload) != "from-b" {
		t.Errorf("B→A payload = %q, want from-b", msg.Payload)
	}
}

func TestRemapStripOnly(t *testing.T) {
	srcBroker := mock.New()
	dstBroker := mock.New()

	b := mqttbridge.New(srcBroker.Dial(), dstBroker.Dial(), mqttbridge.Route{
		Filters:     []string{"local/#"},
		MaxQoS:      mqtt.ExactlyOnce,
		StripPrefix: "local",
	})
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	consumer := dstBroker.Dial()
	sub, err := consumer.Subscribe("#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub := srcBroker.Dial()
	if err := pub.Publish(context.Background(), "local/x/y", mqtt.AtMostOnce, []byte("v")); err != nil {
		t.Fatal(err)
	}

	msg := recvTimeout(t, sub, time.Second)
	if msg.Topic != "x/y" {
		t.Errorf("stripped topic = %q, want x/y", msg.Topic)
	}
}
