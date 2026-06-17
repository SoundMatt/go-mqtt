// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqtt_test

import (
	"context"
	"errors"
	"testing"

	relay "github.com/SoundMatt/RELAY"
	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/mock"
)

// ── SpecVersion ───────────────────────────────────────────────────────────────

func TestSpecVersion(t *testing.T) {
	if mqtt.SpecVersion != "1.0" {
		t.Errorf("SpecVersion = %q, want %q", mqtt.SpecVersion, "1.0")
	}
	// go-mqtt binds SpecVersion to the RELAY package constant so they can't drift.
	if mqtt.SpecVersion != relay.SpecVersion {
		t.Errorf("SpecVersion = %q, want relay.SpecVersion %q", mqtt.SpecVersion, relay.SpecVersion)
	}
}

// ── Error wrapping ────────────────────────────────────────────────────────────

func TestErrClosedWrapsRelay(t *testing.T) {
	if !errors.Is(mqtt.ErrClosed, relay.ErrClosed) {
		t.Error("errors.Is(mqtt.ErrClosed, relay.ErrClosed) = false, want true")
	}
}

func TestErrNotConnectedWrapsRelay(t *testing.T) {
	if !errors.Is(mqtt.ErrNotConnected, relay.ErrNotConnected) {
		t.Error("errors.Is(mqtt.ErrNotConnected, relay.ErrNotConnected) = false, want true")
	}
}

func TestErrTimeoutWrapsRelay(t *testing.T) {
	if !errors.Is(mqtt.ErrTimeout, relay.ErrTimeout) {
		t.Error("errors.Is(mqtt.ErrTimeout, relay.ErrTimeout) = false, want true")
	}
}

func TestErrPayloadTooLargeWrapsRelay(t *testing.T) {
	if !errors.Is(mqtt.ErrPayloadTooLarge, relay.ErrPayloadTooLarge) {
		t.Error("errors.Is(mqtt.ErrPayloadTooLarge, relay.ErrPayloadTooLarge) = false, want true")
	}
}

func TestErrTopicEmptyWrapsRelayNotConnected(t *testing.T) {
	if !errors.Is(mqtt.ErrTopicEmpty, relay.ErrNotConnected) {
		t.Error("errors.Is(mqtt.ErrTopicEmpty, relay.ErrNotConnected) = false, want true")
	}
}

func TestErrQoSUnsupportedWrapsRelayNotConnected(t *testing.T) {
	if !errors.Is(mqtt.ErrQoSUnsupported, relay.ErrNotConnected) {
		t.Error("errors.Is(mqtt.ErrQoSUnsupported, relay.ErrNotConnected) = false, want true")
	}
}

// ── BackPressurePolicy ────────────────────────────────────────────────────────

func TestBackPressurePolicyValues(t *testing.T) {
	if mqtt.DropNewest != 0 {
		t.Errorf("DropNewest = %d, want 0", mqtt.DropNewest)
	}
	if mqtt.DropOldest != 1 {
		t.Errorf("DropOldest = %d, want 1", mqtt.DropOldest)
	}
	if mqtt.Block != 2 {
		t.Errorf("Block = %d, want 2", mqtt.Block)
	}
}

func TestWithBackPressure(t *testing.T) {
	cfg := mqtt.ApplySubscriberOpts([]mqtt.SubscriberOption{
		mqtt.WithBackPressure(mqtt.DropOldest),
	})
	if cfg.BackPressure != mqtt.DropOldest {
		t.Errorf("BackPressure = %d, want DropOldest", cfg.BackPressure)
	}
}

// ── ToMessage / FromMessage ───────────────────────────────────────────────────

func TestToMessage(t *testing.T) {
	m := mqtt.Message{
		Topic:    "test/topic",
		Payload:  []byte("hello"),
		QoS:      mqtt.AtLeastOnce,
		Retained: true,
	}
	rm := m.ToMessage()

	if rm.Protocol != relay.MQTT {
		t.Errorf("Protocol = %v, want relay.MQTT", rm.Protocol)
	}
	if rm.ID != "test/topic" {
		t.Errorf("ID = %q, want %q", rm.ID, "test/topic")
	}
	if string(rm.Payload) != "hello" {
		t.Errorf("Payload = %q, want %q", rm.Payload, "hello")
	}
	if rm.Meta["mqtt.qos"] != "1" {
		t.Errorf("mqtt.qos = %q, want %q", rm.Meta["mqtt.qos"], "1")
	}
	if rm.Meta["mqtt.retained"] != "true" {
		t.Errorf("mqtt.retained = %q, want %q", rm.Meta["mqtt.retained"], "true")
	}
}

func TestFromMessage(t *testing.T) {
	rm := relay.Message{
		Protocol: relay.MQTT,
		ID:       "sensors/temp",
		Payload:  []byte("42"),
		Meta: map[string]string{
			"mqtt.qos":      "1",
			"mqtt.retained": "true",
		},
	}
	m, err := mqtt.FromMessage(rm)
	if err != nil {
		t.Fatalf("FromMessage error: %v", err)
	}
	if m.Topic != "sensors/temp" {
		t.Errorf("Topic = %q, want %q", m.Topic, "sensors/temp")
	}
	if string(m.Payload) != "42" {
		t.Errorf("Payload = %q, want %q", m.Payload, "42")
	}
	if m.QoS != mqtt.AtLeastOnce {
		t.Errorf("QoS = %v, want AtLeastOnce", m.QoS)
	}
	if !m.Retained {
		t.Error("Retained = false, want true")
	}
}

func TestFromMessageDefaults(t *testing.T) {
	rm := relay.Message{ID: "a/b", Payload: []byte("x")}
	m, err := mqtt.FromMessage(rm)
	if err != nil {
		t.Fatalf("FromMessage error: %v", err)
	}
	if m.QoS != mqtt.AtMostOnce {
		t.Errorf("QoS = %v, want AtMostOnce", m.QoS)
	}
	if m.Retained {
		t.Error("Retained = true, want false")
	}
}

// ── Adapt ────────────────────────────────────────────────────────────────────

func TestAdaptProtocol(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	node := mqtt.Adapt(c)
	if node.Protocol() != relay.MQTT {
		t.Errorf("Protocol() = %v, want relay.MQTT", node.Protocol())
	}
	defer func() { _ = node.Close() }()
}

func TestAdaptSendSubscribe(t *testing.T) {
	b := mock.New()
	c1 := b.Dial()
	c2 := b.Dial()
	node2 := mqtt.Adapt(c2)

	ch, err := node2.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	if err := c1.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("relay")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := <-ch
	if msg.ID != "a/b" {
		t.Errorf("ID = %q, want %q", msg.ID, "a/b")
	}
	if string(msg.Payload) != "relay" {
		t.Errorf("Payload = %q, want %q", msg.Payload, "relay")
	}

	_ = c1.Close()
	_ = node2.Close()
}

func TestAdaptClose(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	node := mqtt.Adapt(c)
	if err := node.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Second close should be a no-op (idempotent).
	_ = node.Close()
}

// ── Optional interfaces (§9) ──────────────────────────────────────────────────

func TestBrokerHealthOK(t *testing.T) {
	b := mock.New()
	var hp mqtt.HealthProvider = b
	h := hp.Health()
	if h.Status != mqtt.HealthOK {
		t.Errorf("Health.Status = %v, want HealthOK", h.Status)
	}
}

func TestBrokerHealthDownAfterDrain(t *testing.T) {
	b := mock.New()
	if err := b.CloseWithDrain(context.Background()); err != nil {
		t.Fatalf("CloseWithDrain: %v", err)
	}
	h := b.Health()
	if h.Status != mqtt.HealthDown {
		t.Errorf("Health.Status = %v after close, want HealthDown", h.Status)
	}
}

func TestBrokerMetricsCountsWrites(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	_ = c.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("hello"))
	_ = c.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("world"))

	var mp mqtt.MetricsProvider = b
	m := mp.Metrics()
	if m.WriteCount != 2 {
		t.Errorf("WriteCount = %d, want 2", m.WriteCount)
	}
	if m.BytesWritten != 10 {
		t.Errorf("BytesWritten = %d, want 10", m.BytesWritten)
	}
}

func TestBrokerMetricsCountsDelivers(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	_ = c.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("hi"))
	<-sub.C() // wait for delivery

	m := b.Metrics()
	if m.DeliverCount != 1 {
		t.Errorf("DeliverCount = %d, want 1", m.DeliverCount)
	}
	if m.BytesDelivered != 2 {
		t.Errorf("BytesDelivered = %d, want 2", m.BytesDelivered)
	}
}

func TestAdaptCloseWithDrain(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	node := mqtt.Adapt(c)
	d, ok := node.(mqtt.Drainer)
	if !ok {
		t.Fatal("Adapt result does not implement mqtt.Drainer")
	}
	if err := d.CloseWithDrain(context.Background()); err != nil {
		t.Errorf("CloseWithDrain: %v", err)
	}
}
