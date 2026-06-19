// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package broker_test

import (
	"context"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/broker"
	"github.com/SoundMatt/go-mqtt/v3"
)

// startBroker starts an embedded broker on a free port and returns its address.
func startBroker(t *testing.T) (*broker.Server, string) {
	t.Helper()
	srv := broker.New()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe("127.0.0.1:0") }()

	// Wait for the listener to be registered.
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("broker did not start listening")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, srv.Addr()
}

func dial(t *testing.T, addr, id string) mqtt.Client {
	t.Helper()
	c, err := v3.Dial(addr, v3.WithClientID(id), v3.WithKeepalive(0))
	if err != nil {
		t.Fatalf("dial %s: %v", id, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func recv(t *testing.T, sub mqtt.Subscription) mqtt.Message {
	t.Helper()
	select {
	case m := <-sub.C():
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
		return mqtt.Message{}
	}
}

// Requirements verified by this broker test suite (in-process MQTT v3.1.1
// server): Server construction + session tracking, accept loop, idempotent
// Close, CONNECT/CONNACK, QoS 0/1/2 routing, wildcard delivery, retained
// replay, unsubscribe, and the MetricsProvider counters. WithTLS is covered in
// tls_test.go; the will and wire framing in raw_test.go.
//
//fusa:test REQ-BROKER-001
//fusa:test REQ-BROKER-002
//fusa:test REQ-BROKER-003
//fusa:test REQ-BROKER-004
//fusa:test REQ-BROKER-005
//fusa:test REQ-BROKER-006
//fusa:test REQ-BROKER-007
//fusa:test REQ-BROKER-010
func TestPubSubQoS0(t *testing.T) {
	_, addr := startBroker(t)
	sub := dial(t, addr, "sub")
	pub := dial(t, addr, "pub")

	s, err := sub.Subscribe("Vehicle/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // allow SUBACK

	if err := pub.Publish(context.Background(), "Vehicle/Speed", mqtt.AtMostOnce, []byte("60")); err != nil {
		t.Fatal(err)
	}
	msg := recv(t, s)
	if msg.Topic != "Vehicle/Speed" || string(msg.Payload) != "60" {
		t.Errorf("got %q=%q, want Vehicle/Speed=60", msg.Topic, msg.Payload)
	}
}

func TestPubSubQoS1(t *testing.T) {
	_, addr := startBroker(t)
	sub := dial(t, addr, "sub1")
	pub := dial(t, addr, "pub1")

	s, err := sub.Subscribe("a/#", mqtt.AtLeastOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := pub.Publish(context.Background(), "a/b", mqtt.AtLeastOnce, []byte("q1")); err != nil {
		t.Fatalf("publish qos1: %v", err)
	}
	msg := recv(t, s)
	if string(msg.Payload) != "q1" {
		t.Errorf("payload = %q, want q1", msg.Payload)
	}
}

func TestPubSubQoS2(t *testing.T) {
	_, addr := startBroker(t)
	sub := dial(t, addr, "sub2")
	pub := dial(t, addr, "pub2")

	s, err := sub.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// Publisher uses QoS 2: the full PUBLISH→PUBREC→PUBREL→PUBCOMP handshake
	// must complete against the broker.
	if err := pub.Publish(context.Background(), "a/b", mqtt.ExactlyOnce, []byte("q2")); err != nil {
		t.Fatalf("publish qos2: %v", err)
	}
	msg := recv(t, s)
	if string(msg.Payload) != "q2" {
		t.Errorf("payload = %q, want q2", msg.Payload)
	}
}

func TestWildcardSingleLevel(t *testing.T) {
	_, addr := startBroker(t)
	sub := dial(t, addr, "subw")
	pub := dial(t, addr, "pubw")

	s, err := sub.Subscribe("sensors/+/temp", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := pub.Publish(context.Background(), "sensors/room1/temp", mqtt.AtMostOnce, []byte("21")); err != nil {
		t.Fatal(err)
	}
	if msg := recv(t, s); msg.Topic != "sensors/room1/temp" {
		t.Errorf("topic = %q", msg.Topic)
	}
}

func TestNonRetainedNotReplayed(t *testing.T) {
	_, addr := startBroker(t)

	// A live (non-retained) message published before a subscriber connects must
	// not be replayed on subscribe. (Retained replay is covered by raw_test.go,
	// since the v3 client cannot set the retain flag.)
	pub := dial(t, addr, "pubr")
	if err := pub.Publish(context.Background(), "r/x", mqtt.AtMostOnce, []byte("live")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	sub := dial(t, addr, "subr")
	s, err := sub.Subscribe("r/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-s.C():
		t.Errorf("unexpected replay of non-retained message: %q", m.Payload)
	case <-time.After(200 * time.Millisecond):
		// Good: nothing retained.
	}
}

func TestIndependentSubscriptions(t *testing.T) {
	_, addr := startBroker(t)
	pub := dial(t, addr, "pubi")
	subA := dial(t, addr, "subA")
	subB := dial(t, addr, "subB")

	sa, err := subA.Subscribe("x/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := subB.Subscribe("x/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := pub.Publish(context.Background(), "x/y", mqtt.AtMostOnce, []byte("fanout")); err != nil {
		t.Fatal(err)
	}
	if string(recv(t, sa).Payload) != "fanout" {
		t.Error("subA did not receive")
	}
	if string(recv(t, sb).Payload) != "fanout" {
		t.Error("subB did not receive")
	}
}

func TestUnsubscribe(t *testing.T) {
	_, addr := startBroker(t)
	pub := dial(t, addr, "pubu")
	sub := dial(t, addr, "subu")

	s, err := sub.Subscribe("u/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := s.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := pub.Publish(context.Background(), "u/v", mqtt.AtMostOnce, []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-s.C():
		t.Errorf("received after unsubscribe: %q", m.Payload)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestMetrics(t *testing.T) {
	srv, addr := startBroker(t)
	pub := dial(t, addr, "pubm")
	sub := dial(t, addr, "subm")

	s, err := sub.Subscribe("m/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := pub.Publish(context.Background(), "m/a", mqtt.AtMostOnce, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	recv(t, s)

	// The broker implements mqtt.MetricsProvider. DeliverCount is incremented
	// just after the broker writes the PUBLISH to the subscriber socket, which
	// can lag the subscriber's receipt — poll until the counters settle.
	var mp mqtt.MetricsProvider = srv
	deadline := time.Now().Add(time.Second)
	var m mqtt.Metrics
	for time.Now().Before(deadline) {
		m = mp.Metrics()
		if m.WriteCount >= 1 && m.DeliverCount >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if m.WriteCount < 1 {
		t.Errorf("WriteCount = %d, want >= 1", m.WriteCount)
	}
	if m.DeliverCount < 1 {
		t.Errorf("DeliverCount = %d, want >= 1", m.DeliverCount)
	}
	if srv.SubscriberCount() != 1 {
		t.Errorf("SubscriberCount = %d, want 1", srv.SubscriberCount())
	}
}

func TestCloseIdempotent(t *testing.T) {
	srv, _ := startBroker(t)
	if err := srv.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
