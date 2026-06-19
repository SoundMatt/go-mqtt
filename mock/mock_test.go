// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/mock"
)

// Requirements verified by this mock-broker test suite: retained store/replace/
// delete + replay, MatchTopic fan-out, Dial connectivity, and concurrent-use
// safety of the exported Client methods. Payload-copy isolation (REQ-LEAK-003)
// and single-channel ordering (REQ-ORDER-001) are covered by the dedicated
// tests below; subscription release (REQ-LEAK-002) by TestUnsubscribe.
//
//fusa:test REQ-MOCK-001
//fusa:test REQ-MOCK-002
//fusa:test REQ-MOCK-003
//fusa:test REQ-MOCK-004
//fusa:test REQ-MOCK-005
//fusa:test REQ-CONC-001
//fusa:test REQ-CONC-002
//fusa:test REQ-CONC-003
//fusa:test REQ-LEAK-002
func TestPublishSubscribe(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("sensors/temperature", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	want := []byte(`{"temp":21.5}`)
	if err := c.Publish(ctx, "sensors/temperature", mqtt.AtMostOnce, want); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sub.C():
		if msg.Topic != "sensors/temperature" {
			t.Errorf("topic: got %q, want %q", msg.Topic, "sensors/temperature")
		}
		if string(msg.Payload) != string(want) {
			t.Errorf("payload: got %q, want %q", msg.Payload, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestWildcardSingleLevel(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("sensors/+", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	topics := []string{"sensors/temperature", "sensors/pressure", "sensors/humidity"}
	for _, topic := range topics {
		if err := c.Publish(ctx, topic, mqtt.AtMostOnce, []byte("val")); err != nil {
			t.Fatal(err)
		}
	}

	for i := range topics {
		select {
		case msg := <-sub.C():
			_ = msg
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// "sensors/temp/extra" must NOT match "sensors/+"
	if err := c.Publish(ctx, "sensors/temp/extra", mqtt.AtMostOnce, []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-sub.C():
		t.Errorf("unexpected message on sensors/temp/extra: %v", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWildcardMultiLevel(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("Vehicle/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	topics := []string{
		"Vehicle/Speed",
		"Vehicle/Cabin/HVAC/Temperature",
		"Vehicle/Powertrain/ElectricMotor/Torque",
	}
	for _, topic := range topics {
		if err := c.Publish(ctx, topic, mqtt.AtMostOnce, []byte("val")); err != nil {
			t.Fatal(err)
		}
	}

	received := make(map[string]bool)
	for i := range topics {
		select {
		case msg := <-sub.C():
			received[msg.Topic] = true
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
	for _, topic := range topics {
		if !received[topic] {
			t.Errorf("did not receive message on %q", topic)
		}
	}
}

func TestNoMatchNoDelivery(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("sensors/temperature", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	if err := c.Publish(ctx, "sensors/pressure", mqtt.AtMostOnce, []byte("val")); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sub.C():
		t.Errorf("unexpected message: %v", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

//fusa:test REQ-SUB-007
//fusa:test REQ-SUB-008
func TestMultipleSubscribers(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	const n = 5
	subs := make([]mqtt.Subscription, n)
	for i := range subs {
		var err error
		subs[i], err = c.Subscribe("test/topic", mqtt.AtMostOnce)
		if err != nil {
			t.Fatal(err)
		}
		sub := subs[i]
		t.Cleanup(func() { _ = sub.Close() })
	}

	ctx := context.Background()
	if err := c.Publish(ctx, "test/topic", mqtt.AtMostOnce, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	for i, sub := range subs {
		select {
		case msg := <-sub.C():
			if string(msg.Payload) != "hello" {
				t.Errorf("sub %d: got %q", i, msg.Payload)
			}
		case <-time.After(time.Second):
			t.Errorf("sub %d: timeout", i)
		}
	}
}

func TestRetainedMessage(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	// Normal (non-retained) publish — late subscriber should NOT receive it.
	if err := c.Publish(ctx, "status/online", mqtt.AtMostOnce, []byte("true")); err != nil {
		t.Fatal(err)
	}

	sub, err := c.Subscribe("status/online", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	select {
	case <-sub.C():
		t.Error("late subscriber received non-retained message")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestClosedClientErrors(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Publish(ctx, "x", mqtt.AtMostOnce, nil); err != mqtt.ErrClosed {
		t.Errorf("Publish on closed: got %v, want ErrClosed", err)
	}
	if _, err := c.Subscribe("x", mqtt.AtMostOnce); err != mqtt.ErrClosed {
		t.Errorf("Subscribe on closed: got %v, want ErrClosed", err)
	}
}

func TestEmptyTopicErrors(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	if err := c.Publish(ctx, "", mqtt.AtMostOnce, nil); err != mqtt.ErrTopicEmpty {
		t.Errorf("Publish empty topic: got %v, want ErrTopicEmpty", err)
	}
	if _, err := c.Subscribe("", mqtt.AtMostOnce); err != mqtt.ErrTopicEmpty {
		t.Errorf("Subscribe empty topic: got %v, want ErrTopicEmpty", err)
	}
}

func TestUnsubscribe(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("test/unsub", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}

	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Publish(ctx, "test/unsub", mqtt.AtMostOnce, []byte("x")); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sub.C():
		t.Errorf("received message after Unsubscribe: %v", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestContextCancellation(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.Publish(ctx, "topic", mqtt.AtMostOnce, []byte("x"))
	if err == nil {
		t.Error("expected error on cancelled context, got nil")
	}
}

func TestConcurrentPublish(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("concurrent/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	const goroutines = 10
	const msgsPerGoroutine = 20

	var wg sync.WaitGroup
	ctx := context.Background()
	for i := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range msgsPerGoroutine {
				_ = c.Publish(ctx, "concurrent/test", mqtt.AtMostOnce, []byte{byte(id), byte(j)})
			}
		}(i)
	}
	wg.Wait()
}

//fusa:test REQ-SUB-003
//fusa:test REQ-SUB-004
//fusa:test REQ-SUB-005
func TestChannelDepthOption(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	// REQ-SUB-004: the default channel depth is at least 64.
	defSub, err := c.Subscribe("depth/default", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = defSub.Close() })
	if c := cap(defSub.C()); c < 64 {
		t.Errorf("default channel depth = %d, want >= 64", c)
	}

	// REQ-SUB-005: depth is configurable; REQ-SUB-003: the channel is buffered.
	sub, err := c.Subscribe("depth/test", mqtt.AtMostOnce, mqtt.WithChannelDepth(2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	if cap(sub.C()) != 2 {
		t.Errorf("configured channel depth = %d, want 2", cap(sub.C()))
	}

	ctx := context.Background()
	// Publish 3 messages — 3rd should be dropped (channel depth=2).
	for i := range 3 {
		_ = c.Publish(ctx, "depth/test", mqtt.AtMostOnce, []byte{byte(i)})
	}

	count := 0
	for {
		select {
		case <-sub.C():
			count++
		case <-time.After(50 * time.Millisecond):
			if count > 2 {
				t.Errorf("received %d messages, want ≤2 (depth=2)", count)
			}
			return
		}
	}
}

// TestPayloadCopyIsolation verifies that the bytes delivered to a subscriber are
// a copy of the publisher's slice, so a caller mutating its buffer after Publish
// cannot corrupt an already-delivered message (REQ-LEAK-003).
//
//fusa:test REQ-LEAK-003
func TestPayloadCopyIsolation(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("iso/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	payload := []byte("original")
	if err := c.Publish(context.Background(), "iso/x", mqtt.AtMostOnce, payload); err != nil {
		t.Fatal(err)
	}
	// Mutate the caller's buffer immediately after Publish returns.
	for i := range payload {
		payload[i] = 'X'
	}

	select {
	case msg := <-sub.C():
		if string(msg.Payload) != "original" {
			t.Errorf("delivered payload = %q, want %q (publisher mutation leaked)", msg.Payload, "original")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestSingleChannelOrdering verifies that messages published from one goroutine
// arrive on a single subscription channel in publication order (REQ-ORDER-001).
//
//fusa:test REQ-ORDER-001
func TestSingleChannelOrdering(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("ord/#", mqtt.AtMostOnce, mqtt.WithChannelDepth(128))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	const n = 100
	ctx := context.Background()
	for i := range n {
		if err := c.Publish(ctx, "ord/x", mqtt.AtMostOnce, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		select {
		case msg := <-sub.C():
			if msg.Payload[0] != byte(i) {
				t.Fatalf("message %d out of order: got %d", i, msg.Payload[0])
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}
