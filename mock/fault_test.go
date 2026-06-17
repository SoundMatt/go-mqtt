// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Fault injection tests for the mock broker (ISO 26262 ASIL-B / IEC 61508 SIL 2).
// Each test verifies a specific failure mode enumerated in the FMEA and SAFETY_PLAN.
package mock_test

//fusa:req REQ-FAULT-004
//fusa:req REQ-FAULT-005
//fusa:req REQ-FAULT-006
//fusa:req REQ-FAULT-007
//fusa:req REQ-FAULT-008
//fusa:req REQ-FAULT-009
//fusa:req REQ-FAULT-010

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/mock"
)

// TestFaultPublishAfterClose verifies that Publish on a closed client returns
// ErrClosed (FMEA: session loss → all operations must return ErrClosed).
//
//fusa:req REQ-FAULT-004
func TestFaultPublishAfterClose(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err := c.Publish(context.Background(), "a/b", mqtt.AtMostOnce, []byte("x"))
	if !errors.Is(err, mqtt.ErrClosed) {
		t.Errorf("errors.Is(err, ErrClosed) = false; got %v", err)
	}
}

// TestFaultSubscribeAfterClose verifies that Subscribe on a closed client
// returns ErrClosed (FMEA: session loss).
//
//fusa:req REQ-FAULT-005
func TestFaultSubscribeAfterClose(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := c.Subscribe("a/b", mqtt.AtMostOnce)
	if !errors.Is(err, mqtt.ErrClosed) {
		t.Errorf("errors.Is(err, ErrClosed) = false; got %v", err)
	}
}

// TestFaultIdempotentClose verifies Close is safe to call multiple times
// (FMEA: double-close must not panic or return an error).
//
//fusa:req REQ-FAULT-006
func TestFaultIdempotentClose(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	if err := c.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestFaultEmptyTopic verifies that an empty topic is rejected at the API
// boundary before any network operation (FMEA: invalid input).
//
//fusa:req REQ-FAULT-007
func TestFaultEmptyTopicPublish(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	defer func() { _ = c.Close() }()

	err := c.Publish(context.Background(), "", mqtt.AtMostOnce, []byte("x"))
	if !errors.Is(err, mqtt.ErrTopicEmpty) {
		t.Errorf("errors.Is(err, ErrTopicEmpty) = false; got %v", err)
	}
}

// TestFaultEmptyTopicSubscribe verifies that an empty topic filter is rejected
// (FMEA: invalid input — structural violation before subscription is created).
//
//fusa:req REQ-FAULT-007
func TestFaultEmptyTopicSubscribe(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	defer func() { _ = c.Close() }()

	_, err := c.Subscribe("", mqtt.AtMostOnce)
	if !errors.Is(err, mqtt.ErrTopicEmpty) {
		t.Errorf("errors.Is(err, ErrTopicEmpty) = false; got %v", err)
	}
}

// TestFaultContextCancelled verifies that Publish on a cancelled context returns
// a context error (FMEA: packet loss / timeout).
//
//fusa:req REQ-FAULT-008
func TestFaultContextCancelled(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("x"))
	if err == nil {
		t.Error("expected error on cancelled context, got nil")
	}
}

// TestFaultSubscriptionChannelDrop verifies that a full subscription channel
// causes messages to be dropped rather than blocking the publisher
// (FMEA: packet loss — channel back-pressure with DropNewest policy).
//
//fusa:req REQ-FAULT-009
func TestFaultSubscriptionChannelDrop(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	defer func() { _ = c.Close() }()

	// Channel depth 1 so the second publish overflows.
	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce, mqtt.WithChannelDepth(1))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	ctx := context.Background()
	// First publish fills the channel.
	if err := c.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("first")); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	// Second publish drops (channel full, DropNewest default).
	if err := c.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("second")); err != nil {
		t.Fatalf("second Publish must not error: %v", err)
	}

	// DropCount should reflect at least one drop.
	m := b.Metrics()
	if m.DropCount == 0 {
		t.Error("DropCount = 0, want >= 1 after channel overflow")
	}
}

// TestFaultConcurrentClosePublish verifies that concurrent Close and Publish
// do not panic or deadlock (FMEA: session loss under concurrent access).
//
//fusa:req REQ-FAULT-010
func TestFaultConcurrentClosePublish(t *testing.T) {
	const n = 50
	b := mock.New()
	c := b.Dial()

	var wg sync.WaitGroup
	ctx := context.Background()

	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			_ = c.Publish(ctx, "a/b", mqtt.AtMostOnce, []byte("payload"))
		}(i)
	}

	// Close races with the publishers — must not panic.
	time.Sleep(time.Microsecond)
	_ = c.Close()

	wg.Wait()
}
