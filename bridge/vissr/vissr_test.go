// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package vissr_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/bridge/vissr"
	"github.com/SoundMatt/go-mqtt/mock"
)

// Requirements verified by this VISSR bridge test suite (VSS dot ↔ MQTT slash):
// path/topic mapping, Signal envelope + typed accessors, Set*/Subscribe
// transport delegation, malformed-payload drop, and Unsubscribe/Close.
//
//fusa:test REQ-VISSR-001
//fusa:test REQ-VISSR-002
//fusa:test REQ-VISSR-003
//fusa:test REQ-VISSR-004
//fusa:test REQ-VISSR-005
//fusa:test REQ-VISSR-006
//fusa:test REQ-VISSR-007
//fusa:test REQ-VISSR-008
//fusa:test REQ-VISSR-009
//fusa:test REQ-VISSR-010

// ── Path / topic mapping ──────────────────────────────────────────────────────

func TestPathToTopic(t *testing.T) {
	cases := []struct {
		path, want string
	}{
		{"Vehicle.Speed", "Vehicle/Speed"},
		{"Vehicle.ADAS.AEB.IsActive", "Vehicle/ADAS/AEB/IsActive"},
		{"Vehicle.ADAS.*", "Vehicle/ADAS/#"},
		{"Vehicle.*.Speed", "Vehicle/+/Speed"},
		{"Vehicle.Cabin.Door.*", "Vehicle/Cabin/Door/#"},
		{"", ""},
		{"Single", "Single"},
	}
	for _, tc := range cases {
		if got := vissr.PathToTopic(tc.path); got != tc.want {
			t.Errorf("PathToTopic(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestTopicToPath(t *testing.T) {
	cases := []struct {
		topic, want string
	}{
		{"Vehicle/Speed", "Vehicle.Speed"},
		{"Vehicle/ADAS/AEB/IsActive", "Vehicle.ADAS.AEB.IsActive"},
		{"Single", "Single"},
	}
	for _, tc := range cases {
		if got := vissr.TopicToPath(tc.topic); got != tc.want {
			t.Errorf("TopicToPath(%q) = %q, want %q", tc.topic, got, tc.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	paths := []string{"Vehicle.Speed", "Vehicle.ADAS.AEB.IsActive", "A.B.C.D.E"}
	for _, p := range paths {
		if got := vissr.TopicToPath(vissr.PathToTopic(p)); got != p {
			t.Errorf("round trip %q = %q", p, got)
		}
	}
}

// ── Signal accessors ──────────────────────────────────────────────────────────

func TestSignalFloat(t *testing.T) {
	s := vissr.Signal{Value: 60.0}
	if v, ok := s.Float(); !ok || v != 60.0 {
		t.Errorf("Float() = %v, %v; want 60.0, true", v, ok)
	}
	s = vissr.Signal{Value: "not a number"}
	if _, ok := s.Float(); ok {
		t.Error("Float() on string returned ok=true")
	}
}

func TestSignalBool(t *testing.T) {
	s := vissr.Signal{Value: true}
	if v, ok := s.Bool(); !ok || !v {
		t.Errorf("Bool() = %v, %v; want true, true", v, ok)
	}
	s = vissr.Signal{Value: 1.0}
	if _, ok := s.Bool(); ok {
		t.Error("Bool() on float returned ok=true")
	}
}

func TestSignalString(t *testing.T) {
	s := vissr.Signal{Value: "DriverSeat"}
	if v, ok := s.String(); !ok || v != "DriverSeat" {
		t.Errorf("String() = %v, %v; want DriverSeat, true", v, ok)
	}
}

// ── Client publish/subscribe ──────────────────────────────────────────────────

func TestSetAndSubscribeFloat(t *testing.T) {
	b := mock.New()
	vc := vissr.New(b.Dial())
	t.Cleanup(func() { _ = vc.Close() })

	sub, err := vc.Subscribe("Vehicle.Speed", mqtt.AtLeastOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	if err := vc.SetFloat(ctx, "Vehicle.Speed", 88.5, mqtt.AtLeastOnce); err != nil {
		t.Fatal(err)
	}

	select {
	case sig := <-sub.C():
		if sig.Path != "Vehicle.Speed" {
			t.Errorf("Path = %q, want Vehicle.Speed", sig.Path)
		}
		v, ok := sig.Float()
		if !ok || v != 88.5 {
			t.Errorf("Float() = %v, %v; want 88.5, true", v, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for signal")
	}
}

func TestSetAndSubscribeBool(t *testing.T) {
	b := mock.New()
	vc := vissr.New(b.Dial())
	t.Cleanup(func() { _ = vc.Close() })

	sub, err := vc.Subscribe("Vehicle.ADAS.AEB.IsActive", mqtt.AtLeastOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	if err := vc.SetBool(ctx, "Vehicle.ADAS.AEB.IsActive", true, mqtt.AtLeastOnce); err != nil {
		t.Fatal(err)
	}

	select {
	case sig := <-sub.C():
		v, ok := sig.Bool()
		if !ok || !v {
			t.Errorf("Bool() = %v, %v; want true, true", v, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for signal")
	}
}

func TestSubscribeSubtreeWildcard(t *testing.T) {
	b := mock.New()
	vc := vissr.New(b.Dial())
	t.Cleanup(func() { _ = vc.Close() })

	// Subscribe to the whole ADAS subtree.
	sub, err := vc.Subscribe("Vehicle.ADAS.*", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	if err := vc.SetBool(ctx, "Vehicle.ADAS.AEB.IsActive", true, mqtt.AtMostOnce); err != nil {
		t.Fatal(err)
	}

	select {
	case sig := <-sub.C():
		if sig.Path != "Vehicle.ADAS.AEB.IsActive" {
			t.Errorf("Path = %q, want Vehicle.ADAS.AEB.IsActive", sig.Path)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subtree signal")
	}
}

func TestSetEmptyPath(t *testing.T) {
	b := mock.New()
	vc := vissr.New(b.Dial())
	t.Cleanup(func() { _ = vc.Close() })

	err := vc.Set(context.Background(), "", 1.0, mqtt.AtMostOnce)
	if !errors.Is(err, mqtt.ErrTopicEmpty) {
		t.Errorf("Set empty path: got %v, want ErrTopicEmpty", err)
	}
}

func TestSubscribeEmptyPath(t *testing.T) {
	b := mock.New()
	vc := vissr.New(b.Dial())
	t.Cleanup(func() { _ = vc.Close() })

	_, err := vc.Subscribe("", mqtt.AtMostOnce)
	if !errors.Is(err, mqtt.ErrTopicEmpty) {
		t.Errorf("Subscribe empty path: got %v, want ErrTopicEmpty", err)
	}
}

func TestMalformedPayloadDropped(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	vc := vissr.New(c)
	t.Cleanup(func() { _ = vc.Close() })

	sub, err := vc.Subscribe("Vehicle.Speed", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx := context.Background()
	// Publish a raw non-JSON payload directly on the mapped topic.
	if err := c.Publish(ctx, "Vehicle/Speed", mqtt.AtMostOnce, []byte("not json")); err != nil {
		t.Fatal(err)
	}
	// Then a valid signal.
	if err := vc.SetFloat(ctx, "Vehicle.Speed", 42.0, mqtt.AtMostOnce); err != nil {
		t.Fatal(err)
	}

	select {
	case sig := <-sub.C():
		// The malformed payload must have been dropped; we get the valid one.
		v, ok := sig.Float()
		if !ok || v != 42.0 {
			t.Errorf("Float() = %v, %v; want 42.0, true", v, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout — malformed payload may have stalled the stream")
	}
}

func TestPathBackfillFromTopic(t *testing.T) {
	b := mock.New()
	c := b.Dial()
	vc := vissr.New(c)
	t.Cleanup(func() { _ = vc.Close() })

	sub, err := vc.Subscribe("Vehicle.Speed", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	// Payload with no "path" field — should be backfilled from the topic.
	if err := c.Publish(context.Background(), "Vehicle/Speed", mqtt.AtMostOnce,
		[]byte(`{"value":55.0}`)); err != nil {
		t.Fatal(err)
	}

	select {
	case sig := <-sub.C():
		if sig.Path != "Vehicle.Speed" {
			t.Errorf("Path = %q, want backfilled Vehicle.Speed", sig.Path)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for signal")
	}
}
