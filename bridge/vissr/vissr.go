// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package vissr bridges COVESA VSS signal paths onto MQTT topics.
//
// VSS (Vehicle Signal Specification) uses dot-separated signal paths such as
// "Vehicle.Speed" or "Vehicle.ADAS.AEB.IsActive". MQTT uses slash-separated
// topic hierarchies. This package maps between the two and provides a
// signal-oriented client on top of any mqtt.Client.
//
//	broker := mock.New()
//	vc := vissr.New(broker.Dial())
//	defer vc.Close()
//
//	sub, _ := vc.Subscribe("Vehicle.Speed", mqtt.AtLeastOnce)
//	vc.SetFloat(ctx, "Vehicle.Speed", 60.0)
//	sig := <-sub.C()   // sig.Path == "Vehicle.Speed", sig.Value == 60.0
//
// The package is transport-agnostic: it works with the mock broker, the v3
// TCP client, or the v5 client, since all satisfy mqtt.Client.
package vissr

//fusa:req REQ-VISSR-001
//fusa:req REQ-VISSR-002
//fusa:req REQ-VISSR-003
//fusa:req REQ-VISSR-004
//fusa:req REQ-VISSR-005
//fusa:req REQ-VISSR-006
//fusa:req REQ-VISSR-007
//fusa:req REQ-VISSR-008
//fusa:req REQ-VISSR-009
//fusa:req REQ-VISSR-010

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// ── Path / topic mapping ──────────────────────────────────────────────────────

// PathToTopic converts a VSS dot-separated signal path to an MQTT topic.
//
// "Vehicle.Speed" → "Vehicle/Speed". A trailing ".*" subtree wildcard is
// converted to the MQTT multi-level wildcard "/#"; a bare "*" segment becomes
// the single-level wildcard "+".
//
//fusa:req REQ-VISSR-001
func PathToTopic(path string) string {
	if path == "" {
		return ""
	}
	// Subtree wildcard: "Vehicle.ADAS.*" → "Vehicle/ADAS/#".
	if strings.HasSuffix(path, ".*") {
		prefix := strings.TrimSuffix(path, ".*")
		return strings.ReplaceAll(prefix, ".", "/") + "/#"
	}
	topic := strings.ReplaceAll(path, ".", "/")
	// Single-level wildcard: a "*" segment → "+".
	topic = strings.ReplaceAll(topic, "/*/", "/+/")
	if strings.HasSuffix(topic, "/*") {
		topic = strings.TrimSuffix(topic, "/*") + "/+"
	}
	return topic
}

// TopicToPath converts an MQTT topic back to a VSS dot-separated signal path.
//
// "Vehicle/Speed" → "Vehicle.Speed". This is the inverse of PathToTopic for
// concrete (non-wildcard) topics.
//
//fusa:req REQ-VISSR-002
func TopicToPath(topic string) string {
	return strings.ReplaceAll(topic, "/", ".")
}

// ── Signal ────────────────────────────────────────────────────────────────────

// Signal is a single VSS signal value as carried in an MQTT payload.
//
// The JSON wire form matches the VISSv2 data value object:
//
//	{"path":"Vehicle.Speed","value":60.0,"timestamp":"2026-06-17T00:00:00Z"}
//
//fusa:req REQ-VISSR-003
type Signal struct {
	Path      string    `json:"path"`
	Value     any       `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// Float returns the signal value as a float64. The second return value is false
// if the value is not numeric.
//
//fusa:req REQ-VISSR-004
func (s Signal) Float() (float64, bool) {
	switch v := s.Value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	}
	return 0, false
}

// Bool returns the signal value as a bool. The second return value is false if
// the value is not a boolean.
//
//fusa:req REQ-VISSR-004
func (s Signal) Bool() (bool, bool) {
	v, ok := s.Value.(bool)
	return v, ok
}

// String returns the signal value as a string. The second return value is false
// if the value is not a string.
//
//fusa:req REQ-VISSR-004
func (s Signal) String() (string, bool) {
	v, ok := s.Value.(string)
	return v, ok
}

// ── Client ────────────────────────────────────────────────────────────────────

// Client is a VSS-aware wrapper around an mqtt.Client.
//
//fusa:req REQ-VISSR-005
type Client struct {
	mc mqtt.Client
}

// New returns a VISSR Client wrapping the given mqtt.Client. The underlying
// client is closed when the VISSR Client is closed.
//
//fusa:req REQ-VISSR-005
func New(mc mqtt.Client) *Client {
	return &Client{mc: mc}
}

// Set publishes value as the current value of the VSS signal at path. The value
// is wrapped in a Signal envelope and JSON-encoded. Use the typed helpers
// (SetFloat, SetBool, SetString) for compile-time type safety.
//
//fusa:req REQ-VISSR-006
func (c *Client) Set(ctx context.Context, path string, value any, qos mqtt.QoS) error {
	if path == "" {
		return mqtt.ErrTopicEmpty
	}
	sig := Signal{Path: path, Value: value, Timestamp: time.Now().UTC()}
	payload, err := json.Marshal(sig)
	if err != nil {
		return fmt.Errorf("vissr: marshal signal %q: %w", path, err)
	}
	return c.mc.Publish(ctx, PathToTopic(path), qos, payload)
}

// SetFloat publishes a numeric signal value at the safety-relevant default QoS.
//
//fusa:req REQ-VISSR-006
func (c *Client) SetFloat(ctx context.Context, path string, value float64, qos mqtt.QoS) error {
	return c.Set(ctx, path, value, qos)
}

// SetBool publishes a boolean signal value.
//
//fusa:req REQ-VISSR-006
func (c *Client) SetBool(ctx context.Context, path string, value bool, qos mqtt.QoS) error {
	return c.Set(ctx, path, value, qos)
}

// SetString publishes a string signal value.
//
//fusa:req REQ-VISSR-006
func (c *Client) SetString(ctx context.Context, path string, value string, qos mqtt.QoS) error {
	return c.Set(ctx, path, value, qos)
}

// Subscribe subscribes to a VSS signal path (or subtree, using a trailing ".*")
// and returns a Subscription delivering decoded Signals.
//
//fusa:req REQ-VISSR-007
func (c *Client) Subscribe(path string, qos mqtt.QoS, opts ...mqtt.SubscriberOption) (*Subscription, error) {
	if path == "" {
		return nil, mqtt.ErrTopicEmpty
	}
	sub, err := c.mc.Subscribe(PathToTopic(path), qos, opts...)
	if err != nil {
		return nil, err
	}
	vs := &Subscription{
		inner: sub,
		ch:    make(chan Signal, cap(sub.C())),
	}
	go vs.pump()
	return vs, nil
}

// Close closes the underlying mqtt.Client.
//
//fusa:req REQ-VISSR-008
func (c *Client) Close() error {
	return c.mc.Close()
}

// ── Subscription ──────────────────────────────────────────────────────────────

// Subscription delivers decoded VSS Signals from a subscribed path.
//
//fusa:req REQ-VISSR-009
type Subscription struct {
	inner mqtt.Subscription
	ch    chan Signal
}

// C returns the channel on which decoded Signals are delivered. The channel is
// closed when the subscription or underlying client is closed.
//
//fusa:req REQ-VISSR-009
func (s *Subscription) C() <-chan Signal { return s.ch }

// Unsubscribe stops delivery without closing the channel.
//
//fusa:req REQ-VISSR-010
func (s *Subscription) Unsubscribe() error { return s.inner.Unsubscribe() }

// Close unsubscribes and closes the Signal channel.
//
//fusa:req REQ-VISSR-010
func (s *Subscription) Close() error { return s.inner.Close() }

// pump decodes inbound MQTT messages into Signals. Messages that fail to decode
// are dropped silently so a single malformed payload cannot stall the stream.
//
//fusa:req REQ-VISSR-009
func (s *Subscription) pump() {
	defer close(s.ch)
	for msg := range s.inner.C() {
		var sig Signal
		if err := json.Unmarshal(msg.Payload, &sig); err != nil {
			continue // drop malformed payloads
		}
		// Backfill the path from the topic if the payload omitted it.
		if sig.Path == "" {
			sig.Path = TopicToPath(msg.Topic)
		}
		s.ch <- sig
	}
}
