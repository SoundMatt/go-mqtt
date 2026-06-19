// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	relay "github.com/SoundMatt/RELAY"
	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/mock"
)

// useMock points dialFunc at a shared in-process mock broker and restores it
// when the test ends.
func useMock(t *testing.T) *mock.Broker {
	t.Helper()
	b := mock.New()
	prev := dialFunc
	dialFunc = func(string) (mqtt.Client, error) { return b.Dial(), nil }
	t.Cleanup(func() { dialFunc = prev })
	return b
}

func recvWithin(t *testing.T, sub mqtt.Subscription, d time.Duration) mqtt.Message {
	t.Helper()
	select {
	case m := <-sub.C():
		return m
	case <-time.After(d):
		t.Fatal("timed out waiting for message")
		return mqtt.Message{}
	}
}

func TestSendSingleMessage(t *testing.T) {
	b := useMock(t)
	listener := b.Dial()
	defer func() { _ = listener.Close() }()
	sub, err := listener.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	var out, errBuf bytes.Buffer
	code := runSend(strings.NewReader(""), &out, &errBuf,
		[]string{"--topic", "a/b", "--payload", "hello", "--qos", "0"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %s)", code, errBuf.String())
	}
	m := recvWithin(t, sub, time.Second)
	if m.Topic != "a/b" || string(m.Payload) != "hello" {
		t.Errorf("got topic=%q payload=%q, want a/b/hello", m.Topic, m.Payload)
	}
}

func TestSendStreamSink(t *testing.T) {
	b := useMock(t)
	listener := b.Dial()
	defer func() { _ = listener.Close() }()
	sub, err := listener.Subscribe("#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Two relay.Message values as NDJSON on stdin.
	line := func(topic, payload string) string {
		rm := mqtt.Message{Topic: topic, Payload: []byte(payload)}.ToMessage()
		bs, _ := json.Marshal(rm)
		return string(bs)
	}
	stdin := line("x/1", "one") + "\n" + line("x/2", "two") + "\n"

	var out, errBuf bytes.Buffer
	code := runSend(strings.NewReader(stdin), &out, &errBuf, []string{"--format", "json"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %s)", code, errBuf.String())
	}

	got := map[string]string{}
	for i := 0; i < 2; i++ {
		m := recvWithin(t, sub, time.Second)
		got[m.Topic] = string(m.Payload)
	}
	if got["x/1"] != "one" || got["x/2"] != "two" {
		t.Errorf("received = %v, want x/1=one x/2=two", got)
	}
}

func TestSendStreamSinkInvalidLine(t *testing.T) {
	useMock(t)
	var out, errBuf bytes.Buffer
	code := runSend(strings.NewReader("{not json\n"), &out, &errBuf, []string{"--format", "json"})
	if code != 1 {
		t.Errorf("exit = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "invalid relay.Message line") {
		t.Errorf("stderr = %q, want invalid-line error", errBuf.String())
	}
}

func TestSendBadQoS(t *testing.T) {
	useMock(t)
	var out, errBuf bytes.Buffer
	code := runSend(strings.NewReader(""), &out, &errBuf, []string{"--topic", "a", "--qos", "7"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestSubscribeNDJSON(t *testing.T) {
	b := useMock(t)
	// Retain a message so a fresh subscription receives it immediately.
	pub := b.Dial()
	if err := pub.Publish(context.Background(), "a/b", mqtt.AtMostOnce, []byte("retained-payload")); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	pubsub := b.Dial()
	defer func() { _ = pubsub.Close() }()

	var out, errBuf bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runSubscribe(context.Background(), &out, &errBuf,
			[]string{"--topic", "a/#", "--count", "1", "--format", "json"})
	}()

	// Give the subscriber a moment to attach, then publish.
	time.Sleep(50 * time.Millisecond)
	if err := pubsub.Publish(context.Background(), "a/b", mqtt.AtMostOnce, []byte("live")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit = %d, want 0 (stderr: %s)", code, errBuf.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe did not exit after --count messages")
	}

	var rm relay.Message
	firstLine := strings.SplitN(strings.TrimSpace(out.String()), "\n", 2)[0]
	if err := json.Unmarshal([]byte(firstLine), &rm); err != nil {
		t.Fatalf("output line is not a relay.Message: %v\noutput: %s", err, out.String())
	}
	if rm.ID != "a/b" {
		t.Errorf("ID = %q, want a/b", rm.ID)
	}
}

func TestSubscribeBadFormat(t *testing.T) {
	useMock(t)
	var out, errBuf bytes.Buffer
	code := runSubscribe(context.Background(), &out, &errBuf, []string{"--format", "xml"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestBrokerEndpointResolution(t *testing.T) {
	if got := brokerEndpoint("explicit:1883"); got != "explicit:1883" {
		t.Errorf("explicit flag: got %q", got)
	}
	t.Setenv("MQTT_BROKER", "env:1883")
	if got := brokerEndpoint(""); got != "env:1883" {
		t.Errorf("env: got %q", got)
	}
	t.Setenv("MQTT_BROKER", "")
	if got := brokerEndpoint(""); got != "localhost:1883" {
		t.Errorf("default: got %q", got)
	}
}
