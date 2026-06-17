// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package mock provides an in-process MQTT broker for testing.
//
// All clients connected to the same Broker share an in-memory message bus.
// Publish on topic T is delivered synchronously to every Subscription whose
// filter matches T per MQTT §4.7.
//
// The mock is the default implementation used by unit tests. Switch to the
// v3 package to connect to a real MQTT broker over TCP.
//
//	broker := mock.New()
//	client := broker.Dial()
//	defer client.Close()
//
//	sub, _ := client.Subscribe("sensors/#", mqtt.AtMostOnce)
//	client.Publish(ctx, "sensors/temperature", mqtt.AtMostOnce, []byte(`{"temp":21}`))
//	msg := <-sub.C()
package mock

//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-SUB-004
//fusa:req REQ-SUB-005
//fusa:req REQ-SUB-007
//fusa:req REQ-SUB-008
//fusa:req REQ-CONN-008
//fusa:req REQ-SAFETY-001
//fusa:req REQ-SAFETY-002
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-004
//fusa:req REQ-SAFETY-005
//fusa:req REQ-SAFETY-008
//fusa:req REQ-MOCK-001
//fusa:req REQ-MOCK-002
//fusa:req REQ-MOCK-003
//fusa:req REQ-MOCK-004
//fusa:req REQ-MOCK-005
//fusa:req REQ-CONC-001
//fusa:req REQ-CONC-002
//fusa:req REQ-LEAK-002
//fusa:req REQ-LEAK-003

import (
	"context"
	"sync"
	"sync/atomic"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// Broker is an in-process MQTT broker. Create one with New and connect
// clients with Dial. A Broker is safe for concurrent use.
//
// Broker implements mqtt.HealthProvider, mqtt.MetricsProvider, and
// mqtt.Drainer per RELAY spec §9.
type Broker struct {
	mu       sync.RWMutex
	retained map[string]mqtt.Message        // retained messages by topic
	subs     map[string][]*mockSubscription // filter → subscriptions
	closed   atomic.Bool

	// metrics counters — updated atomically by route().
	writeCount     atomic.Uint64
	deliverCount   atomic.Uint64
	dropCount      atomic.Uint64
	bytesWritten   atomic.Uint64
	bytesDelivered atomic.Uint64
	errorCount     atomic.Uint64
}

// New creates a new in-process Broker ready for use.
func New() *Broker {
	return &Broker{
		retained: make(map[string]mqtt.Message),
		subs:     make(map[string][]*mockSubscription),
	}
}

// ── mqtt.HealthProvider ───────────────────────────────────────────────────────

//fusa:req REQ-RELAY-010
//fusa:req REQ-RELAY-011

// Health returns the current health of the broker (RELAY spec §9).
func (b *Broker) Health() mqtt.Health {
	if b.closed.Load() {
		return mqtt.Health{Status: mqtt.HealthDown, Details: "broker closed"}
	}
	return mqtt.Health{Status: mqtt.HealthOK}
}

// ── mqtt.MetricsProvider ──────────────────────────────────────────────────────

//fusa:req REQ-RELAY-012
//fusa:req REQ-RELAY-013

// Metrics returns runtime counters for this broker (RELAY spec §9).
func (b *Broker) Metrics() mqtt.Metrics {
	return mqtt.Metrics{
		WriteCount:     b.writeCount.Load(),
		DeliverCount:   b.deliverCount.Load(),
		DropCount:      b.dropCount.Load(),
		BytesWritten:   b.bytesWritten.Load(),
		BytesDelivered: b.bytesDelivered.Load(),
		ErrorCount:     b.errorCount.Load(),
	}
}

// ── mqtt.Drainer ──────────────────────────────────────────────────────────────

//fusa:req REQ-RELAY-014

// CloseWithDrain marks the broker as closed (RELAY spec §9). Since routing is
// synchronous, all in-flight deliveries are already complete when this returns.
func (b *Broker) CloseWithDrain(_ context.Context) error {
	b.closed.Store(true)
	return nil
}

// Dial creates and returns a new Client connected to this Broker.
func (b *Broker) Dial() mqtt.Client {
	return &mockClient{broker: b}
}

// ── mockClient ────────────────────────────────────────────────────────────────

type mockClient struct {
	broker *Broker
	mu     sync.Mutex
	closed bool
}

//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-SAFETY-001
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-004
func (c *mockClient) Publish(ctx context.Context, topic string, qos mqtt.QoS, payload []byte) error {
	if topic == "" {
		return mqtt.ErrTopicEmpty
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return mqtt.ErrClosed
	}
	c.mu.Unlock()

	p := make([]byte, len(payload))
	copy(p, payload)
	msg := mqtt.Message{Topic: topic, Payload: p, QoS: qos}
	c.broker.route(msg)
	return nil
}

//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-SUB-004
//fusa:req REQ-MOCK-001
//fusa:req REQ-SAFETY-002
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-005
func (c *mockClient) Subscribe(topic string, qos mqtt.QoS, opts ...mqtt.SubscriberOption) (mqtt.Subscription, error) {
	if topic == "" {
		return nil, mqtt.ErrTopicEmpty
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, mqtt.ErrClosed
	}
	c.mu.Unlock()

	cfg := mqtt.ApplySubscriberOpts(opts)
	sub := &mockSubscription{
		filter: topic,
		ch:     make(chan mqtt.Message, cfg.ChanDepth(64)),
		broker: c.broker,
	}
	c.broker.register(sub)

	// Deliver retained messages matching this filter.
	c.broker.mu.RLock()
	var retained []mqtt.Message
	for t, m := range c.broker.retained {
		if mqtt.MatchTopic(topic, t) {
			retained = append(retained, m)
		}
	}
	c.broker.mu.RUnlock()
	for _, m := range retained {
		sub.deliver(m)
	}

	return sub, nil
}

//fusa:req REQ-CONN-008
func (c *mockClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// ── mockSubscription ──────────────────────────────────────────────────────────

type mockSubscription struct {
	filter string
	ch     chan mqtt.Message
	broker *Broker
	once   sync.Once

	sendMu sync.Mutex // serialises deliver against Close
	closed bool       // guarded by sendMu
}

func (s *mockSubscription) C() <-chan mqtt.Message { return s.ch }

func (s *mockSubscription) Unsubscribe() error {
	s.broker.deregister(s)
	return nil
}

func (s *mockSubscription) Close() error {
	_ = s.Unsubscribe()
	s.sendMu.Lock()
	s.closed = true
	s.once.Do(func() { close(s.ch) })
	s.sendMu.Unlock()
	return nil
}

// deliver performs a non-blocking send to the subscription channel. It returns
// true if the message was delivered, false if dropped (channel full) or if the
// subscription has been closed. Holding sendMu makes deliver safe against a
// concurrent Close, preventing a send on a closed channel.
//
//fusa:req REQ-CONC-002
//fusa:req REQ-SAFETY-008
func (s *mockSubscription) deliver(msg mqtt.Message) bool {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.ch <- msg:
		return true
	default:
		return false
	}
}

// ── Broker routing ────────────────────────────────────────────────────────────

func (b *Broker) register(sub *mockSubscription) {
	b.mu.Lock()
	b.subs[sub.filter] = append(b.subs[sub.filter], sub)
	b.mu.Unlock()
}

func (b *Broker) deregister(sub *mockSubscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.subs[sub.filter]
	for i, s := range subs {
		if s == sub {
			b.subs[sub.filter] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

//fusa:req REQ-SUB-007
//fusa:req REQ-SUB-008
//fusa:req REQ-SAFETY-008
//fusa:req REQ-MOCK-002
//fusa:req REQ-MOCK-003
//fusa:req REQ-MOCK-004
//fusa:req REQ-LEAK-003
func (b *Broker) route(msg mqtt.Message) {
	size := uint64(len(msg.Payload))
	b.writeCount.Add(1)
	b.bytesWritten.Add(size)

	// Store/clear retained message.
	if msg.Retained {
		b.mu.Lock()
		if len(msg.Payload) == 0 {
			delete(b.retained, msg.Topic)
		} else {
			b.retained[msg.Topic] = msg
		}
		b.mu.Unlock()
	}

	b.mu.RLock()
	var matched []*mockSubscription
	for filter, subs := range b.subs {
		if mqtt.MatchTopic(filter, msg.Topic) {
			matched = append(matched, subs...)
		}
	}
	b.mu.RUnlock()

	for _, sub := range matched {
		if sub.deliver(msg) {
			b.deliverCount.Add(1)
			b.bytesDelivered.Add(size)
		} else {
			b.dropCount.Add(1)
		}
	}
}
