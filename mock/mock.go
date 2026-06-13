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

//fusa:req REQ-MOCK-001
//fusa:req REQ-MOCK-002
//fusa:req REQ-CLIENT-001
//fusa:req REQ-CLIENT-002
//fusa:req REQ-CLIENT-003
//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-002
//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-MSG-001
//fusa:req REQ-MSG-002

import (
	"context"
	"sync"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// Broker is an in-process MQTT broker. Create one with New and connect
// clients with Dial. A Broker is safe for concurrent use.
type Broker struct {
	mu       sync.RWMutex
	retained map[string]mqtt.Message       // retained messages by topic
	subs     map[string][]*mockSubscription // filter → subscriptions
}

// New creates a new in-process Broker ready for use.
func New() *Broker {
	return &Broker{
		retained: make(map[string]mqtt.Message),
		subs:     make(map[string][]*mockSubscription),
	}
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
		select {
		case sub.ch <- m:
		default:
		}
	}

	return sub, nil
}

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
}

func (s *mockSubscription) C() <-chan mqtt.Message { return s.ch }

func (s *mockSubscription) Unsubscribe() error {
	s.broker.deregister(s)
	return nil
}

func (s *mockSubscription) Close() error {
	s.Unsubscribe()
	s.once.Do(func() { close(s.ch) })
	return nil
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

func (b *Broker) route(msg mqtt.Message) {
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
		select {
		case sub.ch <- msg:
		default: // drop if channel is full
		}
	}
}
