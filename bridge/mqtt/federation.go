// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package mqttbridge forwards messages between two MQTT brokers — a portable Go
// equivalent of Mosquitto's built-in bridge feature.
//
// A Bridge subscribes to a set of topic filters on a source client and
// republishes each matching message to a destination client, optionally
// capping the QoS and remapping the topic prefix. For bidirectional federation,
// create two Bridges (A→B and B→A) or use Pair.
//
//	local := mock.New().Dial()
//	remote, _ := v3.Dial("remote:1883")
//	b := mqttbridge.New(local, remote, mqttbridge.Route{
//	    Filters: []string{"Vehicle/#"},
//	    MaxQoS:  mqtt.AtLeastOnce,
//	})
//	b.Start()
//	defer b.Stop()
//
// Per RELAY spec §6.10, a Bridge does NOT reconnect automatically. When a source
// subscription channel closes (its client dropped), forwarding for that route
// stops; the application is responsible for creating a new Bridge.
package mqttbridge

//fusa:req REQ-FED-001
//fusa:req REQ-FED-002
//fusa:req REQ-FED-003
//fusa:req REQ-FED-004
//fusa:req REQ-FED-005
//fusa:req REQ-FED-006
//fusa:req REQ-FED-007
//fusa:req REQ-FED-008

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// Route describes one directional forwarding rule.
//
//fusa:req REQ-FED-001
type Route struct {
	// Filters are the MQTT topic filters to subscribe to on the source.
	Filters []string
	// MaxQoS caps the QoS of forwarded messages. The forwarded QoS is the
	// minimum of the message's QoS and MaxQoS. NOTE: the zero value
	// (AtMostOnce) downgrades every forwarded message to QoS 0; set MaxQoS to
	// ExactlyOnce to preserve the original QoS.
	MaxQoS mqtt.QoS
	// StripPrefix, if non-empty, is removed from the topic before republishing
	// (only when the topic begins with it, on a topic-level boundary).
	StripPrefix string
	// AddPrefix, if non-empty, is prepended to the topic before republishing.
	AddPrefix string
}

// remap applies StripPrefix and AddPrefix to a topic.
//
//fusa:req REQ-FED-005
func (r Route) remap(topic string) string {
	t := topic
	if r.StripPrefix != "" {
		switch {
		case t == r.StripPrefix:
			t = ""
		case strings.HasPrefix(t, r.StripPrefix+"/"):
			t = t[len(r.StripPrefix)+1:]
		}
	}
	if r.AddPrefix != "" {
		if t == "" {
			t = r.AddPrefix
		} else {
			t = r.AddPrefix + "/" + t
		}
	}
	return t
}

// forwardQoS returns the QoS to use when republishing a message.
//
//fusa:req REQ-FED-004
func (r Route) forwardQoS(msgQoS mqtt.QoS) mqtt.QoS {
	if msgQoS < r.MaxQoS {
		return msgQoS
	}
	return r.MaxQoS
}

// Stats holds forwarding counters for a Bridge.
//
//fusa:req REQ-FED-007
type Stats struct {
	Forwarded uint64 // messages republished to the destination
	Dropped   uint64 // messages that failed to republish
}

// Bridge forwards messages from a source client to a destination client.
//
//fusa:req REQ-FED-002
type Bridge struct {
	src, dst mqtt.Client
	routes   []Route

	mu      sync.Mutex
	subs    []mqtt.Subscription
	done    chan struct{}
	wg      sync.WaitGroup
	started bool

	forwarded atomic.Uint64
	dropped   atomic.Uint64
}

// New creates a Bridge forwarding from src to dst according to routes. Call
// Start to begin forwarding.
//
//fusa:req REQ-FED-002
func New(src, dst mqtt.Client, routes ...Route) *Bridge {
	return &Bridge{
		src:    src,
		dst:    dst,
		routes: routes,
		done:   make(chan struct{}),
	}
}

// Start subscribes to every route filter on the source and begins forwarding to
// the destination. It is idempotent: calling Start on a running Bridge is a
// no-op. Start returns ErrClosed if the source rejects a subscription.
//
//fusa:req REQ-FED-003
func (b *Bridge) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return nil
	}

	for _, route := range b.routes {
		for _, filter := range route.Filters {
			sub, err := b.src.Subscribe(filter, route.MaxQoS)
			if err != nil {
				// Roll back any subscriptions already made.
				for _, s := range b.subs {
					_ = s.Close()
				}
				b.subs = nil
				return err
			}
			b.subs = append(b.subs, sub)
			b.wg.Add(1)
			go b.forward(route, sub)
		}
	}
	b.started = true
	return nil
}

// forward reads from one subscription and republishes to the destination until
// the subscription channel closes or Stop is called.
//
//fusa:req REQ-FED-004
//fusa:req REQ-FED-005
//fusa:req REQ-FED-006
//fusa:req REQ-FED-008
func (b *Bridge) forward(route Route, sub mqtt.Subscription) {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			return
		case msg, ok := <-sub.C():
			if !ok {
				return // source subscription closed; no auto-reconnect (§6.10)
			}
			topic := route.remap(msg.Topic)
			if topic == "" {
				continue
			}
			err := b.dst.Publish(context.Background(), topic,
				route.forwardQoS(msg.QoS), msg.Payload)
			if err != nil {
				b.dropped.Add(1)
				continue
			}
			b.forwarded.Add(1)
		}
	}
}

// Stop ends forwarding and closes all source subscriptions. It does not close
// the source or destination clients. Stop is idempotent.
//
//fusa:req REQ-FED-006
func (b *Bridge) Stop() error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil
	}
	close(b.done)
	subs := b.subs
	b.subs = nil
	b.started = false
	b.mu.Unlock()

	for _, s := range subs {
		_ = s.Close()
	}
	b.wg.Wait()
	return nil
}

// Stats returns a snapshot of the forwarding counters.
//
//fusa:req REQ-FED-007
func (b *Bridge) Stats() Stats {
	return Stats{
		Forwarded: b.forwarded.Load(),
		Dropped:   b.dropped.Load(),
	}
}

// Pair creates two Bridges for bidirectional federation between a and b, with
// independent route sets for each direction. Start and Stop both returned
// bridges.
//
// To avoid forwarding loops, the two directions MUST use non-overlapping topic
// spaces: a message forwarded a→b must not match any bToA filter, or it will be
// echoed back indefinitely. Use distinct filters or prefix remapping per
// direction. A Bridge performs no loop detection.
//
//fusa:req REQ-FED-002
func Pair(a, b mqtt.Client, aToBRoutes, bToARoutes []Route) (aToB, bToA *Bridge) {
	return New(a, b, aToBRoutes...), New(b, a, bToARoutes...)
}
