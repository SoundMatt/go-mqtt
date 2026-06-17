// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package broker is a minimal in-process MQTT v3.1.1 broker. It is suitable for
// edge devices, test harnesses, and embedded Go runtimes where running a
// separate Mosquitto process is not feasible.
//
//	srv := broker.New()
//	go func() { _ = srv.ListenAndServe(":1883") }()
//	defer srv.Close()
//
// Any MQTT v3.1.1 client (including this module's v3 package) can then connect.
// The broker supports QoS 0/1/2 inbound, retained messages, last-will, topic
// wildcards (MQTT §4.7), and keepalive. Delivery to subscribers is at
// min(publish QoS, granted QoS) and capped at QoS 1 (a spec-permitted downgrade).
// Sessions are clean (no persistence across reconnect).
package broker

//fusa:req REQ-BROKER-001
//fusa:req REQ-BROKER-002
//fusa:req REQ-BROKER-003
//fusa:req REQ-BROKER-004
//fusa:req REQ-BROKER-005
//fusa:req REQ-BROKER-006
//fusa:req REQ-BROKER-007
//fusa:req REQ-BROKER-008
//fusa:req REQ-BROKER-009
//fusa:req REQ-BROKER-010

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// Server is an MQTT broker. The zero value is not usable; call New.
//
//fusa:req REQ-BROKER-001
type Server struct {
	mu        sync.RWMutex
	sessions  map[*session]struct{}
	retained  map[string][]byte // topic → retained payload (empty payload clears)
	retainQoS map[string]byte
	listeners map[net.Listener]struct{}
	closed    atomic.Bool

	tlsConfig *tls.Config

	writeCount     atomic.Uint64
	deliverCount   atomic.Uint64
	dropCount      atomic.Uint64
	bytesWritten   atomic.Uint64
	bytesDelivered atomic.Uint64
	errorCount     atomic.Uint64
}

// Option configures a Server.
type Option func(*Server)

// WithTLS makes ListenAndServe wrap accepted connections in TLS using cfg.
//
//fusa:req REQ-BROKER-009
func WithTLS(cfg *tls.Config) Option {
	return func(s *Server) { s.tlsConfig = cfg }
}

// New creates a Server.
//
//fusa:req REQ-BROKER-001
func New(opts ...Option) *Server {
	s := &Server{
		sessions:  make(map[*session]struct{}),
		retained:  make(map[string][]byte),
		retainQoS: make(map[string]byte),
		listeners: make(map[net.Listener]struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ListenAndServe opens a TCP listener on addr and serves until Close. When the
// server was created WithTLS, accepted connections are wrapped in TLS.
//
//fusa:req REQ-BROKER-002
//fusa:req REQ-BROKER-009
func (s *Server) ListenAndServe(addr string) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until the server is closed. It always returns
// a non-nil error; after Close that error is net.ErrClosed.
//
//fusa:req REQ-BROKER-002
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		_ = ln.Close()
		return net.ErrClosed
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return net.ErrClosed
			}
			return err
		}
		sess := &session{conn: conn, server: s, subs: make(map[string]byte), qos2In: make(map[uint16]qos2Pending)}
		go sess.serve()
	}
}

// Addr returns the address of the first active listener, or "" if none.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for ln := range s.listeners {
		return ln.Addr().String()
	}
	return ""
}

// Close stops all listeners and disconnects all sessions. It is idempotent.
//
//fusa:req REQ-BROKER-003
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.mu.Lock()
	for ln := range s.listeners {
		_ = ln.Close()
	}
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		_ = sess.conn.Close()
	}
	return nil
}

// ── §9 MetricsProvider ────────────────────────────────────────────────────────

// Metrics returns runtime counters for the broker (RELAY spec §9).
//
//fusa:req REQ-BROKER-010
func (s *Server) Metrics() mqtt.Metrics {
	return mqtt.Metrics{
		WriteCount:     s.writeCount.Load(),
		DeliverCount:   s.deliverCount.Load(),
		DropCount:      s.dropCount.Load(),
		BytesWritten:   s.bytesWritten.Load(),
		BytesDelivered: s.bytesDelivered.Load(),
		ErrorCount:     s.errorCount.Load(),
	}
}

// SubscriberCount returns the total number of active subscriptions across all
// sessions.
//
//fusa:req REQ-BROKER-010
func (s *Server) SubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for sess := range s.sessions {
		sess.mu.Lock()
		n += len(sess.subs)
		sess.mu.Unlock()
	}
	return n
}

// RetainedCount returns the number of topics holding a retained message.
//
//fusa:req REQ-BROKER-010
func (s *Server) RetainedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.retained)
}

// ── routing ───────────────────────────────────────────────────────────────────

// publish routes a message to every matching subscription and updates retained
// state. Called from a session that received a PUBLISH.
//
//fusa:req REQ-BROKER-006
//fusa:req REQ-BROKER-007
func (s *Server) publish(topic string, payload []byte, qos byte, retain bool) {
	s.writeCount.Add(1)
	s.bytesWritten.Add(uint64(len(payload)))

	if retain {
		s.mu.Lock()
		if len(payload) == 0 {
			delete(s.retained, topic)
			delete(s.retainQoS, topic)
		} else {
			cp := make([]byte, len(payload))
			copy(cp, payload)
			s.retained[topic] = cp
			s.retainQoS[topic] = qos
		}
		s.mu.Unlock()
	}

	s.mu.RLock()
	targets := make([]*session, 0)
	grants := make([]byte, 0)
	for sess := range s.sessions {
		if g, ok := sess.matchQoS(topic); ok {
			targets = append(targets, sess)
			grants = append(grants, g)
		}
	}
	s.mu.RUnlock()

	for i, sess := range targets {
		dq := qos
		if grants[i] < dq {
			dq = grants[i]
		}
		if dq > 1 { // cap outbound at QoS 1 (spec-permitted downgrade)
			dq = 1
		}
		if sess.deliver(topic, payload, dq, false) {
			s.deliverCount.Add(1)
			s.bytesDelivered.Add(uint64(len(payload)))
		} else {
			s.dropCount.Add(1)
		}
	}
}

// register adds a session to the broker's set.
func (s *Server) register(sess *session) {
	s.mu.Lock()
	s.sessions[sess] = struct{}{}
	s.mu.Unlock()
}

// unregister removes a session from the broker's set.
func (s *Server) unregister(sess *session) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
}

// retainedFor returns retained messages whose topic matches filter.
//
//fusa:req REQ-BROKER-007
func (s *Server) retainedFor(filter string) (topics []string, payloads [][]byte, qoss []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for topic, payload := range s.retained {
		if mqtt.MatchTopic(filter, topic) {
			topics = append(topics, topic)
			payloads = append(payloads, payload)
			qoss = append(qoss, s.retainQoS[topic])
		}
	}
	return topics, payloads, qoss
}
