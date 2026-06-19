// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package rest exposes an MQTT client over HTTP so that web clients and tools
// that cannot speak MQTT directly can publish and subscribe.
//
// Endpoints (all topic/filter path segments may contain slashes):
//
//	POST /publish/{topic}    body = payload, optional ?qos=0|1|2 → MQTT PUBLISH
//	GET  /subscribe/{filter} → Server-Sent Events stream of matching messages
//	GET  /retain/{topic}     → the last retained message, or 404 if none
//
//	gw := rest.New(client)
//	http.ListenAndServe(":8080", gw.Handler())
package rest

//fusa:req REQ-REST-001
//fusa:req REQ-REST-002
//fusa:req REQ-REST-003
//fusa:req REQ-REST-004
//fusa:req REQ-REST-005
//fusa:req REQ-REST-006
//fusa:req REQ-REST-007
//fusa:req REQ-REST-008

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// Gateway maps HTTP requests onto an mqtt.Client.
//
//fusa:req REQ-REST-001
type Gateway struct {
	client       mqtt.Client
	retainWait   time.Duration
	maxBodyBytes int64
}

// Option configures a Gateway.
type Option func(*Gateway)

// WithRetainTimeout sets how long GET /retain/{topic} waits for the broker to
// deliver a retained message before responding 404. Default: 500ms.
//
//fusa:req REQ-REST-007
func WithRetainTimeout(d time.Duration) Option {
	return func(g *Gateway) { g.retainWait = d }
}

// WithMaxBody sets the maximum accepted publish body size in bytes. Default: 1 MiB.
//
//fusa:req REQ-REST-002
func WithMaxBody(n int64) Option {
	return func(g *Gateway) { g.maxBodyBytes = n }
}

// New returns a Gateway over client.
//
//fusa:req REQ-REST-001
func New(client mqtt.Client, opts ...Option) *Gateway {
	g := &Gateway{
		client:       client,
		retainWait:   500 * time.Millisecond,
		maxBodyBytes: 1 << 20,
	}
	for _, o := range opts {
		o(g)
	}
	return g
}

// Handler returns an http.Handler serving the gateway endpoints.
//
//fusa:req REQ-REST-001
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /publish/{topic...}", g.handlePublish)
	mux.HandleFunc("GET /subscribe/{filter...}", g.handleSubscribe)
	mux.HandleFunc("GET /retain/{topic...}", g.handleRetain)
	return mux
}

// parseQoS reads an optional ?qos= query parameter (0, 1, or 2; default 0).
//
//fusa:req REQ-REST-003
func parseQoS(r *http.Request) (mqtt.QoS, bool) {
	v := r.URL.Query().Get("qos")
	if v == "" {
		return mqtt.AtMostOnce, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > 2 {
		return 0, false
	}
	return mqtt.QoS(n), true
}

// handlePublish maps POST /publish/{topic} to an MQTT PUBLISH.
//
//fusa:req REQ-REST-002
//fusa:req REQ-REST-003
//fusa:req REQ-SEC-006
func (g *Gateway) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		http.Error(w, "empty topic", http.StatusBadRequest)
		return
	}
	qos, ok := parseQoS(r)
	if !ok {
		http.Error(w, "invalid qos (want 0, 1, or 2)", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, g.maxBodyBytes))
	if err != nil {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := g.client.Publish(r.Context(), topic, qos, body); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// handleSubscribe maps GET /subscribe/{filter} to a Server-Sent Events stream.
//
//fusa:req REQ-REST-004
//fusa:req REQ-REST-005
//fusa:req REQ-REST-006
func (g *Gateway) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	filter := r.PathValue("filter")
	if filter == "" {
		http.Error(w, "empty filter", http.StatusBadRequest)
		return
	}
	qos, ok := parseQoS(r)
	if !ok {
		http.Error(w, "invalid qos (want 0, 1, or 2)", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	sub, err := g.client.Subscribe(filter, qos)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = sub.Close() }()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, open := <-sub.C():
			if !open {
				return
			}
			if _, err := io.WriteString(w, "data: "); err != nil {
				return
			}
			if err := enc.Encode(msg); err != nil { // Encode writes a trailing newline
				return
			}
			if _, err := io.WriteString(w, "\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// handleRetain maps GET /retain/{topic} to the last retained message for that
// exact topic. It subscribes briefly and returns the retained message the broker
// replays, or 404 if none arrives within the retain timeout.
//
//fusa:req REQ-REST-007
//fusa:req REQ-REST-008
func (g *Gateway) handleRetain(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		http.Error(w, "empty topic", http.StatusBadRequest)
		return
	}
	sub, err := g.client.Subscribe(topic, mqtt.AtMostOnce)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = sub.Close() }()

	// A retained message, if any, is delivered first on subscribe and carries
	// Retained=true. Ignore any live (non-retained) traffic and respond 404 if
	// no retained message arrives within the timeout.
	timer := time.NewTimer(g.retainWait)
	defer timer.Stop()
	for {
		select {
		case <-r.Context().Done():
			http.Error(w, "client cancelled", http.StatusRequestTimeout)
			return
		case msg, open := <-sub.C():
			if !open {
				http.Error(w, "subscription closed", http.StatusBadGateway)
				return
			}
			if !msg.Retained {
				continue
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(msg)
			return
		case <-timer.C:
			http.Error(w, "no retained message", http.StatusNotFound)
			return
		}
	}
}
