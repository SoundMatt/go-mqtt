// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package mqtt defines the Go interface for MQTT publish/subscribe operations.
//
// The API is intentionally narrow: it covers the primitives needed for
// vehicle-signal transport and general IoT messaging.
//
// Choose an implementation by importing one of the sub-packages:
//
//	import "github.com/SoundMatt/go-mqtt/mock" // in-process broker, no network
//	import "github.com/SoundMatt/go-mqtt/v3"   // MQTT v3.1.1 TCP client
//
// Both expose a constructor that satisfies this package's Client interface.
package mqtt

//fusa:req REQ-MSG-001
//fusa:req REQ-MSG-002
//fusa:req REQ-MSG-003
//fusa:req REQ-MSG-004
//fusa:req REQ-MSG-005
//fusa:req REQ-V5-MSG-001
//fusa:req REQ-V5-MSG-002
//fusa:req REQ-V5-MSG-003
//fusa:req REQ-V5-MSG-004
//fusa:req REQ-V5-MSG-005
//fusa:req REQ-QOS-001
//fusa:req REQ-QOS-002
//fusa:req REQ-QOS-003
//fusa:req REQ-QOS-004
//fusa:req REQ-SUB-003
//fusa:req REQ-SUB-004
//fusa:req REQ-SUB-005
//fusa:req REQ-WILD-001
//fusa:req REQ-WILD-002
//fusa:req REQ-WILD-003
//fusa:req REQ-WILD-004
//fusa:req REQ-WILD-005
//fusa:req REQ-WILD-006
//fusa:req REQ-WILD-007
//fusa:req REQ-WILD-008
//fusa:req REQ-RELAY-001
//fusa:req REQ-RELAY-002
//fusa:req REQ-RELAY-003
//fusa:req REQ-RELAY-004
//fusa:req REQ-RELAY-005
//fusa:req REQ-RELAY-006
//fusa:req REQ-RELAY-007
//fusa:req REQ-RELAY-008
//fusa:req REQ-RELAY-009

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	relay "github.com/SoundMatt/RELAY"
)

// SpecVersion is the RELAY specification version this package implements.
//
//fusa:req REQ-RELAY-001
const SpecVersion = "0.2"

// ── Sentinel errors ───────────────────────────────────────────────────────────

//fusa:req REQ-RELAY-002
//fusa:req REQ-RELAY-003

// ErrClosed is returned when an operation is called on a closed entity.
var ErrClosed = fmt.Errorf("mqtt: closed: %w", relay.ErrClosed)

// ErrNotConnected is returned when a network client is not connected to a broker.
var ErrNotConnected = fmt.Errorf("mqtt: not connected: %w", relay.ErrNotConnected)

// ErrTimeout is returned when an operation does not complete within the permitted time.
var ErrTimeout = fmt.Errorf("mqtt: timeout: %w", relay.ErrTimeout)

// ErrPayloadTooLarge is returned when a payload exceeds the broker limit.
var ErrPayloadTooLarge = fmt.Errorf("mqtt: payload too large: %w", relay.ErrPayloadTooLarge)

// ErrTopicEmpty is returned when an empty topic string is passed.
var ErrTopicEmpty = fmt.Errorf("mqtt: topic must not be empty: %w", relay.ErrNotConnected)

// ErrQoSUnsupported is returned when a QoS level is not supported.
var ErrQoSUnsupported = fmt.Errorf("mqtt: QoS level not supported: %w", relay.ErrNotConnected)

// ── QoS ──────────────────────────────────────────────────────────────────────

//fusa:req REQ-QOS-001
//fusa:req REQ-QOS-002
//fusa:req REQ-QOS-003
//fusa:req REQ-QOS-004

// QoS is the MQTT Quality of Service delivery guarantee.
type QoS int

const (
	// AtMostOnce (QoS 0) — fire-and-forget. No acknowledgement. Messages may
	// be lost if the network or broker fails.
	AtMostOnce QoS = 0

	// AtLeastOnce (QoS 1) — acknowledged delivery. The message is delivered at
	// least once; duplicates are possible.
	AtLeastOnce QoS = 1

	// ExactlyOnce (QoS 2) — exactly-once delivery. Highest overhead. Use for
	// actuator commands where duplicates cause incorrect behaviour.
	ExactlyOnce QoS = 2
)

// ── Message ───────────────────────────────────────────────────────────────────

//fusa:req REQ-MSG-001
//fusa:req REQ-MSG-002
//fusa:req REQ-MSG-003
//fusa:req REQ-MSG-004
//fusa:req REQ-MSG-005
//fusa:req REQ-V5-MSG-001
//fusa:req REQ-V5-MSG-002
//fusa:req REQ-V5-MSG-003
//fusa:req REQ-V5-MSG-004
//fusa:req REQ-V5-MSG-005

// UserProperty is an MQTT v5 user-defined key/value property pair.
//
//fusa:req REQ-V5-MSG-003
type UserProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Message is a single MQTT publish message delivered to a Subscription.
type Message struct {
	// Topic is the MQTT topic the message was published on.
	Topic string `json:"topic"`
	// Payload is the raw message bytes.
	Payload []byte `json:"payload"`
	// QoS is the quality of service level of this message.
	QoS QoS `json:"qos"`
	// Retained indicates the broker sent this as a retained message.
	Retained bool `json:"retained,omitempty"`
	// PacketID is non-zero for QoS 1 and QoS 2 messages.
	PacketID uint16 `json:"packet_id,omitempty"`

	// MQTT v5 properties — zero values mean "not set".
	ResponseTopic   string         `json:"response_topic,omitempty"`   // REQ-V5-PUB-002
	CorrelationData []byte         `json:"correlation_data,omitempty"` // REQ-V5-PUB-002
	UserProperties  []UserProperty `json:"user_properties,omitempty"`  // REQ-V5-PUB-001
	ContentType     string         `json:"content_type,omitempty"`
	ExpiryInterval  uint32         `json:"expiry_interval,omitempty"` // seconds; 0 = no expiry
}

// ToMessage converts this MQTT message to a relay.Message envelope.
//
//fusa:req REQ-RELAY-008
func (m Message) ToMessage() relay.Message {
	return relay.Message{
		Protocol:  relay.MQTT,
		ID:        m.Topic,
		Payload:   m.Payload,
		Timestamp: time.Now(),
		Meta: map[string]string{
			"mqtt.qos":      strconv.Itoa(int(m.QoS)),
			"mqtt.retained": strconv.FormatBool(m.Retained),
		},
	}
}

// FromMessage converts a relay.Message envelope to an MQTT Message.
//
//fusa:req REQ-RELAY-009
func FromMessage(msg relay.Message) (Message, error) {
	m := Message{
		Topic:   msg.ID,
		Payload: msg.Payload,
	}
	if v, ok := msg.Meta["mqtt.qos"]; ok {
		switch v {
		case "1":
			m.QoS = AtLeastOnce
		case "2":
			m.QoS = ExactlyOnce
		}
	}
	if msg.Meta["mqtt.retained"] == "true" {
		m.Retained = true
	}
	return m, nil
}

// ── Back-pressure ─────────────────────────────────────────────────────────────

//fusa:req REQ-RELAY-004

// BackPressurePolicy controls what happens when a subscription channel is full.
type BackPressurePolicy int

const (
	DropNewest BackPressurePolicy = iota // drop the arriving message (default)
	DropOldest                           // drop the oldest buffered message
	Block                                // block until space is available
)

// ── Subscription options ──────────────────────────────────────────────────────

//fusa:req REQ-SUB-004
//fusa:req REQ-SUB-005
//fusa:req REQ-RELAY-005

// SubscriberConfig holds per-subscription options applied at creation time.
type SubscriberConfig struct {
	// ChannelDepth is the capacity of the subscription's internal channel.
	// 0 means the implementation default (64).
	ChannelDepth int
	// BackPressure controls what happens when the channel is full.
	// Default is DropNewest.
	BackPressure BackPressurePolicy
}

// SubscriberOption configures a subscription at creation time.
type SubscriberOption func(*SubscriberConfig)

// WithChannelDepth sets the capacity of the subscription's message channel.
// A depth of 0 uses the implementation default (64).
func WithChannelDepth(n int) SubscriberOption {
	return func(c *SubscriberConfig) { c.ChannelDepth = n }
}

// WithBackPressure sets the back-pressure policy applied when the channel is full.
//
//fusa:req REQ-RELAY-006
func WithBackPressure(p BackPressurePolicy) SubscriberOption {
	return func(c *SubscriberConfig) { c.BackPressure = p }
}

// ApplySubscriberOpts merges a slice of SubscriberOption into a SubscriberConfig.
func ApplySubscriberOpts(opts []SubscriberOption) SubscriberConfig {
	var c SubscriberConfig
	for _, o := range opts {
		o(&c)
	}
	return c
}

// ChanDepth returns the resolved channel depth: cfg.ChannelDepth if > 0,
// otherwise the provided default.
func (c SubscriberConfig) ChanDepth(defaultDepth int) int {
	if c.ChannelDepth > 0 {
		return c.ChannelDepth
	}
	return defaultDepth
}

// ── Interfaces ────────────────────────────────────────────────────────────────

//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-002
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-CONN-008
//fusa:req REQ-CONC-001
//fusa:req REQ-CONC-002
//fusa:req REQ-CONC-003

// Client connects to an MQTT broker and provides publish/subscribe operations.
// A Client is safe for concurrent use from multiple goroutines.
type Client interface {
	// Publish sends a message to topic at the given QoS level.
	// Returns ErrTopicEmpty if topic is empty, ErrClosed if the client is closed,
	// or ErrQoSUnsupported if the implementation does not support qos.
	Publish(ctx context.Context, topic string, qos QoS, payload []byte) error

	// Subscribe creates a Subscription on topic filter with the given QoS.
	// topic may contain MQTT wildcard characters '+' and '#'.
	// Returns ErrTopicEmpty if topic is empty, ErrClosed if the client is closed.
	Subscribe(topic string, qos QoS, opts ...SubscriberOption) (Subscription, error)

	// Close releases all resources held by the client.
	Close() error
}

// Subscription delivers messages from a subscribed topic filter.
// A Subscription is safe for concurrent use from multiple goroutines.
type Subscription interface {
	// C returns the channel on which messages are delivered.
	// The channel is closed when the subscription or client is closed.
	C() <-chan Message

	// Unsubscribe removes this subscription from the broker without closing
	// the channel. No new messages will be delivered after Unsubscribe returns.
	Unsubscribe() error

	// Close unsubscribes and closes the message channel.
	Close() error
}

// ── Topic wildcard matching ───────────────────────────────────────────────────

// MatchTopic reports whether filter matches topic per MQTT §4.7.
//
// filter may contain '+' (matches exactly one topic level) and '#' (matches
// zero or more topic levels, must be the last character). Topics beginning
// with '$' are not matched by wildcards at the top level, per §4.7.2.
//
//fusa:req REQ-WILD-001
//fusa:req REQ-WILD-002
//fusa:req REQ-WILD-003
//fusa:req REQ-WILD-004
//fusa:req REQ-WILD-005
//fusa:req REQ-WILD-006
//fusa:req REQ-WILD-007
//fusa:req REQ-WILD-008
func MatchTopic(filter, topic string) bool {
	if filter == topic {
		return true
	}

	// '$' system topics are not matched by bare '#' or '+' at the first level.
	topicIsSystem := strings.HasPrefix(topic, "$")

	// '#' alone — matches all non-system topics.
	if filter == "#" {
		return !topicIsSystem
	}

	// 'filter/subtree/#' — matches filter/subtree and anything beneath it.
	if strings.HasSuffix(filter, "/#") {
		prefix := filter[:len(filter)-2]
		if topicIsSystem && !strings.HasPrefix(prefix, "$") {
			return false
		}
		return topic == prefix || strings.HasPrefix(topic, prefix+"/")
	}

	// No '#' — match level-by-level with '+' as single-level wildcard.
	fParts := strings.Split(filter, "/")
	tParts := strings.Split(topic, "/")
	if len(fParts) != len(tParts) {
		return false
	}
	for i, f := range fParts {
		if f == "+" {
			// '+' at the first level does not match '$' topics.
			if i == 0 && topicIsSystem {
				return false
			}
			continue
		}
		if f != tParts[i] {
			return false
		}
	}
	return true
}
