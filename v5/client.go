// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package v5 provides a pure-Go MQTT v5.0 TCP client.
//
// Connect to any MQTT v5.0-capable broker (Mosquitto ≥2.0, HiveMQ, EMQX, …):
//
//	client, err := v5.Dial("broker:1883",
//	    v5.WithClientID("my-sensor"),
//	    v5.WithKeepalive(30*time.Second),
//	    v5.WithSessionExpiry(300), // keep session for 5 min after disconnect
//	)
//	if err != nil { ... }
//	defer func() { _ = client.Close() }()
//
//	// Basic publish (implements mqtt.Client):
//	client.Publish(ctx, "Vehicle/Speed", mqtt.AtMostOnce, []byte(`{"speed":60}`))
//
//	// v5 publish with properties:
//	client.PublishV5(ctx, "Vehicle/Speed", mqtt.AtMostOnce, payload, v5.PublishProps{
//	    ResponseTopic:   "Vehicle/Speed/Reply",
//	    CorrelationData: []byte("req-42"),
//	    UserProperties:  []mqtt.UserProperty{{Key: "unit", Value: "km/h"}},
//	})
//
//	// Subscribe (implements mqtt.Client):
//	sub, _ := client.Subscribe("Vehicle/#", mqtt.AtMostOnce)
//	msg := <-sub.C()
//
// QoS 0 (AtMostOnce) and QoS 1 (AtLeastOnce) are supported. QoS 2 returns
// ErrQoSUnsupported and will be added in v0.5.
package v5

//fusa:req REQ-V5-CONN-001
//fusa:req REQ-V5-CONN-002
//fusa:req REQ-V5-CONN-003
//fusa:req REQ-V5-CONN-004
//fusa:req REQ-V5-PUB-001
//fusa:req REQ-V5-PUB-002
//fusa:req REQ-V5-PUB-003
//fusa:req REQ-V5-PUB-004
//fusa:req REQ-V5-PUB-005
//fusa:req REQ-V5-PUB-006
//fusa:req REQ-V5-SUB-001
//fusa:req REQ-V5-SUB-002
//fusa:req REQ-V5-SUB-003
//fusa:req REQ-V5-SUB-004
//fusa:req REQ-V5-ALIAS-001
//fusa:req REQ-V5-ALIAS-002
//fusa:req REQ-V5-ALIAS-003
//fusa:req REQ-V5-SESSION-001
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
//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-002
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-PUB-005
//fusa:req REQ-PUB-006
//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-SUB-004
//fusa:req REQ-SUB-006
//fusa:req REQ-SUB-007
//fusa:req REQ-SUB-008
//fusa:req REQ-CONN-006
//fusa:req REQ-CONN-007
//fusa:req REQ-CONN-008
//fusa:req REQ-CONN-009
//fusa:req REQ-CONN-010
//fusa:req REQ-SAFETY-001
//fusa:req REQ-SAFETY-002
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-004
//fusa:req REQ-SAFETY-005
//fusa:req REQ-SAFETY-006
//fusa:req REQ-SAFETY-007
//fusa:req REQ-SAFETY-008
//fusa:req REQ-CONC-001
//fusa:req REQ-CONC-002
//fusa:req REQ-CONC-003
//fusa:req REQ-LEAK-001
//fusa:req REQ-LEAK-002
//fusa:req REQ-LEAK-003
//fusa:req REQ-ORDER-001
//fusa:req REQ-ORDER-002
//fusa:req REQ-FAULT-001
//fusa:req REQ-FAULT-002
//fusa:req REQ-FAULT-003

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// Option configures a v5 Client.
type Option func(*options)

type options struct {
	clientID      string
	keepalive     time.Duration
	dialTimeout   time.Duration
	sessionExpiry uint32 // 0 = session ends on disconnect
	receiveMax    uint16 // 0 = do not send (server default applies)
}

func defaultOptions() *options {
	return &options{
		clientID:    fmt.Sprintf("go-mqtt-v5-%d", time.Now().UnixNano()),
		keepalive:   30 * time.Second,
		dialTimeout: 10 * time.Second,
	}
}

// WithClientID sets the MQTT client identifier sent in the CONNECT packet.
func WithClientID(id string) Option { return func(o *options) { o.clientID = id } }

// WithKeepalive sets the MQTT keepalive interval. Default: 30s.
func WithKeepalive(d time.Duration) Option { return func(o *options) { o.keepalive = d } }

// WithDialTimeout sets the TCP dial timeout. Default: 10s.
func WithDialTimeout(d time.Duration) Option { return func(o *options) { o.dialTimeout = d } }

// WithSessionExpiry sets the Session Expiry Interval (seconds). When > 0 the
// broker retains the session state for that duration after disconnect.
// 0 means the session ends immediately on disconnect (CleanStart behaviour).
func WithSessionExpiry(secs uint32) Option { return func(o *options) { o.sessionExpiry = secs } }

// WithReceiveMax limits the number of in-flight QoS 1 messages the client
// will accept from the broker simultaneously. 0 means no client-side limit.
func WithReceiveMax(n uint16) Option { return func(o *options) { o.receiveMax = n } }

// Client is an MQTT v5.0 client. It implements mqtt.Client and adds v5
// extensions via PublishV5 and SubscribeV5.
//
// A Client is safe for concurrent use from multiple goroutines.
type Client struct {
	conn   net.Conn
	opts   *options
	mu     sync.RWMutex
	subs   map[string][]*v5Subscription
	done   chan struct{}
	once   sync.Once
	sendMu sync.Mutex
	pktID  atomic.Uint32

	// negotiated v5 values (set from CONNACK properties)
	serverTopicAliasMax uint16
	sessionPresent      bool

	// incoming topic alias table: alias → topic
	aliasMu sync.RWMutex
	aliases map[uint16]string
}

// Dial connects to the MQTT v5.0 broker at addr (e.g. "localhost:1883") and
// performs the CONNECT/CONNACK handshake before returning.
//
//fusa:req REQ-V5-CONN-001
//fusa:req REQ-V5-CONN-002
//fusa:req REQ-V5-CONN-003
//fusa:req REQ-V5-CONN-004
//fusa:req REQ-CONN-003
//fusa:req REQ-CONN-004
//fusa:req REQ-V5-SESSION-001
func Dial(addr string, opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), o.dialTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqtt/v5: dial %s: %w", addr, err)
	}

	c := &Client{
		conn:    conn,
		opts:    o,
		subs:    make(map[string][]*v5Subscription),
		done:    make(chan struct{}),
		aliases: make(map[uint16]string),
	}

	if err := c.send(buildCONNECT(o.clientID, uint16(o.keepalive.Seconds()), o.sessionExpiry, o.receiveMax)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mqtt/v5: send CONNECT: %w", err)
	}
	if err := c.readCONNACK(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mqtt/v5: CONNACK: %w", err)
	}

	go c.readLoop()
	if o.keepalive > 0 {
		go c.pingLoop()
	}
	return c, nil
}

func (c *Client) readCONNACK() error {
	var hdr [1]byte
	if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
		return err
	}
	if hdr[0] != pktCONNACK {
		return fmt.Errorf("expected CONNACK (0x%02x), got 0x%02x", pktCONNACK, hdr[0])
	}
	remLen, err := readVarLen(c.conn)
	if err != nil {
		return err
	}
	body := make([]byte, remLen)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return err
	}
	if len(body) < 2 {
		return fmt.Errorf("mqtt/v5: CONNACK too short (%d bytes)", len(body))
	}
	c.sessionPresent = body[0]&0x01 != 0
	if body[1] != 0x00 {
		return fmt.Errorf("mqtt/v5: broker refused with reason code 0x%02x", body[1])
	}
	if len(body) > 2 {
		props, _, err := readPropSet(body[2:])
		if err != nil {
			return fmt.Errorf("mqtt/v5: CONNACK properties: %w", err)
		}
		if props.topicAliasMax != nil {
			c.serverTopicAliasMax = *props.topicAliasMax
		}
		if props.serverKeepalive != nil && *props.serverKeepalive > 0 {
			c.opts.keepalive = time.Duration(*props.serverKeepalive) * time.Second
		}
	}
	return nil
}

//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-002
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-SAFETY-001
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-004
func (c *Client) Publish(ctx context.Context, topic string, qos mqtt.QoS, payload []byte) error {
	return c.PublishV5(ctx, topic, qos, payload, PublishProps{})
}

//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-002
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-PUB-005
//fusa:req REQ-PUB-006
//fusa:req REQ-V5-PUB-001
//fusa:req REQ-V5-PUB-002
//fusa:req REQ-V5-PUB-003
//fusa:req REQ-V5-PUB-004
//fusa:req REQ-V5-PUB-005
//fusa:req REQ-V5-PUB-006
//fusa:req REQ-SAFETY-001
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-004
//fusa:req REQ-ORDER-002
func (c *Client) PublishV5(ctx context.Context, topic string, qos mqtt.QoS, payload []byte, props PublishProps) error {
	if topic == "" {
		return mqtt.ErrTopicEmpty
	}
	if qos == mqtt.ExactlyOnce {
		return mqtt.ErrQoSUnsupported
	}
	select {
	case <-c.done:
		return mqtt.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	var packetID uint16
	if qos == mqtt.AtLeastOnce {
		packetID = c.nextID()
	}
	return c.send(buildPUBLISH(topic, payload, byte(qos), false, packetID, props))
}

//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-SUB-004
//fusa:req REQ-SAFETY-002
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-005
func (c *Client) Subscribe(topic string, qos mqtt.QoS, opts ...mqtt.SubscriberOption) (mqtt.Subscription, error) {
	return c.SubscribeV5(topic, qos, SubscribeOpts{}, opts...)
}

//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-SUB-004
//fusa:req REQ-SUB-006
//fusa:req REQ-V5-SUB-001
//fusa:req REQ-V5-SUB-002
//fusa:req REQ-V5-SUB-003
//fusa:req REQ-V5-SUB-004
//fusa:req REQ-SAFETY-002
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-005
func (c *Client) SubscribeV5(topic string, qos mqtt.QoS, sopts SubscribeOpts, opts ...mqtt.SubscriberOption) (mqtt.Subscription, error) {
	if topic == "" {
		return nil, mqtt.ErrTopicEmpty
	}
	if qos == mqtt.ExactlyOnce {
		return nil, mqtt.ErrQoSUnsupported
	}
	select {
	case <-c.done:
		return nil, mqtt.ErrClosed
	default:
	}

	cfg := mqtt.ApplySubscriberOpts(opts)
	sub := &v5Subscription{
		filter: topic,
		ch:     make(chan mqtt.Message, cfg.ChanDepth(64)),
		client: c,
	}

	c.mu.Lock()
	c.subs[topic] = append(c.subs[topic], sub)
	c.mu.Unlock()

	if err := c.send(buildSUBSCRIBE(topic, byte(qos), c.nextID(), sopts)); err != nil {
		c.removeSubscription(sub)
		return nil, fmt.Errorf("mqtt/v5: SUBSCRIBE: %w", err)
	}
	return sub, nil
}

//fusa:req REQ-CONN-006
//fusa:req REQ-CONN-007
//fusa:req REQ-CONN-008
//fusa:req REQ-SAFETY-007
func (c *Client) Close() error {
	var connErr error
	c.once.Do(func() {
		close(c.done)
		_ = c.send(buildDISCONNECT())
		connErr = c.conn.Close()
		c.mu.Lock()
		for _, subs := range c.subs {
			for _, sub := range subs {
				sub.closeOnce()
			}
		}
		c.mu.Unlock()
	})
	return connErr
}

func (c *Client) nextID() uint16 {
	id := c.pktID.Add(1) & 0xFFFF
	if id == 0 {
		id = c.pktID.Add(1) & 0xFFFF
	}
	return uint16(id)
}

func (c *Client) send(data []byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	_, err := c.conn.Write(data)
	return err
}

func (c *Client) removeSubscription(sub *v5Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	subs := c.subs[sub.filter]
	for i, s := range subs {
		if s == sub {
			c.subs[sub.filter] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

//fusa:req REQ-SAFETY-006
//fusa:req REQ-FAULT-002
//fusa:req REQ-FAULT-003
//fusa:req REQ-LEAK-001
func (c *Client) readLoop() {
	defer func() {
		c.mu.RLock()
		for _, subs := range c.subs {
			for _, sub := range subs {
				sub.closeOnce()
			}
		}
		c.mu.RUnlock()
	}()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		var hdrBuf [1]byte
		if _, err := io.ReadFull(c.conn, hdrBuf[:]); err != nil {
			return
		}
		hdr := hdrBuf[0]

		remLen, err := readVarLen(c.conn)
		if err != nil {
			return
		}
		body := make([]byte, remLen)
		if remLen > 0 {
			if _, err := io.ReadFull(c.conn, body); err != nil {
				return
			}
		}

		switch hdr & 0xF0 {
		case pktPUBLISH & 0xF0:
			c.handlePUBLISH(hdr, body)
		case pktPUBACK & 0xF0:
			// QoS 1 ACK — no in-flight tracking in v0.2
		case pktSUBACK & 0xF0:
			// SUBACK — no pending verification in v0.2
		case pktUNSUBACK & 0xF0:
			// acknowledged
		case pktPINGRESP & 0xF0:
			// keepalive response
		case pktDISCONNECT & 0xF0:
			return // broker-initiated disconnect
		}
	}
}

//fusa:req REQ-MSG-001
//fusa:req REQ-MSG-003
//fusa:req REQ-MSG-004
//fusa:req REQ-MSG-005
//fusa:req REQ-V5-MSG-001
//fusa:req REQ-V5-MSG-002
//fusa:req REQ-V5-MSG-003
//fusa:req REQ-V5-MSG-004
//fusa:req REQ-V5-MSG-005
//fusa:req REQ-SUB-007
//fusa:req REQ-SUB-008
//fusa:req REQ-SAFETY-008
//fusa:req REQ-V5-ALIAS-001
//fusa:req REQ-V5-ALIAS-002
//fusa:req REQ-V5-ALIAS-003
//fusa:req REQ-LEAK-003
//fusa:req REQ-ORDER-001
//fusa:req REQ-SEC-009
func (c *Client) handlePUBLISH(hdr byte, body []byte) {
	qos := mqtt.QoS((hdr >> 1) & 0x03)
	retain := hdr&0x01 != 0

	if len(body) < 2 {
		return
	}
	topicLen := int(body[0])<<8 | int(body[1])
	if len(body) < 2+topicLen {
		return
	}
	topic := string(body[2 : 2+topicLen])
	body = body[2+topicLen:]

	var packetID uint16
	if qos == mqtt.AtLeastOnce || qos == mqtt.ExactlyOnce {
		if len(body) < 2 {
			return
		}
		packetID = uint16(body[0])<<8 | uint16(body[1])
		body = body[2:]
		if qos == mqtt.AtLeastOnce {
			_ = c.send(buildPUBACK(packetID))
		}
	}

	props, remaining, err := readPropSet(body)
	if err != nil {
		return
	}

	// Resolve topic alias per MQTT v5 §3.3.2.3.4.
	if props.topicAlias != nil {
		alias := *props.topicAlias
		if topic != "" {
			c.aliasMu.Lock()
			c.aliases[alias] = topic
			c.aliasMu.Unlock()
		} else {
			c.aliasMu.RLock()
			topic = c.aliases[alias]
			c.aliasMu.RUnlock()
			if topic == "" {
				return // unknown alias; drop
			}
		}
	}
	if topic == "" {
		return
	}

	payload := make([]byte, len(remaining))
	copy(payload, remaining)

	msg := mqtt.Message{
		Topic:           topic,
		Payload:         payload,
		QoS:             qos,
		Retained:        retain,
		PacketID:        packetID,
		ResponseTopic:   props.responseTopic,
		CorrelationData: props.correlationData,
		ContentType:     props.contentType,
		UserProperties:  props.userProps,
	}
	if props.expiryInterval != nil {
		msg.ExpiryInterval = *props.expiryInterval
	}

	c.mu.RLock()
	var matched []*v5Subscription
	for filter, subs := range c.subs {
		if mqtt.MatchTopic(filter, topic) {
			matched = append(matched, subs...)
		}
	}
	c.mu.RUnlock()

	for _, sub := range matched {
		select {
		case sub.ch <- msg:
		default: // drop if channel is full
		}
	}
}

//fusa:req REQ-CONN-009
//fusa:req REQ-CONN-010
func (c *Client) pingLoop() {
	ticker := time.NewTicker(c.opts.keepalive)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			_ = c.send(pingReq)
		}
	}
}

// v5Subscription implements mqtt.Subscription.
type v5Subscription struct {
	filter string
	ch     chan mqtt.Message
	client *Client
	mu     sync.Mutex
	closed bool
}

func (s *v5Subscription) C() <-chan mqtt.Message { return s.ch }

func (s *v5Subscription) Unsubscribe() error {
	s.client.removeSubscription(s)
	_ = s.client.send(buildUNSUBSCRIBE(s.filter, s.client.nextID()))
	return nil
}

func (s *v5Subscription) Close() error {
	_ = s.Unsubscribe()
	s.closeOnce()
	return nil
}

func (s *v5Subscription) closeOnce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}
