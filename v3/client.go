// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package v3 provides a pure-Go MQTT v3.1.1 TCP client.
//
// Connect to any MQTT broker (Mosquitto, HiveMQ, EMQX, …) with Dial:
//
//	client, err := v3.Dial("broker:1883",
//	    v3.WithClientID("my-sensor"),
//	    v3.WithKeepalive(30*time.Second),
//	)
//	if err != nil { ... }
//	defer client.Close()
//
//	sub, _ := client.Subscribe("Vehicle/#", mqtt.AtMostOnce)
//	client.Publish(ctx, "Vehicle/Speed", mqtt.AtMostOnce, []byte(`{"speed":60}`))
//	msg := <-sub.C()
//
// The client supports QoS 0 (AtMostOnce) and QoS 1 (AtLeastOnce). QoS 2 is
// not implemented in v0.1; use AtLeastOnce for acknowledged delivery.
package v3

//fusa:req REQ-CONN-001
//fusa:req REQ-CONN-002
//fusa:req REQ-CONN-003
//fusa:req REQ-CONN-004
//fusa:req REQ-CONN-005
//fusa:req REQ-CONN-006
//fusa:req REQ-CONN-007
//fusa:req REQ-CONN-008
//fusa:req REQ-CONN-009
//fusa:req REQ-CONN-010
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
//fusa:req REQ-MSG-001
//fusa:req REQ-MSG-002
//fusa:req REQ-MSG-003
//fusa:req REQ-MSG-004
//fusa:req REQ-MSG-005
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

// Option configures a v3 Client.
type Option func(*options)

type options struct {
	clientID  string
	keepalive time.Duration
	dialTimeout time.Duration
}

// WithClientID sets the MQTT client identifier sent in the CONNECT packet.
// Defaults to a timestamp-based ID if not set.
func WithClientID(id string) Option {
	return func(o *options) { o.clientID = id }
}

// WithKeepalive sets the MQTT keepalive interval. The client sends a PINGREQ
// after each interval if no other packet has been sent. Default: 30s.
func WithKeepalive(d time.Duration) Option {
	return func(o *options) { o.keepalive = d }
}

// WithDialTimeout sets the TCP dial timeout. Default: 10s.
func WithDialTimeout(d time.Duration) Option {
	return func(o *options) { o.dialTimeout = d }
}

// Dial connects to the MQTT broker at addr (e.g. "localhost:1883") and
// returns a Client ready for publish/subscribe operations.
//
// Dial performs the CONNECT/CONNACK handshake before returning. The connection
// uses CleanSession=true and no authentication.
//
//fusa:req REQ-CONN-001
//fusa:req REQ-CONN-002
//fusa:req REQ-CONN-003
//fusa:req REQ-CONN-004
//fusa:req REQ-CONN-005
func Dial(addr string, opts ...Option) (mqtt.Client, error) {
	o := &options{
		clientID:    fmt.Sprintf("go-mqtt-%d", time.Now().UnixNano()),
		keepalive:   30 * time.Second,
		dialTimeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(o)
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), o.dialTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqtt/v3: dial %s: %w", addr, err)
	}

	c := &v3Client{
		conn: conn,
		opts: o,
		subs: make(map[string][]*v3Subscription),
		done: make(chan struct{}),
	}

	if err := c.send(buildCONNECT(o.clientID, uint16(o.keepalive.Seconds()))); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mqtt/v3: send CONNECT: %w", err)
	}
	if err := c.readCONNACK(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mqtt/v3: CONNACK: %w", err)
	}

	go c.readLoop()
	if o.keepalive > 0 {
		go c.pingLoop()
	}
	return c, nil
}

// v3Client implements mqtt.Client over a TCP connection.
type v3Client struct {
	conn   net.Conn
	opts   *options
	mu     sync.RWMutex
	subs   map[string][]*v3Subscription
	done   chan struct{}
	once   sync.Once
	sendMu sync.Mutex
	pktID  atomic.Uint32
}

func (c *v3Client) nextID() uint16 {
	id := c.pktID.Add(1) & 0xFFFF
	if id == 0 {
		id = c.pktID.Add(1) & 0xFFFF
	}
	return uint16(id)
}

func (c *v3Client) send(pkt []byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	_, err := c.conn.Write(pkt)
	return err
}

//fusa:req REQ-CONN-002
//fusa:req REQ-CONN-005
func (c *v3Client) readCONNACK() error {
	// CONNACK: fixed header 0x20, remaining length 0x02, flags byte, return code
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		return err
	}
	if hdr[0] != pktCONNACK {
		return fmt.Errorf("expected CONNACK (0x%02x), got 0x%02x", pktCONNACK, hdr[0])
	}
	if hdr[3] != 0x00 {
		return fmt.Errorf("broker refused connection with return code 0x%02x", hdr[3])
	}
	return nil
}

//fusa:req REQ-PUB-001
//fusa:req REQ-PUB-002
//fusa:req REQ-PUB-003
//fusa:req REQ-PUB-004
//fusa:req REQ-PUB-005
//fusa:req REQ-PUB-006
//fusa:req REQ-SAFETY-001
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-004
//fusa:req REQ-ORDER-002
func (c *v3Client) Publish(ctx context.Context, topic string, qos mqtt.QoS, payload []byte) error {
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
	pkt := buildPUBLISH(topic, payload, byte(qos), false, packetID)
	return c.send(pkt)
}

//fusa:req REQ-SUB-001
//fusa:req REQ-SUB-002
//fusa:req REQ-SUB-003
//fusa:req REQ-SUB-004
//fusa:req REQ-SUB-006
//fusa:req REQ-SAFETY-002
//fusa:req REQ-SAFETY-003
//fusa:req REQ-SAFETY-005
func (c *v3Client) Subscribe(topic string, qos mqtt.QoS, opts ...mqtt.SubscriberOption) (mqtt.Subscription, error) {
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
	sub := &v3Subscription{
		filter: topic,
		ch:     make(chan mqtt.Message, cfg.ChanDepth(64)),
		client: c,
	}

	c.mu.Lock()
	c.subs[topic] = append(c.subs[topic], sub)
	c.mu.Unlock()

	if err := c.send(buildSUBSCRIBE(topic, byte(qos), c.nextID())); err != nil {
		c.removeSubscription(sub)
		return nil, fmt.Errorf("mqtt/v3: SUBSCRIBE: %w", err)
	}
	return sub, nil
}

//fusa:req REQ-CONN-006
//fusa:req REQ-CONN-007
//fusa:req REQ-CONN-008
//fusa:req REQ-SAFETY-007
func (c *v3Client) Close() error {
	var connErr error
	c.once.Do(func() {
		close(c.done)
		_ = c.send(disconnect)
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

func (c *v3Client) removeSubscription(sub *v3Subscription) {
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
func (c *v3Client) readLoop() {
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
			// QoS 1 ACK — no in-flight tracking in v0.1
		case pktSUBACK & 0xF0:
			// SUBACK — no pending-subscribe verification in v0.1
		case pktUNSUBACK & 0xF0:
			// UNSUBACK — acknowledged
		case pktPINGRESP & 0xF0:
			// keepalive response — no action needed
		}
	}
}

//fusa:req REQ-MSG-001
//fusa:req REQ-MSG-003
//fusa:req REQ-MSG-004
//fusa:req REQ-MSG-005
//fusa:req REQ-SUB-007
//fusa:req REQ-SUB-008
//fusa:req REQ-SAFETY-008
//fusa:req REQ-LEAK-003
//fusa:req REQ-ORDER-001
func (c *v3Client) handlePUBLISH(hdr byte, body []byte) {
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
	offset := 2 + topicLen

	var packetID uint16
	if qos == mqtt.AtLeastOnce || qos == mqtt.ExactlyOnce {
		if len(body) < offset+2 {
			return
		}
		packetID = uint16(body[offset])<<8 | uint16(body[offset+1])
		offset += 2
		if qos == mqtt.AtLeastOnce {
			_ = c.send(buildPUBACK(packetID))
		}
	}

	if offset > len(body) {
		return
	}
	payload := make([]byte, len(body)-offset)
	copy(payload, body[offset:])

	msg := mqtt.Message{
		Topic:    topic,
		Payload:  payload,
		QoS:      qos,
		Retained: retain,
		PacketID: packetID,
	}

	c.mu.RLock()
	var matched []*v3Subscription
	for filter, subs := range c.subs {
		if mqtt.MatchTopic(filter, topic) {
			matched = append(matched, subs...)
		}
	}
	c.mu.RUnlock()

	for _, sub := range matched {
		select {
		case sub.ch <- msg:
		default: // drop if full
		}
	}
}

//fusa:req REQ-CONN-009
//fusa:req REQ-CONN-010
func (c *v3Client) pingLoop() {
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

// v3Subscription implements mqtt.Subscription.
type v3Subscription struct {
	filter string
	ch     chan mqtt.Message
	client *v3Client
	mu     sync.Mutex
	closed bool
}

func (s *v3Subscription) C() <-chan mqtt.Message { return s.ch }

func (s *v3Subscription) Unsubscribe() error {
	s.client.removeSubscription(s)
	_ = s.client.send(buildUNSUBSCRIBE(s.filter, s.client.nextID()))
	return nil
}

func (s *v3Subscription) Close() error {
	_ = s.Unsubscribe()
	s.closeOnce()
	return nil
}

func (s *v3Subscription) closeOnce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}
