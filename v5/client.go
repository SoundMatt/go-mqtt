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
// QoS 0 (AtMostOnce) and QoS 1 (AtLeastOnce) are supported. QoS 2 is not
// supported by this client — a permanent limitation, not a near-term gap —
// and Publish/PublishV5/Subscribe/SubscribeV5 return ErrQoSUnsupported for it.
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
	ackTimeout    time.Duration
	maxPacketSize uint32 // inbound cap (client-enforced) and advertised to the server
	topicAliasMax uint16 // 0 = does not accept inbound Topic Aliases (§3.1.2.3.4 default)
}

// defaultTopicAliasMax is the client's default advertised Topic Alias
// Maximum (§3.1.2.3.4): the number of distinct inbound topic-alias mappings
// the client will track. A conservative non-zero default lets alias
// resolution work out of the box while keeping the per-connection alias
// table bounded; callers with different needs can override via
// WithTopicAliasMax.
const defaultTopicAliasMax = 16

func defaultOptions() *options {
	return &options{
		clientID:      fmt.Sprintf("go-mqtt-v5-%d", time.Now().UnixNano()),
		keepalive:     30 * time.Second,
		dialTimeout:   10 * time.Second,
		ackTimeout:    10 * time.Second,
		maxPacketSize: mqtt.DefaultMaxInboundPacketSize,
		topicAliasMax: defaultTopicAliasMax,
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

// WithAckTimeout sets how long SubscribeV5 waits for a SUBACK, and PublishV5
// (QoS 1) waits for a PUBACK, before giving up with ErrTimeout. Default: 10s.
func WithAckTimeout(d time.Duration) Option { return func(o *options) { o.ackTimeout = d } }

// WithMaxPacketSize bounds the Remaining Length the client will accept on any
// single inbound packet (CONNACK or otherwise) before allocating a buffer for
// its body, and is sent to the server as the Maximum Packet Size property in
// CONNECT (§3.1.2.11) so a compliant server also self-limits. Default:
// mqtt.DefaultMaxInboundPacketSize (1 MiB). n == 0 disables the client's own
// cap check (readVarLen's own 4-byte wire ceiling of 268,435,455 still
// applies) and omits the CONNECT property.
func WithMaxPacketSize(n uint32) Option { return func(o *options) { o.maxPacketSize = n } }

// WithTopicAliasMax sets the Topic Alias Maximum this client advertises to
// the server in CONNECT (§3.1.2.3.4): the number of distinct Topic Alias
// values [1, n] the server may use in a PUBLISH to this client. Default: 16.
// n == 0 means the client does not accept any inbound Topic Alias at all —
// any inbound alias is then a Protocol Error and closes the connection.
func WithTopicAliasMax(n uint16) Option { return func(o *options) { o.topicAliasMax = n } }

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

	// pending SUBACK/PUBACK trackers keyed by packet ID, guarded by ackMu.
	// SubscribeV5 registers a channel here before sending SUBSCRIBE so the
	// SUBACK's per-filter Reason Code (§3.9) can be inspected instead of
	// assuming success; PublishV5 (QoS 1) does the same for the PUBACK
	// Reason Code (§3.4).
	ackMu       sync.Mutex
	pendingSubs map[uint16]chan []byte
	pendingPubs map[uint16]chan byte
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
	return dial(context.Background(), addr, opts...)
}

// New connects to the MQTT v5.0 broker at addr and returns a Client as the
// RELAY spec §8.4 mqtt.Client interface, per RELAY spec §7 (Constructor
// Contract, Form 1: endpoint-addressed). It is a context-aware alias for
// Dial: ctx bounds connection establishment (TCP dial and the
// CONNECT/CONNACK handshake). Callers needing v5-specific extensions
// (PublishV5, SubscribeV5, …) should call Dial directly for the concrete
// *Client type.
//
//fusa:req REQ-V5-CONN-001
//fusa:req REQ-V5-CONN-002
//fusa:req REQ-V5-CONN-003
//fusa:req REQ-V5-CONN-004
func New(ctx context.Context, addr string, opts ...Option) (mqtt.Client, error) {
	c, err := dial(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// dial is the shared implementation behind Dial and New.
func dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	// The client ID is encoded into CONNECT as a UTF-8 string with a 2-byte
	// length prefix (§1.5.4); reject an oversized value before dialing.
	if err := mqtt.CheckStringLen(o.clientID); err != nil {
		return nil, fmt.Errorf("mqtt/v5: client ID: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, o.dialTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqtt/v5: dial %s: %w", addr, err)
	}

	c := &Client{
		conn:        conn,
		opts:        o,
		subs:        make(map[string][]*v5Subscription),
		done:        make(chan struct{}),
		aliases:     make(map[uint16]string),
		pendingSubs: make(map[uint16]chan []byte),
		pendingPubs: make(map[uint16]chan byte),
	}

	if err := c.send(buildCONNECT(o.clientID, uint16(o.keepalive.Seconds()), o.sessionExpiry, o.receiveMax, o.maxPacketSize, o.topicAliasMax)); err != nil {
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
	// readVarLen only enforces the wire-format ceiling (268,435,455, §2.2.3);
	// bound the allocation itself against the configured cap before trusting
	// an untrusted broker's declared length (see DefaultMaxInboundPacketSize).
	if c.opts.maxPacketSize > 0 && remLen > int(c.opts.maxPacketSize) {
		return fmt.Errorf("mqtt/v5: CONNACK remaining length %d exceeds max packet size %d", remLen, c.opts.maxPacketSize)
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
	// An MQTT topic is a UTF-8 string with a 2-byte length prefix (§1.5.4):
	// a topic longer than MaxStringLen cannot be represented on the wire and
	// would truncate the length prefix, so reject it before encoding.
	if err := mqtt.CheckStringLen(topic); err != nil {
		return err
	}
	// ResponseTopic/ContentType are UTF-8 strings (§1.5.4) and CorrelationData
	// / each UserProperty key+value are also length-prefixed (§1.5.4, §1.5.6):
	// reject any oversized property field before it reaches encodeStr/encodeBin.
	if err := checkPublishPropsLen(props); err != nil {
		return err
	}
	// FitsRemainingLength checks the payload together with the topic and
	// (for QoS>0) packet-ID overhead that will also be encoded into the wire
	// Remaining Length — a bare payload-size check is not sufficient (see
	// FitsRemainingLength doc). This does not account for PublishProps
	// encoding size (variable, user-controlled via UserProperties); very
	// large property sets combined with a near-max payload can still
	// overflow the encoder — tracked separately, out of scope here.
	if !mqtt.FitsRemainingLength(topic, len(payload), qos != mqtt.AtMostOnce) {
		return mqtt.ErrPayloadTooLarge
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
	if qos == mqtt.AtMostOnce {
		return c.send(buildPUBLISH(topic, payload, byte(qos), false, 0, props))
	}

	// QoS 1: register the packet ID before sending so a PUBACK that arrives
	// on the readLoop goroutine before we start waiting is not missed, then
	// wait for its Reason Code (§3.4) — a broker refusal (0x80-0xFF, e.g.
	// 0x87 Not authorized, 0x97 Quota exceeded) must be surfaced to the
	// caller instead of being silently dropped as it was previously.
	packetID := c.nextID()
	ackCh := c.registerPubAck(packetID)
	if err := c.send(buildPUBLISH(topic, payload, byte(qos), false, packetID, props)); err != nil {
		c.dropPubAck(packetID)
		return err
	}
	reasonCode, err := c.waitPubAck(ctx, packetID, ackCh)
	if err != nil {
		return fmt.Errorf("mqtt/v5: PUBACK: %w", err)
	}
	if reasonCode >= reasonCodeFailure {
		return fmt.Errorf("mqtt/v5: PUBACK reason 0x%02x: %w", reasonCode, mqtt.ErrPublishRefused)
	}
	return nil
}

// registerPubAck records a pending PUBACK wait for packetID. Callers MUST
// call this before send()ing the PUBLISH so a fast PUBACK from the readLoop
// goroutine cannot arrive before the tracking entry exists.
func (c *Client) registerPubAck(packetID uint16) chan byte {
	ch := make(chan byte, 1)
	c.ackMu.Lock()
	c.pendingPubs[packetID] = ch
	c.ackMu.Unlock()
	return ch
}

func (c *Client) dropPubAck(packetID uint16) {
	c.ackMu.Lock()
	delete(c.pendingPubs, packetID)
	c.ackMu.Unlock()
}

// waitPubAck blocks until the PUBACK for packetID arrives, ctx is canceled,
// the client closes, or the configured ack timeout elapses (whichever is
// first), and always releases the tracking entry before returning.
func (c *Client) waitPubAck(ctx context.Context, packetID uint16, ch chan byte) (byte, error) {
	defer c.dropPubAck(packetID)
	timer := time.NewTimer(c.opts.ackTimeout)
	defer timer.Stop()
	select {
	case code := <-ch:
		return code, nil
	case <-c.done:
		return 0, mqtt.ErrClosed
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
		return 0, mqtt.ErrTimeout
	}
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
	// A topic filter is a UTF-8 string with a 2-byte length prefix (§1.5.4);
	// reject before encoding into SUBSCRIBE.
	if err := mqtt.CheckStringLen(topic); err != nil {
		return nil, err
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

	// Register the packet ID before sending so a fast SUBACK from the
	// readLoop goroutine cannot arrive before the tracking entry exists.
	packetID := c.nextID()
	ackCh := c.registerSubAck(packetID)

	if err := c.send(buildSUBSCRIBE(topic, byte(qos), packetID, sopts)); err != nil {
		c.dropSubAck(packetID)
		c.removeSubscription(sub)
		sub.closeOnce()
		return nil, fmt.Errorf("mqtt/v5: SUBSCRIBE: %w", err)
	}

	// Wait for the SUBACK and inspect its Reason Code (§3.9) instead of
	// reporting success as soon as the write completes: a broker refusal
	// (0x80-0xFF, e.g. 0x87 Not authorized, 0x8F Topic Filter invalid) must
	// not be indistinguishable from a granted subscription. On any failure
	// path below, the subscription is removed and its channel closed so it
	// is not left dangling — previously it stayed open, undelivered, and
	// unclosed until the whole client closed.
	reasonCodes, err := c.waitSubAck(packetID, ackCh)
	if err != nil {
		c.removeSubscription(sub)
		sub.closeOnce()
		return nil, fmt.Errorf("mqtt/v5: SUBACK: %w", err)
	}
	// This client always requests exactly one Topic Filter per SUBSCRIBE
	// (buildSUBSCRIBE takes a single filter), so exactly one Reason Code is
	// expected per §3.9; only the first is inspected.
	if reasonCodes[0] >= reasonCodeFailure {
		c.removeSubscription(sub)
		sub.closeOnce()
		return nil, fmt.Errorf("mqtt/v5: SUBACK reason 0x%02x: %w", reasonCodes[0], mqtt.ErrSubscribeRefused)
	}
	return sub, nil
}

// registerSubAck records a pending SUBACK wait for packetID. Callers MUST
// call this before send()ing the SUBSCRIBE so a fast SUBACK from the
// readLoop goroutine cannot arrive before the tracking entry exists.
func (c *Client) registerSubAck(packetID uint16) chan []byte {
	ch := make(chan []byte, 1)
	c.ackMu.Lock()
	c.pendingSubs[packetID] = ch
	c.ackMu.Unlock()
	return ch
}

func (c *Client) dropSubAck(packetID uint16) {
	c.ackMu.Lock()
	delete(c.pendingSubs, packetID)
	c.ackMu.Unlock()
}

// waitSubAck blocks until the SUBACK for packetID arrives, the client
// closes, or the configured ack timeout elapses (whichever is first), and
// always releases the tracking entry before returning. SubscribeV5 has no
// ctx parameter (matching the mqtt.Client.Subscribe interface), so unlike
// waitPubAck this has no ctx.Done() case.
func (c *Client) waitSubAck(packetID uint16, ch chan []byte) ([]byte, error) {
	defer c.dropSubAck(packetID)
	timer := time.NewTimer(c.opts.ackTimeout)
	defer timer.Stop()
	select {
	case codes := <-ch:
		return codes, nil
	case <-c.done:
		return nil, mqtt.ErrClosed
	case <-timer.C:
		return nil, mqtt.ErrTimeout
	}
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
		// readVarLen only enforces the wire-format ceiling (268,435,455,
		// §2.2.3); bound the allocation itself against the configured cap
		// before trusting an untrusted broker's declared length (see
		// DefaultMaxInboundPacketSize). A crafted or corrupt frame otherwise
		// forces a ~268MB allocation, repeatably, on every such frame.
		if c.opts.maxPacketSize > 0 && remLen > int(c.opts.maxPacketSize) {
			_ = c.send(buildDISCONNECTReason(0x95)) // Packet too large (§2.4)
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
			c.handlePUBACK(body)
		case pktSUBACK & 0xF0:
			c.handleSUBACK(body)
		case pktUNSUBACK & 0xF0:
			// acknowledged
		case pktPINGRESP & 0xF0:
			// keepalive response
		case pktDISCONNECT & 0xF0:
			return // broker-initiated disconnect
		}
	}
}

// handleSUBACK delivers a SUBACK's per-Topic-Filter Reason Codes (§3.9) to
// the SubscribeV5 call awaiting them, if any. A malformed SUBACK (short body,
// bad property block, or no reason codes) is dropped: it cannot be attributed
// to a pending SubscribeV5 call, which will instead time out.
//
//fusa:req REQ-V5-SUB-001
func (c *Client) handleSUBACK(body []byte) {
	packetID, codes, err := parseSUBACK(body)
	if err != nil {
		return
	}
	c.ackMu.Lock()
	ch := c.pendingSubs[packetID]
	c.ackMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- codes:
	default: // no one waiting (already timed out) — drop
	}
}

// handlePUBACK delivers a PUBACK's Reason Code (§3.4.2) to the PublishV5
// (QoS 1) call awaiting it, if any. A malformed PUBACK (short body) is
// dropped: it cannot be attributed to a pending PublishV5 call, which will
// instead time out.
//
//fusa:req REQ-V5-PUB-001
func (c *Client) handlePUBACK(body []byte) {
	packetID, reasonCode, err := parsePUBACK(body)
	if err != nil {
		return
	}
	c.ackMu.Lock()
	ch := c.pendingPubs[packetID]
	c.ackMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- reasonCode:
	default: // no one waiting (already timed out) — drop
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

	// Resolve topic alias per MQTT v5 §3.3.2.3.4. A Topic Alias of 0, or one
	// greater than the Topic Alias Maximum we advertised in CONNECT (0 if we
	// advertised none at all, i.e. WithTopicAliasMax(0) or the default when
	// disabled), is a Protocol Error: the server "MUST NOT send a Topic Alias
	// ... greater than the Topic Alias Maximum". Silently accepting it would
	// let an out-of-bound alias grow the alias table unboundedly and/or
	// resolve to a topic the client never agreed to track. §4.13 requires
	// closing the Network Connection on a Protocol Error, so send DISCONNECT
	// with reason 0x82 (Protocol Error) and stop reading — the closed conn
	// then makes readLoop's next io.ReadFull fail and return.
	if props.topicAlias != nil {
		alias := *props.topicAlias
		if alias == 0 || alias > c.opts.topicAliasMax {
			_ = c.send(buildDISCONNECTReason(0x82)) // Protocol Error
			_ = c.conn.Close()
			return
		}
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
