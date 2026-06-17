// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqtt

//fusa:req REQ-RELAY-007

import (
	"context"

	relay "github.com/SoundMatt/RELAY"
)

type nodeAdapter struct {
	c Client
}

// Adapt wraps c as a relay.Node for protocol-agnostic use (RELAY spec §10.3).
//
//fusa:req REQ-RELAY-007
func Adapt(c Client) relay.Node {
	return &nodeAdapter{c: c}
}

func (a *nodeAdapter) Protocol() relay.Protocol {
	return relay.MQTT
}

// Send publishes msg to the MQTT broker. msg.ID is used as the topic.
// The QoS level is read from msg.Meta["mqtt.qos"] (default: AtMostOnce).
func (a *nodeAdapter) Send(ctx context.Context, msg relay.Message) error {
	qos := AtMostOnce
	switch msg.Meta["mqtt.qos"] {
	case "1":
		qos = AtLeastOnce
	case "2":
		qos = ExactlyOnce
	}
	return a.c.Publish(ctx, msg.ID, qos, msg.Payload)
}

// Subscribe returns a channel of all inbound relay.Message values.
// The adapter subscribes to "#" (all non-system topics) on the underlying client.
func (a *nodeAdapter) Subscribe(opts ...relay.SubscriberOption) (<-chan relay.Message, error) {
	cfg := relay.ApplySubscriberOpts(opts)
	depth := cfg.ChanDepth(64)

	sub, err := a.c.Subscribe("#", AtMostOnce)
	if err != nil {
		return nil, err
	}

	ch := make(chan relay.Message, depth)

	go func() {
		defer close(ch)
		for m := range sub.C() {
			msg := m.ToMessage()
			switch cfg.BackPressure {
			case relay.DropOldest:
				select {
				case ch <- msg:
				default:
					<-ch
					ch <- msg
				}
			case relay.Block:
				ch <- msg
			default: // DropNewest
				select {
				case ch <- msg:
				default:
				}
			}
		}
	}()

	return ch, nil
}

func (a *nodeAdapter) Close() error {
	return a.c.Close()
}
