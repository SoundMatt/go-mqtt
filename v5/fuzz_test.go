// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v5

import (
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// FuzzReadPropSet verifies that readPropSet never panics on arbitrary input.
func FuzzReadPropSet(f *testing.F) {
	// Seed corpus: empty props, session expiry, receive max, user property.
	f.Add([]byte{0})
	f.Add(encodeProps(propU32(propSessionExpiry, 300)))
	f.Add(encodeProps(propU16(propReceiveMax, 100)))
	f.Add(encodeProps(propUserProp("k", "v")))
	f.Add(encodeProps(
		propU16(propTopicAlias, 1),
		propStr(propResponseTopic, "reply/topic"),
		propBin(propCorrelationData, []byte("id")),
	))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic.
		_, _, _ = readPropSet(data)
	})
}

// FuzzBuildPublish verifies that buildPUBLISH never panics on arbitrary inputs.
func FuzzBuildPublish(f *testing.F) {
	f.Add("Vehicle/Speed", []byte(`{"speed":60}`), byte(0), false, uint16(0))
	f.Add("t", []byte{}, byte(1), true, uint16(1))

	f.Fuzz(func(t *testing.T, topic string, payload []byte, qos byte, retain bool, pktID uint16) {
		qos = qos % 2 // only QoS 0 and 1
		_ = buildPUBLISH(topic, payload, qos, retain, pktID, PublishProps{
			ResponseTopic:  topic,
			UserProperties: []mqtt.UserProperty{{Key: "k", Value: "v"}},
		})
	})
}
