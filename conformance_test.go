// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mqtt_test

// Conformance tests against the RELAY golden reference vectors (spec §15.7,
// RELAY spec/vectors/). The fixture in testdata/mqtt-message.json is a verbatim
// copy of github.com/SoundMatt/RELAY spec/vectors/mqtt-message.json; this test
// verifies go-mqtt's Message JSON form and ToMessage/FromMessage mapping match
// the canonical vector.

import (
	"encoding/json"
	"os"
	"testing"

	relay "github.com/SoundMatt/RELAY"
	mqtt "github.com/SoundMatt/go-mqtt"
)

type mqttMessageVector struct {
	Name    string        `json:"name"`
	Value   mqtt.Message  `json:"value"`
	Message relay.Message `json:"message"`
}

func loadVector(t *testing.T) mqttMessageVector {
	t.Helper()
	data, err := os.ReadFile("testdata/mqtt-message.json")
	if err != nil {
		t.Fatalf("read vector: %v", err)
	}
	var v mqttMessageVector
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("unmarshal vector: %v", err)
	}
	return v
}

// TestVectorMessageDecodes verifies the canonical "value" JSON decodes into the
// expected mqtt.Message (field tags and types match the spec §15.4 schema).
//
//fusa:test REQ-RELAY-001
func TestVectorMessageDecodes(t *testing.T) {
	v := loadVector(t)
	if v.Value.Topic != "sensors/temp" {
		t.Errorf("Topic = %q, want sensors/temp", v.Value.Topic)
	}
	if string(v.Value.Payload) != "21.5" {
		t.Errorf("Payload = %q, want 21.5", v.Value.Payload)
	}
	if v.Value.QoS != mqtt.AtLeastOnce {
		t.Errorf("QoS = %v, want AtLeastOnce", v.Value.QoS)
	}
	if !v.Value.Retained {
		t.Error("Retained = false, want true")
	}
}

// TestVectorToMessage verifies Message.ToMessage produces the canonical
// relay.Message envelope (timestamp is implementation-set and excluded).
//
//fusa:test REQ-RELAY-008
func TestVectorToMessage(t *testing.T) {
	v := loadVector(t)
	got := v.Value.ToMessage()

	if got.Protocol != relay.MQTT {
		t.Errorf("Protocol = %v, want relay.MQTT", got.Protocol)
	}
	if got.ID != v.Message.ID {
		t.Errorf("ID = %q, want %q", got.ID, v.Message.ID)
	}
	if string(got.Payload) != string(v.Message.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, v.Message.Payload)
	}
	for k, want := range v.Message.Meta {
		if got.Meta[k] != want {
			t.Errorf("Meta[%q] = %q, want %q", k, got.Meta[k], want)
		}
	}
}

// TestVectorFromMessage verifies FromMessage reconstructs the canonical Message
// from the relay.Message envelope.
//
//fusa:test REQ-RELAY-009
func TestVectorFromMessage(t *testing.T) {
	v := loadVector(t)
	got, err := mqtt.FromMessage(v.Message)
	if err != nil {
		t.Fatalf("FromMessage: %v", err)
	}
	if got.Topic != v.Value.Topic {
		t.Errorf("Topic = %q, want %q", got.Topic, v.Value.Topic)
	}
	if string(got.Payload) != string(v.Value.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, v.Value.Payload)
	}
	if got.QoS != v.Value.QoS {
		t.Errorf("QoS = %v, want %v", got.QoS, v.Value.QoS)
	}
	if got.Retained != v.Value.Retained {
		t.Errorf("Retained = %v, want %v", got.Retained, v.Value.Retained)
	}
}

// TestMockSatisfiesRelayOptional verifies that, with the §9 types now aliased to
// RELAY's, an mqtt optional-interface implementer also satisfies the RELAY
// interface directly.
//
//fusa:test REQ-RELAY-011
//fusa:test REQ-RELAY-013
func TestMockSatisfiesRelayOptional(t *testing.T) {
	// mqtt.Health is identical to relay.Health, etc. A compile-time assertion:
	var _ relay.HealthProvider = healthOnly{}
	var _ relay.MetricsProvider = metricsOnly{}
}

type healthOnly struct{}

func (healthOnly) Health() mqtt.Health { return mqtt.Health{Status: mqtt.HealthOK} }

type metricsOnly struct{}

func (metricsOnly) Metrics() mqtt.Metrics { return mqtt.Metrics{} }
