// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	mqtt "github.com/SoundMatt/go-mqtt"
)

func TestRunVersionText(t *testing.T) {
	var out bytes.Buffer
	runVersion(&out, nil)
	s := out.String()
	if !strings.Contains(s, "tool:         "+toolName) {
		t.Errorf("version text missing tool name: %q", s)
	}
	if !strings.Contains(s, "spec_version: "+mqtt.SpecVersion) {
		t.Errorf("version text missing spec_version %q: %q", mqtt.SpecVersion, s)
	}
}

func TestRunVersionJSON(t *testing.T) {
	var out bytes.Buffer
	runVersion(&out, []string{"--format", "json"})
	var doc map[string]any
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("version json invalid: %v", err)
	}
	if doc["tool"] != toolName {
		t.Errorf("tool = %v, want %s", doc["tool"], toolName)
	}
	if doc["protocol"] != "MQTT" || doc["spec_version"] != mqtt.SpecVersion {
		t.Errorf("version json fields wrong: %v", doc)
	}
}

func TestRunCapabilities(t *testing.T) {
	var out bytes.Buffer
	runCapabilities(&out)
	var doc map[string]any
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("capabilities json invalid: %v", err)
	}
	if doc["kind"] != "capabilities" {
		t.Errorf("kind = %v, want capabilities", doc["kind"])
	}
	cmds, ok := doc["commands"].([]any)
	if !ok {
		t.Fatalf("commands not an array: %v", doc["commands"])
	}
	// convert must be advertised (tooling-conformance).
	found := false
	for _, c := range cmds {
		if c == "convert" {
			found = true
		}
	}
	if !found {
		t.Errorf("capabilities commands missing convert: %v", cmds)
	}
}

func TestRunStatusText(t *testing.T) {
	var out bytes.Buffer
	runStatus(&out, nil)
	if !strings.Contains(out.String(), "healthy:   true") {
		t.Errorf("status text missing healthy: %q", out.String())
	}
}

func TestRunStatusJSON(t *testing.T) {
	var out bytes.Buffer
	runStatus(&out, []string{"--format", "json"})
	var doc map[string]any
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("status json invalid: %v", err)
	}
	if doc["healthy"] != true || doc["connected"] != false {
		t.Errorf("status json wrong: %v", doc)
	}
}
