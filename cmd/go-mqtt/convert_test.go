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

	relay "github.com/SoundMatt/RELAY"
)

func TestConvertGoldenVector(t *testing.T) {
	in := `{"topic":"sensors/temp","payload":"MjEuNQ==","qos":1,"retained":true}`
	var out, errBuf bytes.Buffer
	code := runConvert(strings.NewReader(in), &out, &errBuf, []string{"--protocol", "MQTT", "--format", "json"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %s)", code, errBuf.String())
	}

	var rm relay.Message
	if err := json.Unmarshal(out.Bytes(), &rm); err != nil {
		t.Fatalf("output is not a relay.Message: %v\noutput: %s", err, out.String())
	}
	if rm.Protocol != relay.MQTT {
		t.Errorf("Protocol = %v, want relay.MQTT", rm.Protocol)
	}
	if rm.ID != "sensors/temp" {
		t.Errorf("ID = %q, want %q", rm.ID, "sensors/temp")
	}
	if rm.Meta["mqtt.qos"] != "1" || rm.Meta["mqtt.retained"] != "true" {
		t.Errorf("Meta = %v, want qos=1 retained=true", rm.Meta)
	}
	if !rm.Timestamp.IsZero() {
		t.Errorf("Timestamp = %v, want zero (normalised for comparison)", rm.Timestamp)
	}
	// Output must be indented JSON terminated by a newline (matches the reference).
	if !bytes.HasSuffix(out.Bytes(), []byte("}\n")) {
		t.Errorf("output not newline-terminated indented JSON: %q", out.String())
	}
}

func TestConvertErrors(t *testing.T) {
	tests := []struct {
		name     string
		stdin    string
		args     []string
		wantCode int
		wantErr  string // substring expected on stderr
	}{
		{"empty topic", `{"topic":"","qos":0}`, []string{"--protocol", "MQTT"}, 1, "ErrTopicEmpty"},
		{"bad qos", `{"topic":"a/b","qos":9}`, []string{"--protocol", "MQTT"}, 1, "ErrQoSUnsupported"},
		{"invalid json", `{not json`, []string{"--protocol", "MQTT"}, 1, "invalid canonical value"},
		{"missing protocol", `{}`, nil, 2, "--protocol is required"},
		{"wrong protocol", `{}`, []string{"--protocol", "CAN"}, 2, "unsupported protocol"},
		{"bad format", `{"topic":"a/b"}`, []string{"--protocol", "MQTT", "--format", "xml"}, 2, "unsupported format"},
		{"bad flag", `{}`, []string{"--nope"}, 2, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			code := runConvert(strings.NewReader(tc.stdin), &out, &errBuf, tc.args)
			if code != tc.wantCode {
				t.Errorf("exit = %d, want %d (stderr: %s)", code, tc.wantCode, errBuf.String())
			}
			if tc.wantErr != "" && !strings.Contains(errBuf.String(), tc.wantErr) {
				t.Errorf("stderr = %q, want substring %q", errBuf.String(), tc.wantErr)
			}
			if out.Len() != 0 {
				t.Errorf("stdout = %q, want empty on error", out.String())
			}
		})
	}
}
