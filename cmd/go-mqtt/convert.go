// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// runConvert implements the §11.2 `convert` driver:
//
//	convert --protocol MQTT [--format json]
//
// It reads one canonical mqtt.Message value as JSON on stdin, runs it through
// this implementation's own ToMessage() conversion (the same code path used at
// runtime, so the output is a faithful witness of real behaviour), and writes
// the resulting relay.Message as JSON on stdout with a zeroed timestamp so the
// result is byte-comparable across implementations (the cross-language equality
// oracle used by `relay interop`).
//
// Exit codes: 0 converted, 1 invalid input, 2 invalid args.
func runConvert(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("convert", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	protocol := fs.String("protocol", "", "protocol of the canonical value (MQTT)")
	format := fs.String("format", "json", "output format: json")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "convert: %v\n", err)
		return 2
	}
	if *protocol == "" {
		fmt.Fprintln(stderr, "convert: --protocol is required")
		return 2
	}
	if strings.ToUpper(*protocol) != "MQTT" {
		fmt.Fprintf(stderr, "convert: unsupported protocol %q (want MQTT)\n", *protocol)
		return 2
	}
	if *format != "json" {
		fmt.Fprintf(stderr, "convert: unsupported format %q\n", *format)
		return 2
	}

	value, err := io.ReadAll(stdin)
	if err != nil {
		fmt.Fprintf(stderr, "convert: read stdin: %v\n", err)
		return 1
	}

	var m mqtt.Message
	if jerr := json.Unmarshal(value, &m); jerr != nil {
		fmt.Fprintf(stderr, "convert: invalid canonical value: %v\n", jerr)
		return 1
	}
	if name, ok := validateMessage(m); !ok {
		// Write the §5 sentinel error name to stderr per the convert contract.
		fmt.Fprintln(stderr, name)
		return 1
	}

	msg := m.ToMessage()
	msg.Timestamp = time.Time{} // normalise for cross-implementation comparison

	out, err := json.MarshalIndent(msg, "", "    ")
	if err != nil {
		fmt.Fprintf(stderr, "convert: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, string(out))
	return 0
}

// validateMessage applies this implementation's runtime validity rules to a
// decoded mqtt.Message and, on failure, returns the §5 sentinel error name that
// the convert contract requires on stderr.
func validateMessage(m mqtt.Message) (sentinel string, ok bool) {
	if m.Topic == "" {
		return "ErrTopicEmpty", false
	}
	if m.QoS < mqtt.AtMostOnce || m.QoS > mqtt.ExactlyOnce {
		return "ErrQoSUnsupported", false
	}
	return "", true
}
