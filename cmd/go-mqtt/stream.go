// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	relay "github.com/SoundMatt/RELAY"
	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/v3"
)

// dialFunc is the broker dialer, overridable in tests.
var dialFunc = func(addr string) (mqtt.Client, error) { return v3.Dial(addr) }

// brokerEndpoint resolves the broker address from an explicit flag, then the
// MQTT_BROKER environment variable, then a localhost default.
func brokerEndpoint(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if env := os.Getenv("MQTT_BROKER"); env != "" {
		return env
	}
	return "localhost:1883"
}

// runSend implements the §11.2 `send` command in two forms:
//
//	send --topic T --payload P [--qos 0|1|2] [--endpoint addr]   (single message)
//	send --format json [--endpoint addr]                          (streaming sink)
//
// The streaming JSON sink reads relay.Message values as NDJSON on stdin (one per
// line) and publishes each until EOF — the egress dual of `subscribe --format
// json` and the portable sink used by `relay crossbar`. Exit: 0 sent, 1 error.
func runSend(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	format := fs.String("format", "text", "output format: text|json (json reads an NDJSON stream on stdin)")
	endpoint := fs.String("endpoint", "", "broker endpoint (default: $MQTT_BROKER or localhost:1883)")
	topic := fs.String("topic", "", "topic to publish to (single-message form)")
	payload := fs.String("payload", "", "message payload (single-message form)")
	qos := fs.Int("qos", 0, "QoS level: 0|1|2 (single-message form)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "send: %v\n", err)
		return 2
	}
	if *qos < 0 || *qos > 2 {
		fmt.Fprintf(stderr, "send: invalid --qos %d (want 0, 1, or 2)\n", *qos)
		return 2
	}

	client, err := dialFunc(brokerEndpoint(*endpoint))
	if err != nil {
		fmt.Fprintf(stderr, "send: connect: %v\n", err)
		return 1
	}
	defer func() { _ = client.Close() }()

	if *format == "json" {
		return sendStream(stdin, stderr, client)
	}

	if *topic == "" {
		fmt.Fprintln(stderr, "send: --topic is required (or use --format json for the streaming sink)")
		return 2
	}
	if err := client.Publish(context.Background(), *topic, mqtt.QoS(*qos), []byte(*payload)); err != nil {
		fmt.Fprintf(stderr, "send: publish: %v\n", err)
		return 1
	}
	return 0
}

// sendStream reads relay.Message NDJSON from r and publishes each via client.
func sendStream(r io.Reader, stderr io.Writer, client mqtt.Client) int {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rm relay.Message
		if err := json.Unmarshal([]byte(line), &rm); err != nil {
			fmt.Fprintf(stderr, "send: invalid relay.Message line: %v\n", err)
			return 1
		}
		m, err := mqtt.FromMessage(rm)
		if err != nil {
			fmt.Fprintf(stderr, "send: from message: %v\n", err)
			return 1
		}
		if err := client.Publish(context.Background(), m.Topic, m.QoS, m.Payload); err != nil {
			fmt.Fprintf(stderr, "send: publish: %v\n", err)
			return 1
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(stderr, "send: read stdin: %v\n", err)
		return 1
	}
	return 0
}

// runSubscribe implements the §11.2 `subscribe` command:
//
//	subscribe --topic FILTER [--format json] [--count N] [--qos 0|1|2] [--endpoint addr]
//
// It subscribes and prints each received message as relay.Message NDJSON on
// stdout. --count N exits after N messages; omitting it runs until the context
// is cancelled (SIGINT). Exit: 0 clean, 1 error.
func runSubscribe(ctx context.Context, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("subscribe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	format := fs.String("format", "json", "output format: json")
	endpoint := fs.String("endpoint", "", "broker endpoint (default: $MQTT_BROKER or localhost:1883)")
	topic := fs.String("topic", "#", "topic filter to subscribe to")
	qos := fs.Int("qos", 0, "QoS level: 0|1|2")
	count := fs.Int("count", 0, "exit after N messages (0 = run until SIGINT)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "subscribe: %v\n", err)
		return 2
	}
	if *format != "json" {
		fmt.Fprintf(stderr, "subscribe: unsupported format %q\n", *format)
		return 2
	}
	if *qos < 0 || *qos > 2 {
		fmt.Fprintf(stderr, "subscribe: invalid --qos %d (want 0, 1, or 2)\n", *qos)
		return 2
	}

	client, err := dialFunc(brokerEndpoint(*endpoint))
	if err != nil {
		fmt.Fprintf(stderr, "subscribe: connect: %v\n", err)
		return 1
	}
	defer func() { _ = client.Close() }()

	sub, err := client.Subscribe(*topic, mqtt.QoS(*qos))
	if err != nil {
		fmt.Fprintf(stderr, "subscribe: %v\n", err)
		return 1
	}
	defer func() { _ = sub.Close() }()

	enc := json.NewEncoder(stdout) // compact, one object per line — NDJSON
	n := 0
	for {
		select {
		case <-ctx.Done():
			return 0
		case m, ok := <-sub.C():
			if !ok {
				return 0
			}
			if err := enc.Encode(m.ToMessage()); err != nil {
				fmt.Fprintf(stderr, "subscribe: encode: %v\n", err)
				return 1
			}
			n++
			if *count > 0 && n >= *count {
				return 0
			}
		}
	}
}
