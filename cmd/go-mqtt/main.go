// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Binary go-mqtt is the RELAY-conformant CLI for the go-mqtt library.
// It implements the mandatory RELAY CLI contract (spec §11).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"

	mqtt "github.com/SoundMatt/go-mqtt"
)

const (
	toolName    = "go-mqtt"
	toolVersion = "1.0.0"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <command>\ncommands: version, capabilities, status, convert, send, subscribe\n", toolName)
		os.Exit(2)
	}

	switch os.Args[1] {
	case "version":
		runVersion(os.Stdout, os.Args[2:])
	case "capabilities":
		runCapabilities(os.Stdout)
	case "status":
		runStatus(os.Stdout, os.Args[2:])
	case "convert":
		os.Exit(runConvert(os.Stdin, os.Stdout, os.Stderr, os.Args[2:]))
	case "send":
		os.Exit(runSend(os.Stdin, os.Stdout, os.Stderr, os.Args[2:]))
	case "subscribe":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		os.Exit(runSubscribe(ctx, os.Stdout, os.Stderr, os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(2)
	}
}

func runVersion(w io.Writer, args []string) {
	fs := flag.NewFlagSet("version", flag.ExitOnError)
	format := fs.String("format", "text", "output format: text|json")
	_ = fs.Parse(args)

	if *format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "    ")
		_ = enc.Encode(map[string]any{
			"tool":         toolName,
			"protocol":     "MQTT",
			"protocol_int": 4,
			"version":      toolVersion,
			"spec_version": mqtt.SpecVersion,
			"language":     "go",
			"runtime":      runtime.Version(),
		})
		return
	}
	fmt.Fprintf(w, "tool:         %s\nprotocol:     MQTT\nversion:      %s\nspec_version: %s\n",
		toolName, toolVersion, mqtt.SpecVersion)
}

func runCapabilities(w io.Writer) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "    ")
	_ = enc.Encode(map[string]any{
		"kind":                "capabilities",
		"tool":                toolName,
		"protocol":            "MQTT",
		"protocol_int":        4,
		"version":             toolVersion,
		"spec_version":        mqtt.SpecVersion,
		"commands":            []string{"version", "capabilities", "status", "convert", "send", "subscribe"},
		"transports":          []string{"tcp"},
		"features":            []string{},
		"interfaces":          []string{"Client", "Subscription"},
		"optional_interfaces": []string{"HealthProvider", "MetricsProvider", "Drainer"},
		"adapt":               true,
	})
}

func runStatus(w io.Writer, args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	format := fs.String("format", "text", "output format: text|json")
	_ = fs.Parse(args)

	if *format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "    ")
		_ = enc.Encode(map[string]any{
			"protocol":  "MQTT",
			"tool":      toolName,
			"version":   toolVersion,
			"healthy":   true,
			"connected": false,
			"endpoint":  "",
			"details":   map[string]any{},
		})
		return
	}
	fmt.Fprintf(w, "tool:      %s\nprotocol:  MQTT\nversion:   %s\nhealthy:   true\nconnected: false\n",
		toolName, toolVersion)
}
