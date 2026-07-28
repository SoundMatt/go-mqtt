// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build integration

package v3_test

// Third-party-peer interop: go-mqtt's own v3 client cross-checked against
// Eclipse Mosquitto's own `mosquitto_pub`/`mosquitto_sub` CLI tools, which
// ship with the broker and are an independent implementation of the wire
// protocol. This is the MQTT analogue of what CAN interop testing does with
// `cangen`/`candump` from can-utils, and of rust-DDS's live-peer interop
// against a real CycloneDDS process (ROADMAP.md's "Interop testing"
// section there): self-consistency between two go-mqtt clients only proves
// go-mqtt agrees with itself, not that it is wire-correct. An independent
// third-party tool talking to the same broker closes that gap.
//
// Two directions:
//
//   - TestIntegration_ThirdPartySubscriberVerifiesGoMqttPublish: publish via
//     go-mqtt's v3 client, receive via `mosquitto_sub`. Confirms go-mqtt's
//     PUBLISH encoding is wire-correct, not just self-consistent.
//   - TestIntegration_GoMqttSubscriberVerifiesThirdPartyPublish: publish via
//     `mosquitto_pub`, receive via go-mqtt's v3 client. Confirms go-mqtt's
//     SUBSCRIBE/decode path is wire-correct.
//
// Both are gated by the same `integration` build tag as
// client_integration_test.go (see that file for MQTT_BROKER / brokerAddr)
// and additionally skip cleanly — not fail — when `mosquitto_pub`/
// `mosquitto_sub` are not on PATH, since a developer running
// `go test -tags integration ./...` locally against a bare broker (no
// mosquitto-clients installed) should get a clean skip, not a build/runtime
// error. CI (`.github/workflows/ci.yml`'s `mqtt-interop` job) installs
// `mosquitto-clients` via apt before running this suite, so it always
// exercises the real cross-check there.

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/v3"
)

// requireMosquittoClients skips the test when the Mosquitto CLI tools are not
// available on PATH, rather than failing — mirrors this repo's existing
// "skip cleanly when the peer/broker is unavailable" posture for optional
// live-infrastructure tests (see test-mosquitto's prereqs check in ci.yml).
func requireMosquittoClients(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("mosquitto_pub"); err != nil {
		t.Skip("mosquitto_pub not on PATH — skipping third-party-peer interop test (install mosquitto-clients)")
	}
	if _, err := exec.LookPath("mosquitto_sub"); err != nil {
		t.Skip("mosquitto_sub not on PATH — skipping third-party-peer interop test (install mosquitto-clients)")
	}
}

// splitAddr splits a "host:port" broker address into its parts for the
// Mosquitto CLI tools' separate -h/-p flags.
func splitAddr(t *testing.T, addr string) (host, port string) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("splitAddr(%q): %v", addr, err)
	}
	return host, port
}

// uniqueTopic returns a topic under go-mqtt/v3/interop/thirdparty that is
// unique per test run, so concurrent/re-run test invocations against a
// shared broker never cross-deliver retained or in-flight messages from a
// previous run.
func uniqueTopic(suffix string) string {
	return fmt.Sprintf("go-mqtt/v3/interop/thirdparty/%s/%d", suffix, time.Now().UnixNano())
}

// TestIntegration_ThirdPartySubscriberVerifiesGoMqttPublish publishes via
// go-mqtt's own v3.Client and independently verifies receipt using
// Mosquitto's `mosquitto_sub` CLI — confirming go-mqtt's PUBLISH encoding is
// wire-correct against a genuinely independent MQTT implementation, not just
// self-consistent with another go-mqtt client.
func TestIntegration_ThirdPartySubscriberVerifiesGoMqttPublish(t *testing.T) {
	requireMosquittoClients(t)

	addr := brokerAddr(t)
	host, port := splitAddr(t, addr)
	topic := uniqueTopic("sub")
	const payload = "go-mqtt-wire-correct-publish"

	// Start the independent third-party subscriber first: -C 1 exits after
	// one message, -W bounds how long it waits so a wire-format bug that
	// makes go-mqtt's PUBLISH unparseable by Mosquitto fails the test
	// instead of hanging CI. subCtx is a hard ceiling on the subprocess
	// itself (belt and suspenders alongside -W), satisfying noctx.
	subCtx, subCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer subCancel()
	var out bytes.Buffer
	sub := exec.CommandContext(subCtx, "mosquitto_sub", "-h", host, "-p", port, "-t", topic, "-C", "1", "-W", "10")
	sub.Stdout = &out
	sub.Stderr = &out
	if err := sub.Start(); err != nil {
		t.Fatalf("starting mosquitto_sub: %v", err)
	}

	// Give mosquitto_sub time to CONNECT and SUBSCRIBE before go-mqtt
	// publishes, so the publish isn't racing the third party's own
	// handshake with the broker.
	time.Sleep(300 * time.Millisecond)

	pub, err := v3.Dial(addr, v3.WithClientID("go-mqtt-v3-thirdparty-pub"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, topic, mqtt.AtLeastOnce, []byte(payload)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if err := sub.Wait(); err != nil {
		t.Fatalf("mosquitto_sub did not receive go-mqtt's publish (wire-format mismatch?): %v\noutput: %s", err, out.String())
	}

	got := strings.TrimRight(out.String(), "\n")
	if got != payload {
		t.Errorf("mosquitto_sub payload: got %q, want %q", got, payload)
	}
}

// TestIntegration_GoMqttSubscriberVerifiesThirdPartyPublish publishes via
// Mosquitto's `mosquitto_pub` CLI and verifies correct receipt/decoding
// using go-mqtt's own v3.Client — confirming go-mqtt's SUBSCRIBE/decode path
// is wire-correct against a genuinely independent publisher.
func TestIntegration_GoMqttSubscriberVerifiesThirdPartyPublish(t *testing.T) {
	requireMosquittoClients(t)

	addr := brokerAddr(t)
	host, port := splitAddr(t, addr)
	topic := uniqueTopic("pub")
	const payload = "mosquitto-pub-cli-message"

	sub, err := v3.Dial(addr, v3.WithClientID("go-mqtt-v3-thirdparty-sub"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	subscription, err := sub.Subscribe(topic, mqtt.AtLeastOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	// Allow the SUBACK to arrive before the third party publishes, so the
	// message isn't racing go-mqtt's own subscription setup.
	time.Sleep(300 * time.Millisecond)

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pubCancel()
	pub := exec.CommandContext(pubCtx, "mosquitto_pub", "-h", host, "-p", port, "-t", topic, "-q", "1", "-m", payload)
	if out, err := pub.CombinedOutput(); err != nil {
		t.Fatalf("mosquitto_pub: %v\noutput: %s", err, out)
	}

	select {
	case msg := <-subscription.C():
		if msg.Topic != topic {
			t.Errorf("topic: got %q, want %q", msg.Topic, topic)
		}
		if string(msg.Payload) != payload {
			t.Errorf("payload: got %q, want %q", msg.Payload, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for mosquitto_pub's message via go-mqtt subscriber")
	}
}
