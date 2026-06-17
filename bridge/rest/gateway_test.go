// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package rest_test

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/bridge/rest"
	"github.com/SoundMatt/go-mqtt/mock"
)

// postString issues a context-bound POST with a string body.
func postString(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// getURL issues a context-bound GET.
func getURL(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestPublishEndpoint(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial())
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	// A consumer on the same broker.
	consumer := broker.Dial()
	sub, err := consumer.Subscribe("Vehicle/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	resp := postString(t, srv.URL+"/publish/Vehicle/Speed", "60")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}

	select {
	case msg := <-sub.C():
		if msg.Topic != "Vehicle/Speed" {
			t.Errorf("topic = %q, want Vehicle/Speed", msg.Topic)
		}
		if string(msg.Payload) != "60" {
			t.Errorf("payload = %q, want 60", msg.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for published message")
	}
}

func TestPublishQoS(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial())
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	consumer := broker.Dial()
	sub, err := consumer.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	resp := postString(t, srv.URL+"/publish/a/b?qos=1", "x")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}

	msg := <-sub.C()
	if msg.QoS != mqtt.AtLeastOnce {
		t.Errorf("QoS = %v, want AtLeastOnce", msg.QoS)
	}
}

func TestPublishInvalidQoS(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial())
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	resp := postString(t, srv.URL+"/publish/a/b?qos=9", "x")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestPublishBodyTooLarge(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial(), rest.WithMaxBody(4))
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	resp := postString(t, srv.URL+"/publish/a/b", "way too long")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
}

func TestSubscribeSSE(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial())
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/subscribe/a/%23", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	// Give the server a moment to register the subscription, then publish.
	time.Sleep(50 * time.Millisecond)
	pub := broker.Dial()
	if err := pub.Publish(context.Background(), "a/b", mqtt.AtMostOnce, []byte("sse-payload")); err != nil {
		t.Fatal(err)
	}

	// Read one SSE data line.
	dataCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "data: ") {
				dataCh <- strings.TrimPrefix(line, "data: ")
				return
			}
		}
	}()

	select {
	case data := <-dataCh:
		if !strings.Contains(data, "sse-payload") && !strings.Contains(data, "c3NlLXBheWxvYWQ=") {
			// payload is base64-encoded in relay/mqtt JSON; accept either form
			t.Errorf("SSE data missing payload: %s", data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SSE message")
	}
}

func TestRetainNotFound(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial(), rest.WithRetainTimeout(100*time.Millisecond))
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	resp := getURL(t, srv.URL+"/retain/no/such/topic")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestRetainIgnoresLiveTraffic(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial(), rest.WithRetainTimeout(150*time.Millisecond))
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	// Publish live (non-retained) traffic on the topic while /retain is waiting.
	pub := broker.Dial()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 3 {
			_ = pub.Publish(context.Background(), "live/topic", mqtt.AtMostOnce, []byte("live"))
			time.Sleep(20 * time.Millisecond)
		}
	}()

	resp := getURL(t, srv.URL+"/retain/live/topic")
	defer func() { _ = resp.Body.Close() }()
	<-done
	// No retained message exists; live traffic must be ignored → 404.
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("status = %d, want 404 (live traffic must not count as retained); body=%s",
			resp.StatusCode, body)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	broker := mock.New()
	gw := rest.New(broker.Dial())
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	// GET on the publish route is not registered → 405.
	resp := getURL(t, srv.URL+"/publish/a/b")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}
