// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Command sub subscribes to a Vehicle signal topic and logs received messages.
// Part of the Docker Quickstart.
//
// Environment variables:
//
//	MQTT_BROKER  Broker address (default: localhost:1883)
//	MQTT_TOPIC   Subscribe filter (default: Vehicle/#)
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/v3"
)

func main() {
	addr := envOrDefault("MQTT_BROKER", "localhost:1883")
	filter := envOrDefault("MQTT_TOPIC", "Vehicle/#")

	client, err := v3.Dial(addr,
		v3.WithClientID(fmt.Sprintf("go-mqtt-sub-%d", os.Getpid())),
		v3.WithKeepalive(30*time.Second),
	)
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	defer func() { _ = client.Close() }()

	sub, err := client.Subscribe(filter, mqtt.AtMostOnce)
	if err != nil {
		log.Fatalf("subscribe %s: %v", filter, err)
	}
	defer func() { _ = sub.Close() }()

	log.Printf("connected to %s, subscribed to %s", addr, filter)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case msg, ok := <-sub.C():
			if !ok {
				return
			}
			log.Printf("[%s] %s", msg.Topic, msg.Payload)
		case <-sig:
			return
		}
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
