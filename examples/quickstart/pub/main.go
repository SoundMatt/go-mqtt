// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Command pub publishes vehicle signal readings over MQTT.
// Part of the Docker Quickstart.
//
// Environment variables:
//
//	MQTT_BROKER  Broker address (default: localhost:1883)
//	MQTT_TOPIC   Publish topic  (default: Vehicle/Speed)
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/v3"
)

func main() {
	addr := envOrDefault("MQTT_BROKER", "localhost:1883")
	topic := envOrDefault("MQTT_TOPIC", "Vehicle/Speed")

	client, err := v3.Dial(addr,
		v3.WithClientID(fmt.Sprintf("go-mqtt-pub-%d", os.Getpid())),
		v3.WithKeepalive(30*time.Second),
	)
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	defer client.Close()

	log.Printf("connected to %s, publishing on %s every second", addr, topic)

	ctx := context.Background()
	for seq := 1; ; seq++ {
		speed := 40.0 + rand.Float64()*80
		msg := fmt.Sprintf(`{"seq":%d,"speed":%.1f,"unit":"km/h"}`, seq, speed)
		if err := client.Publish(ctx, topic, mqtt.AtMostOnce, []byte(msg)); err != nil {
			log.Printf("publish error: %v", err)
			return
		}
		log.Printf("published: %s", msg)
		time.Sleep(time.Second)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
