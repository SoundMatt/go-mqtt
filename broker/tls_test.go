// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package broker_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
	"github.com/SoundMatt/go-mqtt/broker"
	v3 "github.com/SoundMatt/go-mqtt/v3"
)

// genServerCert creates a self-signed ECDSA server certificate for
// localhost/127.0.0.1 and a pool that trusts it.
func genServerCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

// TestBrokerWithTLS verifies that a broker created WithTLS wraps accepted
// connections in TLS and a TLS client can complete CONNECT and pub/sub.
//
//fusa:test REQ-BROKER-009
func TestBrokerWithTLS(t *testing.T) {
	cert, pool := genServerCert(t)
	serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}

	srv := broker.New(broker.WithTLS(serverCfg))
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("TLS broker did not start listening")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Close() })

	clientCfg := &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c, err := v3.Dial(srv.Addr(), v3.WithClientID("tls"), v3.WithKeepalive(0), v3.WithTLS(clientCfg))
	if err != nil {
		t.Fatalf("Dial over TLS: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	sub, err := c.Subscribe("secure/#", mqtt.AtLeastOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := c.Publish(context.Background(), "secure/topic", mqtt.AtLeastOnce, []byte("ciphered")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case m := <-sub.C():
		if string(m.Payload) != "ciphered" {
			t.Errorf("payload = %q, want ciphered", m.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message over TLS")
	}
}
