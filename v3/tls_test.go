// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

//fusa:req REQ-TLS-001
//fusa:req REQ-TLS-002
//fusa:req REQ-TLS-003

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
)

// certPair holds a generated certificate and its pool for verification.
type certPair struct {
	tlsCert tls.Certificate
	pool    *x509.CertPool
}

// genCert creates a self-signed ECDSA certificate for "localhost"/127.0.0.1.
// If forClientAuth is true the cert is usable for client authentication.
func genCert(t *testing.T, forClientAuth bool) certPair {
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
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	if forClientAuth {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
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
	return certPair{
		tlsCert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf},
		pool:    pool,
	}
}

// serveTLS accepts one TLS client, completes CONNECT/CONNACK, then runs handler.
func (fb *fakeBroker) serveTLS(t *testing.T, cfg *tls.Config, handler func()) {
	t.Helper()
	go func() {
		rawConn, err := fb.ln.Accept()
		if err != nil {
			return
		}
		conn := tls.Server(rawConn, cfg)
		if err := conn.HandshakeContext(context.Background()); err != nil {
			_ = rawConn.Close()
			return
		}
		fb.conn = conn
		fb.readPacket(t) // CONNECT
		if _, err := conn.Write([]byte{pktCONNACK, 0x02, 0x00, 0x00}); err != nil {
			return
		}
		if handler != nil {
			handler()
		}
	}()
}

// TestDialTLS verifies that a client can connect over TLS to a broker presenting
// a server certificate the client trusts.
//
//fusa:req REQ-TLS-001
func TestDialTLS(t *testing.T) {
	server := genCert(t, false)

	fb := newFakeBroker(t)
	defer fb.close()

	serverCfg := &tls.Config{Certificates: []tls.Certificate{server.tlsCert}}
	fb.serveTLS(t, serverCfg, nil)

	clientCfg := &tls.Config{RootCAs: server.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c, err := Dial(fb.addr(), WithClientID("tls-client"), WithKeepalive(0), WithTLS(clientCfg))
	if err != nil {
		t.Fatalf("Dial over TLS: %v", err)
	}
	defer func() { _ = c.Close() }()
}

// TestDialTLSUntrustedFails verifies that the handshake fails when the client
// does not trust the server certificate.
//
//fusa:req REQ-TLS-002
func TestDialTLSUntrustedFails(t *testing.T) {
	server := genCert(t, false)

	fb := newFakeBroker(t)
	defer fb.close()

	serverCfg := &tls.Config{Certificates: []tls.Certificate{server.tlsCert}}
	fb.serveTLS(t, serverCfg, nil)

	// Empty RootCAs → server cert is untrusted.
	clientCfg := &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "localhost", MinVersion: tls.VersionTLS12}
	_, err := Dial(fb.addr(), WithClientID("tls-untrusted"), WithKeepalive(0), WithTLS(clientCfg))
	if err == nil {
		t.Fatal("Dial over TLS with untrusted cert: expected error, got nil")
	}
}

// TestMutualTLS verifies that mTLS succeeds when the client presents a
// certificate the server requires and trusts.
//
//fusa:req REQ-TLS-002
func TestMutualTLS(t *testing.T) {
	server := genCert(t, false)
	client := genCert(t, true)

	fb := newFakeBroker(t)
	defer fb.close()

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{server.tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    client.pool,
	}
	fb.serveTLS(t, serverCfg, nil)

	clientCfg := &tls.Config{
		Certificates: []tls.Certificate{client.tlsCert},
		RootCAs:      server.pool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
	}
	c, err := Dial(fb.addr(), WithClientID("mtls-client"), WithKeepalive(0), WithTLS(clientCfg))
	if err != nil {
		t.Fatalf("mTLS Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
}

// TestMutualTLSNoClientCertFails verifies that a server requiring a client
// certificate rejects a client that presents none.
//
//fusa:req REQ-TLS-002
func TestMutualTLSNoClientCertFails(t *testing.T) {
	server := genCert(t, false)
	client := genCert(t, true)

	fb := newFakeBroker(t)
	defer fb.close()

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{server.tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    client.pool,
	}
	fb.serveTLS(t, serverCfg, nil)

	// Client presents no certificate.
	clientCfg := &tls.Config{RootCAs: server.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	_, err := Dial(fb.addr(), WithClientID("mtls-missing"), WithKeepalive(0), WithTLS(clientCfg))
	if err == nil {
		t.Fatal("mTLS without client cert: expected error, got nil")
	}
}

// TestDialTLSPublishSubscribe verifies normal pub/sub works over a TLS
// connection (the TLS layer is transparent to the MQTT protocol).
//
//fusa:req REQ-TLS-001
func TestDialTLSPublishSubscribe(t *testing.T) {
	server := genCert(t, false)

	fb := newFakeBroker(t)
	defer fb.close()

	serverCfg := &tls.Config{Certificates: []tls.Certificate{server.tlsCert}}
	fb.serveTLS(t, serverCfg, func() {
		fb.readPacket(t) // SUBSCRIBE
		// Deliver a QoS 0 PUBLISH.
		_, _ = fb.conn.Write(buildPUBLISH("a/b", []byte("over-tls"), 0, false, 0))
	})

	clientCfg := &tls.Config{RootCAs: server.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c, err := Dial(fb.addr(), WithClientID("tls-pubsub"), WithKeepalive(0), WithTLS(clientCfg))
	if err != nil {
		t.Fatalf("Dial over TLS: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	select {
	case msg := <-sub.C():
		if string(msg.Payload) != "over-tls" {
			t.Errorf("payload = %q, want over-tls", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message over TLS")
	}
}

// TestDialTLSConvenience verifies DialTLS supplies a default TLS config and that
// an explicit WithTLS overrides it (here, to trust the test cert).
//
//fusa:req REQ-TLS-003
func TestDialTLSConvenience(t *testing.T) {
	server := genCert(t, false)

	fb := newFakeBroker(t)
	defer fb.close()

	serverCfg := &tls.Config{Certificates: []tls.Certificate{server.tlsCert}}
	fb.serveTLS(t, serverCfg, nil)

	// DialTLS provides a default config; WithTLS overrides it with one trusting
	// the test certificate.
	clientCfg := &tls.Config{RootCAs: server.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c, err := DialTLS(fb.addr(), WithClientID("dialtls"), WithKeepalive(0), WithTLS(clientCfg))
	if err != nil {
		t.Fatalf("DialTLS: %v", err)
	}
	defer func() { _ = c.Close() }()
}
