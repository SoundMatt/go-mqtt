module github.com/SoundMatt/go-mqtt

go 1.25.0

// This pseudo-version's commit (0d471beb) is exactly the RELAY v2.0.3 tag
// content (relay.SpecVersion == "2.0"). It cannot be required as "v2.0.3"
// directly: RELAY's go.mod does not declare the "/v2" module-path suffix
// that Go's semantic-import-versioning rules require for a v2+ tag, so
// `go get github.com/SoundMatt/RELAY@v2.0.3` is rejected outright. Pinning
// by commit hash resolves via a v1-line pseudo-version instead, sidestepping
// that check while still building against the real v2.0.3 source. Re-pin to
// a plain "v2.0.x" once RELAY's module path is fixed upstream.
require github.com/SoundMatt/RELAY v1.14.1-0.20260730210258-0d471bebcb11
