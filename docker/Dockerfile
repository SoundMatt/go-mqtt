# Multi-stage build for go-mqtt images: the quickstart pub/sub demo binaries
# plus the RELAY-conformant CLI (cmd/go-mqtt). All three are compiled from a
# single builder stage. Labels follow RELAY spec §13.5 (Docker image base).

FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG TOOL_VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -o /bin/pub ./examples/quickstart/pub
RUN CGO_ENABLED=0 go build -trimpath -o /bin/sub ./examples/quickstart/sub
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-X main.toolVersion=${TOOL_VERSION}" -o /bin/go-mqtt ./cmd/go-mqtt

# ── publisher image ────────────────────────────────────────────────────────────
FROM alpine:3.20 AS pub
RUN apk add --no-cache git ca-certificates
COPY --from=builder /bin/pub /usr/local/bin/pub
LABEL org.opencontainers.image.title="go-mqtt publisher" \
      org.opencontainers.image.source="https://github.com/SoundMatt/go-mqtt" \
      org.opencontainers.image.licenses="MPL-2.0" \
      io.relay.tool="go-mqtt" \
      io.relay.language="go" \
      io.relay.binary="pub" \
      io.relay.spec-version="1.11"
ENTRYPOINT ["pub"]

# ── subscriber image ───────────────────────────────────────────────────────────
FROM alpine:3.20 AS sub
RUN apk add --no-cache git ca-certificates
COPY --from=builder /bin/sub /usr/local/bin/sub
LABEL org.opencontainers.image.title="go-mqtt subscriber" \
      org.opencontainers.image.source="https://github.com/SoundMatt/go-mqtt" \
      org.opencontainers.image.licenses="MPL-2.0" \
      io.relay.tool="go-mqtt" \
      io.relay.language="go" \
      io.relay.binary="sub" \
      io.relay.spec-version="1.11"
ENTRYPOINT ["sub"]

# ── CLI image (RELAY spec §11/§17 conformance CLI) ─────────────────────────────
# The project directory is expected to be mounted at /project by convention
# (RELAY spec §13.5), e.g.:
#   docker run --rm -v "$(pwd)":/project ghcr.io/soundmatt/go-mqtt version
FROM alpine:3.20 AS cli
RUN apk add --no-cache git ca-certificates
COPY --from=builder /bin/go-mqtt /usr/local/bin/go-mqtt
WORKDIR /project
LABEL org.opencontainers.image.title="go-mqtt" \
      org.opencontainers.image.source="https://github.com/SoundMatt/go-mqtt" \
      org.opencontainers.image.licenses="MPL-2.0" \
      io.relay.tool="go-mqtt" \
      io.relay.language="go" \
      io.relay.binary="go-mqtt" \
      io.relay.spec-version="1.11"
ENTRYPOINT ["go-mqtt"]
