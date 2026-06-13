# Multi-stage build for go-mqtt quickstart binaries.
# Both targets (pub, sub) are compiled from a single builder stage.

FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /bin/pub ./examples/quickstart/pub
RUN CGO_ENABLED=0 go build -trimpath -o /bin/sub ./examples/quickstart/sub

# ── publisher image ────────────────────────────────────────────────────────────
FROM alpine:3.20 AS pub
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/pub /usr/local/bin/pub
LABEL org.opencontainers.image.title="go-mqtt publisher" \
      org.opencontainers.image.source="https://github.com/SoundMatt/go-mqtt" \
      org.opencontainers.image.licenses="MPL-2.0"
ENTRYPOINT ["pub"]

# ── subscriber image ───────────────────────────────────────────────────────────
FROM alpine:3.20 AS sub
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/sub /usr/local/bin/sub
LABEL org.opencontainers.image.title="go-mqtt subscriber" \
      org.opencontainers.image.source="https://github.com/SoundMatt/go-mqtt" \
      org.opencontainers.image.licenses="MPL-2.0"
ENTRYPOINT ["sub"]
