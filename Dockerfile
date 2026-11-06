# OPENH264=1 builds the stream snapshot decoder in (needs CGO and OpenH264).
# The default image leaves it out: H.264 is patent-encumbered, and Cisco's
# patent license covers only the OpenH264 binaries Cisco ships. Without it the
# SFU refuses SFU_R2_ENDPOINT (snapshots); everything else is the same.
FROM golang:1.24-alpine AS builder
ARG VERSION=dev
ARG OPENH264=0
WORKDIR /build
RUN if [ "$OPENH264" = "1" ]; then apk add --no-cache build-base openh264-dev; fi
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN if [ "$OPENH264" = "1" ]; then \
      export CGO_ENABLED=1 TAGS=openh264; \
    else \
      export CGO_ENABLED=0 TAGS=; \
    fi && \
    go build -tags "$TAGS" \
      -ldflags="-X github.com/mollohq/m3llo-sfu/internal/server.Version=${VERSION}" \
      -o /sfu ./cmd/sfu

FROM alpine:3.22
ARG OPENH264=0
RUN apk add --no-cache ca-certificates && \
    if [ "$OPENH264" = "1" ]; then apk add --no-cache openh264; fi
COPY --from=builder /sfu /usr/local/bin/sfu

EXPOSE 8443/tcp
EXPOSE 8080/tcp
EXPOSE 10000/udp
EXPOSE 10000/tcp

ENTRYPOINT ["sfu"]
