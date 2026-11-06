#!/usr/bin/env bash
#
# test-local.sh: run the full test suite locally, including the CGO + openh264
# snapshot decode path that a plain `go test ./...` skips (it falls back to the
# no-op stub in internal/snapshot/h264_stub.go).
#
# The production build uses `-tags openh264` and links libopenh264. On Alpine
# (the Docker image) the openh264-dev headers sit on the default include path;
# on macOS Homebrew keeps them under its own prefix, so we point CGO at it.
#
# Usage:
#   ./test-local.sh              # go test -tags openh264 ./...
#   ./test-local.sh -run TestX   # extra args are passed through to `go test`

set -euo pipefail

cd "$(dirname "$0")"

if command -v brew >/dev/null 2>&1 && brew --prefix openh264 >/dev/null 2>&1; then
    # macOS / Homebrew: headers and lib live under the keg prefix.
    OH_PREFIX="$(brew --prefix openh264)"
    export CGO_CFLAGS="${CGO_CFLAGS:-} -I${OH_PREFIX}/include"
    # The -lopenh264 itself comes from the #cgo LDFLAGS directive in h264.go.
    export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L${OH_PREFIX}/lib"
elif ! pkg-config --exists openh264 2>/dev/null && [[ ! -f /usr/include/wels/codec_api.h ]]; then
    echo "openh264 headers not found." >&2
    echo "  macOS:  brew install openh264" >&2
    echo "  Debian: apt-get install libopenh264-dev" >&2
    echo "  Alpine: apk add openh264-dev" >&2
    exit 1
fi

exec env CGO_ENABLED=1 go test -tags openh264 ./... "$@"
