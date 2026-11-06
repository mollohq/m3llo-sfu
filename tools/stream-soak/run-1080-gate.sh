#!/usr/bin/env bash
set -uo pipefail

# 1080-style transport gate campaign for SFU stream relay.
# The host sends the real H.264 fixture (the 720p60 reference clip of the
# mello streaming quality gate, FU-A fragments of up to 1100 bytes) at 60 fps
# to 20 and 32 viewers for long holds. Cases 2 and 3 add ingress loss and
# frame jitter on the host leg (--sim-drop-pct, --sim-jitter-ms), so the SFU
# NACKs the host and sees jittered ingress. Case 4 drops packets on one
# viewer leg (--viewer-drop-pct) and gates the SFU's repair toward that
# viewer. Run from the repository root.
#
# Go toolchain: go1.24, the version of the production image (Dockerfile).
# On Windows 11 build 26300 a soak host and an SFU built with go1.26.1
# fault in pure Go code (Pion's NACK responder and mDNS cache) in 2 of 3
# 20-viewer runs of 20 s; with go1.24.9 3 of 3 pass. GOTOOLCHAIN overrides.
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.24.9}"

usage() {
  echo "Usage: $0 --endpoint <ws[s]://host:port/ws> --admin-password <password> [--admin-endpoint <http://host:port>] [--session-prefix <prefix>]"
}

ENDPOINT=""
ADMIN_ENDPOINT=""
ADMIN_PASSWORD=""
SESSION_PREFIX="stream-soak-1080gate"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --endpoint)
      ENDPOINT="${2:-}"
      shift 2
      ;;
    --admin-password)
      ADMIN_PASSWORD="${2:-}"
      shift 2
      ;;
    --admin-endpoint)
      ADMIN_ENDPOINT="${2:-}"
      shift 2
      ;;
    --session-prefix)
      SESSION_PREFIX="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$ENDPOINT" || -z "$ADMIN_PASSWORD" ]]; then
  usage
  exit 2
fi

run_case() {
  local name="$1"
  shift
  local admin_args=()
  if [[ -n "$ADMIN_ENDPOINT" ]]; then
    admin_args=(--admin-endpoint "$ADMIN_ENDPOINT")
  fi
  echo ""
  echo "===== CASE: ${name} ====="
  go run ./tools/stream-soak \
    --endpoint "$ENDPOINT" \
    --admin-password "$ADMIN_PASSWORD" \
    "${admin_args[@]}" \
    --session-prefix "${SESSION_PREFIX}-${name}" \
    --fps 60 \
    "$@"
}

overall_fail=0

# 1) Clean ingress at 20 viewers.
if ! run_case "clean-20v" \
  --viewers 20 \
  --hold-ms 90000 \
  --min-rx-ratio 0.995 \
  --max-p95-ms 90; then
  overall_fail=1
fi

# 2) Typical home uplink at 32 viewers: 1 % ingress loss, 8 ms frame jitter.
if ! run_case "typical-home-32v" \
  --viewers 32 \
  --hold-ms 120000 \
  --sim-drop-pct 1 \
  --sim-jitter-ms 8 \
  --min-rx-ratio 0.990 \
  --max-p95-ms 90; then
  overall_fail=1
fi

# 3) Rough home uplink at 32 viewers: 3 % ingress loss, 20 ms frame jitter.
if ! run_case "rough-home-32v" \
  --viewers 32 \
  --hold-ms 120000 \
  --sim-drop-pct 3 \
  --sim-jitter-ms 20 \
  --min-rx-ratio 0.985 \
  --max-p95-ms 130; then
  overall_fail=1
fi

# 4) Viewer-leg repair at 20 viewers: viewer 1 drops 2 % of the video
#    packets on the SFU-to-viewer leg before its NACK generator. The SFU's
#    NACK responder must repair them: the repaired count reaches the dropped
#    count within 1 % (viewer_leg_repaired). Keeps the viewer-leg repair
#    path gated (stream quality plan, stage 1 finding 3).
if ! run_case "viewer-leg-repair-20v" \
  --viewers 20 \
  --hold-ms 60000 \
  --viewer-drop-pct 2 \
  --min-rx-ratio 0.995 \
  --max-p95-ms 90; then
  overall_fail=1
fi

if (( overall_fail != 0 )); then
  echo ""
  echo "1080-style stream-soak gate campaign FAILED."
  exit 1
fi

echo ""
echo "All 1080-style stream-soak gate cases passed."
