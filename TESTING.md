# Testing m3llo-sfu

## Unit tests

```bash
go test -race ./...
```

They use the no-op snapshot decoder. To cover the OpenH264 decode path too:

```bash
./test-local.sh
```

It needs OpenH264 (`brew install openh264` on macOS, `apt-get install
libopenh264-dev` on Debian).

## A local SFU in test mode

The soaks and the canary join with `?test_password=` instead of a backend
token (`01-SFU.md` section 5.1). The SFU accepts that only when it runs with
the same `SFU_TEST_PASSWORD`. The test-mode compose override sets it to
`SFU_ADMIN_PASSWORD` unless you give it too:

```bash
SFU_ADMIN_PASSWORD=devpass docker compose \
  -f docker-compose.yml -f docker-compose.test-mode.yml up -d --build
```

Or natively, for a fast edit loop:

```bash
SFU_PUBLIC_IP=127.0.0.1 SFU_ADMIN_PASSWORD=devpass SFU_TEST_PASSWORD=devpass \
  go run ./cmd/sfu
```

The soaks negotiate over UDP. If your Docker runtime does not pass UDP to
containers cleanly, run the SFU natively for them.

## Voice soak / churn test

Use the built-in soak harness to stress join/leave reconnect churn:

```bash
go run ./tools/voice-soak \
  --endpoint ws://127.0.0.1:8443/ws \
  --test-password devpass \
  --clients 30 \
  --rounds 40 \
  --hold-ms 400
```

To include full voice WebRTC lifecycle (offer/answer + trickle ICE + renegotiation):

```bash
go run ./tools/voice-soak \
  --endpoint ws://127.0.0.1:8443/ws \
  --test-password devpass \
  --clients 12 \
  --rounds 20 \
  --hold-ms 700 \
  --negotiate-webrtc \
  --min-pc-connect-rate 0.80
```

To include RTP forwarding load (synthetic Opus packets from each client):

```bash
go run ./tools/voice-soak \
  --endpoint ws://127.0.0.1:8443/ws \
  --test-password devpass \
  --clients 20 \
  --rounds 12 \
  --hold-ms 1500 \
  --negotiate-webrtc \
  --rtp-audio \
  --rtp-pps 50 \
  --rtp-payload-bytes 80
```

Voice receive slots (01-SFU.md §5.5). Each soak client offers 16 recvonly
receive slots, as the mello client does. The gate fails the run when the
SFU sends a client an offer with no audio m-line beyond the mic and slots:

```bash
go run ./tools/voice-soak \
  --endpoint ws://127.0.0.1:8443/ws \
  --test-password devpass \
  --clients 17 \
  --rounds 3 \
  --hold-ms 4000 \
  --negotiate-webrtc \
  --rtp-audio \
  --vad-gaps \
  --max-offers-within-slots 0
```

- RTP payloads are real Opus speech (`tools/voice-soak/testdata/speech.ogg`)
  with a random start sequence and timestamp and the timestamp on the 48 kHz
  media clock. `--rtp-source synthetic` keeps the old `--rtp-pps` /
  `--rtp-payload-bytes` payloads.
- `--vad-gaps` sends talk spurts (1 to 3 s) with silences (0.5 to 2 s), and
  the in-band `speaking` messages.
- `--recv-slots 0` gives every sender its own m-line, as a client without
  receive slots.
- `--ice-tcp-only` makes each client gather only TCP4 candidates, as a client
  on a network that blocks UDP. It needs the SFU passive ICE-TCP port
  (`SFU_ICE_TCP_PORT`). `pc_pair_udp` / `pc_pair_tcp` count the transport of
  the ICE pair each connection selected.
- Output: `offers_within_slots`, `offers_past_slots`, `slot_msg_rx`,
  `speaking_msg_tx` / `speaking_msg_rx`, and received RTP by m-line:
  `rtp_rx_slot_mapped` (the slot map names the sender), `rtp_rx_unmapped_slot`
  and `rtp_rx_unmapped_own` (no owner in the slot map), `rtp_rx_undeclared`
  (an SSRC that no negotiated m-line declares). The unmapped packets split
  into `rtp_rx_early_*` (before the new owner's `slot` message) and
  `rtp_rx_late_*` (up to 100 ms after the old owner's release);
  `rtp_rx_early_lead_max_ms` is the longest time an early packet came before
  its message.
- `webrtc_error_codes` names the SFU error message and the phase
  (`connect` or `after_answer`).
- Pion clients do not use loopback ICE candidates. On Windows, start the SFU
  with `SFU_PUBLIC_IP` set to the LAN address for a local soak, not
  `127.0.0.1`.
- On Windows, build the SFU with Go 1.26.2 or later, or with Go 1.24 as the
  Dockerfile does. Go 1.26.0 and 1.26.1 corrupt memory in Windows overlapped
  socket I/O (golang/go#77975): the SFU dies with `unexpected fault address`
  in Pion or runtime code, with the high 32 bits of a pointer replaced by 16
  or 28 (the IPv4 or IPv6 socket address length). A local churn soak shows it
  in 2 to 3 runs of 10. Linux builds are not affected.

To estimate one-SFU capacity envelope, run a client ladder:

```bash
go run ./tools/voice-soak \
  --endpoint ws://127.0.0.1:8443/ws \
  --test-password devpass \
  --rounds 8 \
  --hold-ms 1200 \
  --stagger-ms 120 \
  --negotiate-webrtc \
  --rtp-audio \
  --sweep-clients 25,50,75,100,125 \
  --min-success-rate 0.95 \
  --min-pc-connect-rate 0.90
```

To auto-search the highest passing tier (exponential ramp + binary refine):

```bash
go run ./tools/voice-soak \
  --endpoint ws://127.0.0.1:8443/ws \
  --test-password devpass \
  --rounds 3 \
  --hold-ms 900 \
  --stagger-ms 120 \
  --negotiate-webrtc \
  --rtp-audio \
  --auto-capacity-search \
  --search-start-clients 12 \
  --search-max-clients 120 \
  --search-growth-factor 1.6 \
  --search-max-runs 12 \
  --search-safety-factor 0.85 \
  --min-success-rate 0.95 \
  --min-pc-connect-rate 0.90
```

Notes:
- The harness uses `test_password` mode, so `SFU_TEST_PASSWORD` must be set on the SFU. `--test-password` (or `SFU_TEST_PASSWORD`) gives it to the soak; without it the soak uses `--admin-password`.
- Exit code is non-zero when join success rate drops below `--min-success-rate`.
- In `--negotiate-webrtc` mode, exit code is also non-zero if PC connect success drops below `--min-pc-connect-rate`.
- `--rtp-audio` requires `--negotiate-webrtc`; output includes RTP tx/rx packet and throughput counters.
- `--sweep-clients` runs several rounds sequentially and reports the highest client tier that met all gates.
- `--auto-capacity-search` cannot be used with `--sweep-clients`; output includes `max_passing_clients` and `recommended_safe_clients`.
- With `--max-offers-within-slots N`, exit code is also non-zero when more than `N` offers reach clients within their receive slots.
- Tune `--clients`, `--rounds`, and `--hold-ms` for quick smoke vs longer soak.

## Stream RTP soak

Use the stream harness to run SFU **H.264 RTP** relay checks with one synthetic host and N synthetic viewers.
It negotiates full WebRTC (offer/answer + ICE + control DataChannel + RTP video and audio tracks).
The clients offer the client codec set (`01-SFU.md` §6.1): H.264 at PT 96 and Opus at PT 111. A viewer
offers a recvonly audio m-line, and the host sends one silent Opus frame every 20 ms.

The host video is real H.264: `tools/stream-soak/testdata/stream-gate-720p60-2s.h264`, the first
2 s (120 access units, 1280x720, about 500 KB) of the mello streaming quality gate's reference clip,
encoded once with libmello's OpenH264 test-lane encoder at 2500 kbps (one slice, one IDR at frame 0).
The host packetizes it as RFC 6184 mode 1 (single NAL unit packets, FU-A fragments of at most 1100
bytes, as libmello's RTP sender) and loops it at `--fps`, so each pass starts with the IDR. A test
freezes the file's FNV-1a hash. To make it again, run `stream-gate --write-fixture <file>` in the client repo, [mollohq/m3llo](https://github.com/mollohq/m3llo).

```bash
SFU_ADMIN_PASSWORD=devpass \
go run ./tools/stream-soak \
  --endpoint ws://127.0.0.1:8443/ws \
  --test-password devpass \
  --viewers 5 \
  --hold-ms 30000 \
  --fps 30 \
  --min-rx-ratio 0.99 \
  --max-p95-ms 90 \
  --max-queue-drops 0
```

Notes:
- Output banner: `stream-soak session=... transport=rtp`.
- Reports `admin_rtp_in`, `admin_rtp_forwarded`, `admin_rtp_queue_drops`, `admin_rtp_duplicates`.
- Admin reconciliation is authoritative by default; provide `SFU_ADMIN_PASSWORD` or `--admin-password`.
- Reports `rtp_audio_rx` and `audio_rx_ratio` (viewer Opus packets against host audio sent × viewers).
- `--audio-lead-ms N` starts host audio N ms before host video, so the SFU wires the host audio track first.
- `--sim-drop-pct P` keeps P percent of the host video packets off the wire (seeded by `--sim-seed`): they
  use a sequence number, so the SFU sees ingress loss and NACKs the host. Dropped packets do not count as
  sent (`host_sim_dropped`), so the RX ratio gates the relay, not the loss.
- `--sim-jitter-ms J` delays each host frame by a half-normal draw with sigma J ms, frames kept in order.
- `--viewer-drop-pct P` makes viewer 1 drop P percent of the video packets that reach it from the SFU
  (seeded by `--viewer-drop-seed`), once per sequence number, before its NACK generator. The viewer NACKs
  them and the SFU's NACK responder sends them again. Reports `viewer_leg_dropped` and
  `viewer_leg_repaired`; the gate needs the repaired count within 1 % of the dropped count. The viewer
  runs with SRTP replay protection off, because the repair has the sequence number of a packet that SRTP
  already took.
- `tools/stream-soak/run-1080-gate.sh` runs the 20 and 32 viewer campaign at 60 fps, the 32-viewer cases
  with `--sim-drop-pct 1 --sim-jitter-ms 8` and `--sim-drop-pct 3 --sim-jitter-ms 20`, and a 20-viewer
  case with `--viewer-drop-pct 2`. It builds with `GOTOOLCHAIN=go1.24.9`, the Go of the Dockerfile (see the
  script).
- Exit code is non-zero when video RX ratio, audio RX ratio, p95 latency, duplicate, queue-drop, viewer-leg repair, or admin-counter gates fail. `--min-rx-ratio` gates both RX ratios.

### Release fan-out gates

Run 1-viewer smoke, 5 viewers for 30 seconds, then 20 and 32 viewers for 90 seconds. Require:

- 5/20 viewers: RX ratio ≥0.99, p95 ≤90 ms, zero queue drops.
- 32 viewers: RX ratio ≥0.985, p95 ≤130 ms.
- Admin forwarded packets approximately equal ingress packets × active viewers.

## Canary

`/health` runs inside the process, so it cannot show that a client can join
from outside. `tools/sfu-canary` tests a real join through the public
signaling endpoint, including ICE and DTLS (`01-SFU.md` section 8.5). Canary
peers are synthetic: `/stats` leaves them out and the peer minutes count them
apart.

```bash
go build -o sfu-canary ./tools/sfu-canary
SFU_TEST_PASSWORD=... ./sfu-canary -endpoint wss://sfu.example.com:8443/ws -region eu-west
```

It prints one line and exits `0` when the connection reaches `Connected`, or
`1` with the failing stage (`dial`, `welcome`, `join` or `webrtc`). It needs
UDP, so run it from a normal host. Run it from cron and alert on exit `1`.
