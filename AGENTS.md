# AGENTS.md: m3llo-sfu

This file defines hard guardrails for AI coding agents working in `m3llo-sfu`.
If this file conflicts with direct human instructions, follow the human.

## 1) First Steps (Always)

Before making non-trivial changes:

1. Read `01-SFU.md` (architecture contract).
2. Read this `AGENTS.md`.
3. Locate relevant code pointers in section 6 below.
4. If behavior is ambiguous, ask instead of guessing.

## 2) Core Architecture Invariants (Do Not Break)

1. **Two distinct media paths**
   - Stream sessions: H.264 RTP relay plus reliable control DataChannel.
   - Voice sessions: RTP track relay (`OnTrack` + per-peer outgoing tracks).
   - Do not "unify" these paths unless explicitly requested.

2. **No relay-path media transformation**
   - No transcoding.
   - No server-side audio mixing.
   - No server recording pipeline.
   - The optional RTP snapshot tap is the only decode path. It is bounded,
     non-blocking, IDR-only, and must never affect relay behavior.

3. **Session model must stay explicit**
   - Stream session: 1 host + N viewers.
   - Voice session key format: `voice:<crew_id>:<channel_id>`.
   - Session state remains in-memory with drain cleanup (`DrainTimeout`).

4. **Negotiation and recovery behavior are critical**
   - Server may renegotiate for voice topology changes.
   - ICE restart flow is intentional; do not remove "failed -> restart -> timeout eject" behavior.
   - Keep signaling ordering guarantees intact.

5. **Backpressure policy is intentional**
   - DataChannel buffer limit: 2 MiB per peer.
   - Slow-peer forced disconnect window: 5s.
   - RTP stall threshold logic is deliberate; do not weaken without request.

6. **Security posture**
   - JWT verification with RS256 when public key is configured.
   - Auth fails closed: no key means every token is refused, unless
     `SFU_DEV_MODE` is set (01-SFU.md §7.1). Do not add new bypasses.
   - Test mode has its own secret (`SFU_TEST_PASSWORD`), off when empty.
   - Admin endpoints need `SFU_ADMIN_PASSWORD`; without it they are off
     (open only in dev mode).

7. **Contract-first changes**
   - If you change signaling messages, error codes, health/admin JSON schema, or capacity semantics, update `01-SFU.md` in the same PR.

## 3) High-Risk Areas (Extra Caution)

- `internal/server/signaling.go`
- `internal/server/voice_session.go`
- `internal/server/peer.go`
- `internal/server/session_store.go`
- `internal/server/admin_auth.go`

For these files:

- Make minimal, surgical edits.
- Preserve existing log events where possible.
- Do not refactor broadly unless asked.

## 4) What Not To Do

- Do not switch voice media back to DataChannel forwarding.
- Do not remove track renegotiation logic to "simplify" flow.
- Do not add new dependencies without explicit approval.
- Do not change env var names/meaning silently.
- Do not change behavior and only update comments/docs.
- Do not use destructive git operations.

## 5) Implementation Expectations

When implementing changes:

- Keep current style and naming.
- Prefer focused functions over large rewrites.
- Add comments only for non-obvious "why".
- Keep error codes explicit and stable.
- Preserve existing metrics/logging hooks unless asked otherwise.

## 6) Code Pointers (Where To Change)

### Runtime and Wiring

- Startup/config: `cmd/sfu/main.go`, `internal/server/server.go`, `internal/server/config.go`
- Session lifecycle/draining: `internal/server/session_store.go`

### Signaling + Join

- WS entry + auth: `internal/server/signaling.go` (`handleWebSocket`, `handleTestWebSocket`)
- Message dispatch: `handleSignalingLoop`
- Join paths: `handleJoinStream`, `handleJoinVoice`
- SDP/ICE: `handleOffer`, `handleAnswer`, `handleICECandidate`, `applyOfferAndAnswer`
- Leave/cleanup: `handleLeave`, `cleanupPeer`, `removePeerFromSession`

### Stream Path

- Membership/events: `internal/server/stream_session.go` (`AddPeer`, `RemovePeer`)
- Control DC wiring: `WireDataChannels` (control-only for stream)
- RTP relay + stats: `RelaySnapshot`, host ingress → per-viewer egress loops
- Snapshot tap: `readHostVideoRTP` → `tapSnapshotRTP` → `internal/snapshot/rtp_input.go`

### Voice Path

- Membership + renegotiation setup: `internal/server/voice_session.go` (`AddPeer`, `SetupOutgoingTracks`, `RemovePeer`)
- RTP ingress loop: `WireAudioTrack`
- Outgoing RTP tracks: `internal/server/peer.go` (`AddOutgoingTrack`, `SendRTP`, `RemoveOutgoingTrack`)

### Reliability + Slow Peer

- DataChannel pressure: `internal/server/peer.go` (`SendMedia`, `IsSlowPeer`)
- RTP stall: `internal/server/peer.go` (`IsRTPStalled`)
- Ejection loop: `internal/server/server.go` (`runSlowPeerChecker`)
- Renegotiation/ICE restart: `internal/server/peer.go` (`Renegotiate`, `RestartICE`, `StopPhantomTransceivers`)

### Security + Observability

- JWT claims/verification: `internal/auth/jwt.go`
- Health endpoint: `internal/server/health.go`
- Admin auth and endpoints: `internal/server/admin_auth.go`, `internal/server/admin.go`
- Peer aggregation: `internal/server/peers.go`
- Metrics history: `internal/metrics/metrics.go`

### Peer Minutes

- Per-day peer minutes for the admin API: `internal/minutes/daily.go`

## 7) Debugging Workflow (Preferred)

1. Reproduce with smallest scenario first.
2. Confirm `/health` and `/admin` state before changing code.
3. Use soak tools for realistic validation:
   - `tools/stream-soak`
   - `tools/voice-soak`
4. For voice bugs (one-way/missing audio), inspect:
   - track add/remove,
   - renegotiation flow,
   - RTP stall logs.

## 8) Done Checklist

Before saying "done":

- Run `go test ./...` (fast; uses the no-op snapshot decode stub). To also cover
  the CGO + openh264 snapshot decode path (the `OPENH264=1` image), run `./test-local.sh`
  (needs openh264: `brew install openh264` on macOS, `apt-get install libopenh264-dev`
  on Debian).
- Run `gofmt` on touched Go files (or equivalent formatting step)
- Verify no unintended contract drift vs `01-SFU.md`
- If contracts changed, update `01-SFU.md`
- Summarize exactly what changed and why

## 9) Escalate Instead of Guessing

Ask the human before implementing when:

- change impacts signaling schema,
- change impacts voice/stream architecture boundaries,
- change impacts auth/security semantics,
- behavior in spec and code appears contradictory.
