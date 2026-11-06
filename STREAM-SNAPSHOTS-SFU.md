# Stream Snapshots: SFU Specification

> **Component:** SFU Server (Go)
> **Version:** 1.2
> **Status:** Implemented (RTP ingress)
> **Related docs:** [01-SFU.md](./01-SFU.md), client/backend snapshot contract in [mollohq/m3llo](https://github.com/mollohq/m3llo) `specs/features/STREAM-SNAPSHOTS-CLIENT.md`

---

## 1. Purpose

The SFU captures still frames from active stream sessions at a low, rate-gated cadence and uploads them to an S3-compatible bucket (Cloudflare R2, MinIO, AWS S3). That is the SFU's entire responsibility. It has no knowledge of Nakama. At session end, Nakama's `StopStreamRPC` lists the R2 prefix for the session and writes `snapshot_urls` into the `stream_session` event in the crew event ledger.

This pipeline must never affect media forwarding. Snapshot work runs outside the relay hot path; failures are logged and dropped without failing the stream.

---

## 2. Design Constraints

- **Zero forwarding impact.** Host ingress RTP is cloned into the snapshot pipeline via a non-blocking tap before fan-out. Relay loops are unchanged.
- **Rate-gated.** One snapshot is captured at most every `SFU_SNAPSHOT_INTERVAL_S` seconds (default: 10). IDR frames arriving faster than this are discarded after the rate check.
- **Async decode and upload.** Decode, scale, JPEG encode, and R2 upload all happen in a dedicated goroutine per session. The forwarding goroutine never blocks on any of these.
- **Bounded memory.** At most one frame is being decoded/encoded at any time per session. RTP ingest uses a 256-packet tap buffer; decode runs on a dedicated worker goroutine.
- **Silent failure.** Any error in the snapshot pipeline (decode failure, R2 timeout, etc.) is logged at WARN and discarded. The session continues normally.
- **No Nakama coupling.** The SFU does not call Nakama at any point. It only writes to R2.
- **Disabled by default.** If `SFU_R2_ENDPOINT` is unset, the snapshot pipeline is not initialized and no tapping occurs.

- **Decoder required when R2 enabled.** If `SFU_R2_ENDPOINT` is set, server startup fails unless the build provides OpenH264 via CGO (`snapshot.DecoderAvailable()`). Prevents a silent no-op pipeline in production.

---

## 3. RTP Ingress Tap

Host H.264 arrives on the negotiated RTP video track (PT 96, packetization-mode 1). After duplicate suppression in `readHostVideoRTP`, accepted packets are tapped before fan-out:

```go
s.tapSnapshotRTP(pkt)   // non-blocking; drops on overflow
s.fanOutVideoRTP(pkt)
```

`tapSnapshotRTP` calls `Pipeline.FeedRTP`, which clones the packet into a bounded channel (capacity 256) with a `select default` discard. When R2 is disabled, `snapshotPipeline` is nil and the tap is a no-op.

### 3.1 Where to Hook

| Location | Symbol |
|---|---|
| Relay loop | `internal/server/stream_session.go`, `readHostVideoRTP` |
| Non-blocking tap | `internal/server/stream_session.go`, `tapSnapshotRTP` |
| Pipeline intake | `internal/snapshot/snapshot.go`, `Pipeline.FeedRTP` |

---

## 4. RTP Depacketization and AU Assembly

`internal/snapshot/rtp_input.go` depacketizes H.264 PT96 packetization-mode 1:

- Single NAL units
- STAP-A (aggregated parameter sets / slices)
- FU-A (fragment reassembly with expiry)

Access units are assembled by RTP timestamp; the RTP **marker** bit completes an AU. Bounds:

| Limit | Value |
|---|---|
| Max packets per AU | 256 |
| Max bytes per AU | 512 KiB |
| AU assembly deadline | 120 ms |
| FU-A fragment expiry | 120 ms |

SPS/PPS NALs are cached from any packet. Only complete IDR AUs with both parameter sets (in-band or cached) proceed to decode; cached SPS/PPS are injected once when absent from the IDR AU. Decode runs on a worker goroutine so RTP ingest is not blocked by OpenH264.

Malformed, duplicate, late, incomplete, or overflowed snapshot input is dropped without affecting RTP relay.

---

## 5. Decode Pipeline

### 5.1 Overview

```
Host RTP packets (H.264 PT96)
    │
    ▼
Depacketize + assemble AU by timestamp/marker (rtp_input.go)
    │
    ▼
IDR AU complete?  NO → continue collecting / drop on expiry
    │ YES
    ▼
Rate gate: has SFU_SNAPSHOT_INTERVAL_S elapsed?
    │ NO → discard
    │ YES ↓
    ▼
Inject cached SPS/PPS if missing
    │
    ▼
CGo OpenH264 decoder → raw YCbCr (YUV420p)
    │
    ▼
Scale to 1280×720 (golang.org/x/image/draw, BiLinear, letterboxed)
    │
    ▼
JPEG encode, quality 75
    │
    ▼
Upload to R2 (async goroutine per upload)
```

### 5.2 OpenH264 CGo Wrapper

`internal/snapshot/h264.go` (build tags `cgo && openh264`) wraps the OpenH264 decoder:

```go
// DecodeIDRFrame decodes a single H.264 Annex B IDR frame.
// nalData must contain a complete IDR frame in Annex B format.
// Returns a YCbCr image or an error. Caller owns the returned image.
func DecodeIDRFrame(nalData []byte) (*image.YCbCr, error)
```

The CGo layer:
- Allocates one `ISVCDecoder` per call (stateless decode of IDR frames is safe).
- Passes `DECODE_WITH_FORCED_KEY_FRAME` option.
- Frees the decoder immediately after the frame is extracted.
- OpenH264 is linked as a shared library (`libopenh264.so`). The SFU Docker image must include it.

Do not maintain a stateful decoder per session. IDR frames are self-contained and stateless decode is correct here.

### 5.3 Scaling

Use `golang.org/x/image/draw` with `draw.BiLinear` to scale to 1280×720. Pure-Go, ~2ms for a 720p output.

If the source frame is already 1280×720 or smaller, skip scaling.

Preserve aspect ratio: letterbox with black bars if the source aspect ratio differs from 16:9.

### 5.4 JPEG Encode

Use `image/jpeg` from the standard library with `jpeg.Options{Quality: 75}`. Expected output size: 80–120KB per frame at 720p.

### 5.5 Rate Gate

```go
type snapshotRateGate struct {
    interval      time.Duration
    lastCaptureAt time.Time
    mu            sync.Mutex
}

func (g *snapshotRateGate) Allow() bool {
    g.mu.Lock()
    defer g.mu.Unlock()
    if time.Since(g.lastCaptureAt) < g.interval {
        return false
    }
    g.lastCaptureAt = time.Now()
    return true
}
```

The interval is set from `SFU_SNAPSHOT_INTERVAL_S` at startup. Default: 10 seconds.

---

## 6. R2 Upload

### 6.1 Client Setup

Initialize a single `*s3.Client` at server startup (not per-session) using `github.com/aws/aws-sdk-go-v2/service/s3` with a custom endpoint resolver pointing at `SFU_R2_ENDPOINT`. Reuse across all sessions.

```go
r2Client, err := snapshot.NewR2Client(snapshot.R2Config{
    Endpoint:        cfg.R2Endpoint,         // the S3 API endpoint
    Bucket:          cfg.R2Bucket,
    AccessKeyID:     cfg.R2AccessKeyID,
    SecretAccessKey: cfg.R2SecretAccessKey,
    PublicDomain:    cfg.R2PublicDomain,
})
```

### 6.2 Key Structure

```
snapshots/{crew_id}/{session_id}/{unix_timestamp_ms}.jpg
```

`crew_id` and `session_id` come from the validated host join and are stored on `StreamSession`.

The deterministic key structure is intentional: Nakama reconstructs snapshot URLs by listing this R2 prefix at `StopStreamRPC` time without any coordination with the SFU.

### 6.3 Upload Behavior

Each JPEG encode produces a `[]byte`. Spawn a goroutine per upload:

```go
go func(data []byte, key string) {
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    if err := r2Client.PutObject(ctx, key, data); err != nil {
        log.Warnf("snapshot: R2 upload failed: key=%s err=%v", key, err)
        return
    }
    log.Debugf("snapshot: uploaded key=%s", key)
}(jpegBytes, key)
```

The SFU does not track which uploads succeeded. Nakama discovers the actual uploaded objects by listing the R2 prefix at session end (see `specs/features/STREAM-SNAPSHOTS-CLIENT.md` §2 in mollohq/m3llo).

---

## 7. Configuration

`internal/server/config.go` (`Config` and `ConfigFromEnv`) reads:

| Env var | Default | Description |
|---|---|---|
| `SFU_R2_ENDPOINT` | `""` | S3 API endpoint of any S3-compatible store (Cloudflare R2, MinIO, AWS S3). Empty = snapshot pipeline disabled. |
| `SFU_R2_BUCKET` | `""` | Bucket name. Required when the endpoint is set. Keep it apart from the clips bucket. |
| `SFU_R2_ACCESS_KEY_ID` | `""` | Access key ID. |
| `SFU_R2_SECRET_ACCESS_KEY` | `""` | Secret access key. |
| `SFU_R2_PUBLIC_DOMAIN` | `""` | Public CDN domain for URL construction (e.g. `snapshots.example.com`). |
| `SFU_SNAPSHOT_INTERVAL_S` | `10` | Minimum seconds between snapshots per session. |

If `SFU_R2_ENDPOINT` is empty, no snapshot pipeline is created and no tapping occurs.

When `SFU_R2_ENDPOINT` is set, `Server.New()` also requires `snapshot.DecoderAvailable()` (CGO + OpenH264 build tag).

---

## 8. Package Layout

```
internal/snapshot/
├── snapshot.go     # Pipeline, FeedRTP(), Start(), Stop(), upload loop
├── rtp_input.go    # H.264 RTP depacketization and AU assembly
├── h264.go         // +build cgo,openh264: OpenH264 wrapper, DecodeIDRFrame()
├── h264_stub.go    // +build !cgo || !openh264: stub, DecoderAvailable() false
├── r2.go           # R2Client, NewR2Client(), PutObject()
└── ratelimit.go    # snapshotRateGate
```

`Pipeline` is created for each stream session when R2 config is present.

---

## 9. Observability

- INFO lifecycle events: `snapshot_pipeline_enabled`, `snapshot_pipeline_started`, `snapshot_uploaded`.
- DEBUG capture event: `snapshot_captured`.
- WARN failures: `snapshot_decode_failed`, `snapshot_render_failed`, `snapshot_encode_failed`, `snapshot_upload_failed`
- DEBUG skips: `snapshot_decode_skipped_no_params`, `snapshot_decode_no_frame`.
- `/admin/api/session/{id}` exposes `snapshot_count`.
- `/health` exposes `snapshots.total_uploaded` (process-lifetime counter).

---

## 10. Code Pointers

| Concern | File |
|---|---|
| RTP tap before fan-out | `internal/server/stream_session.go`, `readHostVideoRTP`, `tapSnapshotRTP` |
| RTP depacketization / AU assembly | `internal/snapshot/rtp_input.go` |
| Session crew_id / session_id storage | `internal/server/stream_session.go`, `StreamSession` struct |
| Config fields | `internal/server/config.go`, `Config`, `ConfigFromEnv` |
| Pipeline init + decoder gate | `internal/server/server.go`, `New` |
| Snapshot pipeline goroutine | `internal/snapshot/snapshot.go`, `Pipeline.FeedRTP` |
| OpenH264 CGo wrapper | `internal/snapshot/h264.go` |
| R2 client | `internal/snapshot/r2.go` |
