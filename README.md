# m3llo-sfu

The selective forwarding unit (SFU) of [m3llo](https://m3llo.app), the app
for small gaming crews. It relays voice and game streams between the people
in a call. It forwards packets and never decodes, mixes or records them.

- **Voice:** Opus RTP between the members of a voice channel, with receive
  slots so joins and leaves need no renegotiation.
- **Streams:** one host's H.264 and Opus RTP to many viewers, with NACK
  repair on each hop, per-viewer bandwidth estimates and a late-join
  keyframe cache.
- **Auth:** RS256 tokens signed by your m3llo backend. The SFU never talks
  to the backend.

Written in Go on [Pion](https://github.com/pion/webrtc). The client and the
backend are in [mollohq/m3llo](https://github.com/mollohq/m3llo).

## License

[FSL-1.1-ALv2](./LICENSE). You can run, change and self-host it for free,
for your own crews and communities. You may charge members to cover what it
costs you to run. You may not sell it, or a service built on it, as a
commercial product. Each version becomes Apache 2.0 two years after its
release. The m3llo client and backend are Apache 2.0.

## Run it

To run your own SFU for your m3llo backend, follow
[SELF-HOSTING.md](./SELF-HOSTING.md).

To try it on one machine:

```bash
docker compose up -d --build
curl http://localhost:8080/health
```

The compose file expects the backend's public key at `local/jwt_pub.pem`.
`backend/scripts/dev-sfu-keys.sh` in mollohq/m3llo makes a key pair for a
local backend and writes it there. Ports: 8443
signaling, 8080 health and admin, UDP 10000-10100 media, TCP 10000 media
over ICE-TCP.

`SFU_PUBLIC_IP` is the address the SFU puts in its ICE candidates. It
defaults to `127.0.0.1`, which works for clients on the same machine. For a
phone or a second computer, set the LAN address:
`SFU_PUBLIC_IP=192.0.2.10 docker compose up -d --build`.

## Configuration

All configuration is environment variables.

**Network**

| Variable | Default | Meaning |
|---|---|---|
| `SFU_ADDR` | `:8443` | Signaling listen address (WebSocket at `/ws`) |
| `SFU_ADMIN_ADDR` | `:8080` | Health, stats, admin API and pprof listen address |
| `SFU_PUBLIC_IP` | | The IPv4 address clients reach. Set it on any real deployment: it turns on ICE-lite and the UDP mux on port 10000 |
| `SFU_ICE_TCP_PORT` | `10000` | Passive ICE-TCP port, for clients on networks that block UDP. `0` turns it off. Needs `SFU_PUBLIC_IP` |
| `SFU_CERT_FILE`, `SFU_KEY_FILE` | | TLS certificate and key for signaling and the admin port. Leave empty behind a TLS reverse proxy |

**Auth** (`01-SFU.md` section 7)

| Variable | Default | Meaning |
|---|---|---|
| `SFU_JWT_PUB_KEY` | | Path to the backend's RS256 public key (PEM). Without it every token is refused |
| `SFU_REGION` | `eu-west` | This SFU's region. A token signed for another region is refused. Must match the region name your backend signs |
| `SFU_ADMIN_PASSWORD` | | Password of the admin API and pprof (HTTP Basic Auth). Empty: the admin API is off |
| `SFU_ADMIN_IPS` | | Comma-separated IPs or CIDRs allowed to reach the admin API. Empty: password only |
| `SFU_TRUSTED_PROXIES` | | Comma-separated IPs or CIDRs of reverse proxies whose `X-Forwarded-For` names the client. Empty: the header is ignored |
| `SFU_STATS_TOKEN` | | Bearer token of `/stats`, a compact summary for a status dashboard. Empty: `/stats` is off |
| `SFU_TEST_PASSWORD` | | Password of test-mode joins (`?test_password=`) for soaks and canaries. Empty: test mode is off |
| `SFU_DEV_MODE` | `false` | `true`: accepts any token without a key and leaves the admin API open. Local development only |

The SFU does not start when it can authenticate no one: no key, no test
password and no dev mode.

**Limits and identity**

| Variable | Default | Meaning |
|---|---|---|
| `SFU_SERVER_ID` | `sfu-01` | This instance's name, in `/health` and the logs |
| `SFU_MAX_SESSIONS` | `500` | Most voice and stream sessions at once |
| `SFU_MAX_PEERS_PER_SESSION` | `100` | Most people in one session |
| `SFU_PEERS` | | Comma-separated `host:port` admin addresses of sibling SFUs, for the admin API's instance view |
| `SFU_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. Logs are JSON lines on stdout |
| `SFU_REJECT_LEGACY_AUDIO_DC` | `false` | `true` closes the `audio` DataChannel that older m3llo clients open (`01-SFU.md` section 5.3) |

**Media store** (`01-SFU.md` section 8.6). Keeps logs, session timelines
and quality grades for the admin API. Off without a path.

| Variable | Default | Meaning |
|---|---|---|
| `SFU_STORE_PATH` | | SQLite file. Put it on a persistent volume |
| `SFU_STORE_RETENTION_DAYS` | `30` | Rows older than this are deleted |
| `SFU_LOG_RING_SIZE` | `10000` | Log lines kept in memory |

**Stream snapshots** (`STREAM-SNAPSHOTS-SFU.md`). Still frames of live
streams for the crew feed, uploaded to any S3-compatible store. Off without
an endpoint. Needs an image built with OpenH264 (see SELF-HOSTING.md).

| Variable | Default | Meaning |
|---|---|---|
| `SFU_R2_ENDPOINT` | | S3 API endpoint (Cloudflare R2, MinIO, AWS S3) |
| `SFU_R2_BUCKET` | | Bucket |
| `SFU_R2_ACCESS_KEY_ID`, `SFU_R2_SECRET_ACCESS_KEY` | | Credentials |
| `SFU_R2_PUBLIC_DOMAIN` | | Public domain of the bucket, for snapshot URLs |
| `SFU_SNAPSHOT_INTERVAL_S` | `10` | Fewest seconds between two snapshots of one stream |

## Docs

| File | What it covers |
|---|---|
| [SELF-HOSTING.md](./SELF-HOSTING.md) | Running the SFU for your own backend |
| [ADMIN-API.md](./ADMIN-API.md) | The JSON admin API |
| [01-SFU.md](./01-SFU.md) | The contract: signaling, media, auth, observability, error codes |
| [STREAM-SNAPSHOTS-SFU.md](./STREAM-SNAPSHOTS-SFU.md) | The snapshot pipeline |
| [TESTING.md](./TESTING.md) | Unit tests, soaks and the canary |
| [CONTRIBUTING.md](./CONTRIBUTING.md) | How to send a change |
| [SECURITY.md](./SECURITY.md) | Reporting a vulnerability, hardening a deployment |

## Code

```
cmd/sfu/           Entry point
internal/server/   Sessions, peers, signaling, forwarding, admin API
internal/auth/     JWT verification
internal/store/    Media store (SQLite)
internal/snapshot/ Stream snapshots
internal/minutes/  Peer minutes per day
internal/logging/  JSON logger and ring buffer
internal/metrics/  Counters and history
tools/             Soaks, canary and RTP test tools
```
