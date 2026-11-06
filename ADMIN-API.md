# Admin API

The SFU has no web UI. It serves a JSON API on the admin port
(`SFU_ADMIN_ADDR`, default `:8080`) for whatever dashboard you build or use.
The full contract, with every field, is in `01-SFU.md` section 8.

## Access

- Every `/admin/api/*` and `/debug/pprof/*` route needs HTTP Basic Auth with
  `SFU_ADMIN_PASSWORD` (any user name).
- Without `SFU_ADMIN_PASSWORD` the routes answer `503`: the admin API is off.
- `SFU_ADMIN_IPS` limits the client addresses (`403` for others).
  `X-Forwarded-For` counts only from `SFU_TRUSTED_PROXIES`.
- Ten wrong passwords from one address within a minute give `429` until the
  oldest of them is a minute old.

```bash
curl -u admin:$SFU_ADMIN_PASSWORD https://sfu.example.com:8080/admin/api/overview
```

The API serves session data, logs and client log bundles of your users.
Treat the password like a database password.

## Open endpoints

| Endpoint | Auth | What |
|---|---|---|
| `GET /health` | none | Status (`ok`, `degraded`, `unhealthy`), version, uptime, session and peer counts, DTLS certificate validity. `503` when the SFU cannot set up calls (section 8.1) |
| `GET /stats` | `Authorization: Bearer <SFU_STATS_TOKEN>` | A compact summary for a status dashboard: active sessions, people, streams, today's peak, CPU, memory. Real users only. `503` without a token configured (section 8.4) |

## Live state

| Endpoint | What |
|---|---|
| `GET /admin/api/overview` | Sessions, peers, bandwidth and CPU now |
| `GET /admin/api/sessions` | The sessions running now, with their peers and trouble counters |
| `GET /admin/api/session/{id}` | One live session: its peers and, for a stream, the relay counters |
| `GET /admin/api/logs` | The in-memory log ring. Filters: `level`, `event`, `session`, `user`, `after`, `before`, `days`, `limit` |
| `GET /admin/api/peers` | This instance and the sibling SFUs of `SFU_PEERS` |
| `GET /admin/api/troubleshoot?window_minutes=N` | Recent incidents, the renegotiation funnel, the capacity envelope, sessions in trouble |

## History (needs the media store)

These read the SQLite store of `SFU_STORE_PATH` and answer `503` without it
(section 8.6).

| Endpoint | What |
|---|---|
| `GET /admin/api/overview?range=1h\|24h\|7d` | The overview with series: peers, peer minutes, quality, time to first keyframe, CPU, bandwidth |
| `GET /admin/api/sessions?from&to&type&user&crew&quality&limit` | Sessions in a time range, live and ended, with their quality grade |
| `GET /admin/api/session/{id}/timeline?start&from&to&window` | One run of a session: its peers, stats rows, relay rows, events and graded minutes |
| `GET /admin/api/session/{id}/events?start` | One run and its events |
| `GET /admin/api/logs?from&to&...` | The log on disk, paged by `next_to` |
| `GET /admin/api/alerts?from&to&limit` | Stored warnings: capacity, connectivity, media loss, no media |
| `GET /admin/api/minutes?days=N` | Peer minutes per UTC day, voice and stream, real and synthetic. `N` from 1 to 90 |

Times are unix milliseconds or RFC 3339. A peer minute is one person
connected for one minute: a call of 3 people for 10 minutes is 30.

Quality grades (section 8.7): each peer minute is graded on loss, freezes,
A/V offset, voice concealment and frame rate, and a degraded minute names its
leg (`sfu`, `host_uplink`, `host_machine`, `viewer_leg`, ...). A session is
`good`, `degraded` or `bad`.

## One person (needs the media store)

| Endpoint | What |
|---|---|
| `GET /admin/api/user/{id}/sessions?from&to` | The sessions the person was in, with their own grade |
| `GET /admin/api/user/{id}/profile` | Their machine and link from recent reports: GPU, encoder, decoder, RTT |
| `POST /admin/api/user/{id}/request-logs` | Asks the person's connected client for its app log of the last 24 hours |
| `GET /admin/api/user/{id}/bundles` | The log bundles received, and recent requests with their status |
| `GET /admin/api/user/{id}/bundles/{name}?q&limit` | Lines of one bundle, or the lines that contain `q` |
| `GET /admin/api/user/{id}/bundles/{name}/download` | The bundle as a gzip file |

A log bundle comes over the client's own signaling connection and is kept
next to the store, deleted with the store's retention (section 8.8). Ask
for one only to debug a problem the person reported.

## Profiling

`/debug/pprof/*` is Go's profiler, behind the same auth.
