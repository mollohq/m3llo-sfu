# Self-hosting the SFU

The SFU carries all voice and streams of a m3llo backend. If you run your own
backend (see [mollohq/m3llo](https://github.com/mollohq/m3llo)), you run your
own SFU next to it. Your backend signs a token for each call, and your SFU
checks it with the backend's public key. The two never talk to each other.

You can charge your crews and communities what running it costs you. See
[LICENSE](./LICENSE).

## What you need

- A Linux host with a public IPv4 address. 2 vCPU and 4 GB RAM is plenty for
  a few crews. Bandwidth runs out before CPU: a stream at 8 Mbps to 10
  viewers sends 80 Mbps. Voice is about 64 kbps for each speaker, to each
  listener.
- Docker.
- A DNS name for the host, for example `sfu.example.com`, and a TLS
  certificate for it (Let's Encrypt works).
- These ports open to the internet:

| Port | Protocol | What |
|---|---|---|
| 8443 | TCP | Signaling (WebSocket, TLS) |
| 8080 | TCP | `/health`, which clients probe to pick a region, and the admin API |
| 10000 | UDP | Media |
| 10000 | TCP | Media for clients on networks that block UDP (ICE-TCP) |

No TURN server is needed. Clients that cannot use UDP reach the SFU over
ICE-TCP on port 10000.

## 1. The key pair

Your backend signs tokens with a private key, and the SFU verifies them with
the public key. Make the pair once:

```bash
openssl genrsa -out sfu_private.pem 2048
openssl rsa -in sfu_private.pem -pubout -out sfu_public.pem
```

`sfu_private.pem` goes to your backend (`SFU_JWT_PRIVATE_KEY`).
`sfu_public.pem` goes to the SFU host. Commit neither.

## 2. TLS

Clients connect to `wss://sfu.example.com:8443/ws` and probe
`https://sfu.example.com:8080/health`, so the SFU serves TLS on both ports
itself. Get a certificate, for example with certbot:

```bash
sudo certbot certonly --standalone -d sfu.example.com
```

The SFU reads the certificate at start. Restart it after each renewal (a
certbot deploy hook can run `docker restart m3llo-sfu`).

## 3. Run it

Put the public key and the certificate where the container can read them,
for example `/etc/m3llo-sfu/`:

```
/etc/m3llo-sfu/jwt_pub.pem
/etc/m3llo-sfu/cert.pem    (fullchain.pem)
/etc/m3llo-sfu/key.pem     (privkey.pem)
/etc/m3llo-sfu/sfu.env
```

`sfu.env`, mode 600:

```bash
SFU_PUBLIC_IP=203.0.113.10
SFU_REGION=eu-west
SFU_SERVER_ID=sfu-1
SFU_JWT_PUB_KEY=/certs/jwt_pub.pem
SFU_CERT_FILE=/certs/cert.pem
SFU_KEY_FILE=/certs/key.pem
SFU_ADMIN_PASSWORD=<a long random value: openssl rand -hex 24>
SFU_ADMIN_IPS=<your own address, comma-separated>
SFU_STORE_PATH=/data/media.db
```

`docker-compose.yml` on the host:

```yaml
services:
  sfu:
    image: ghcr.io/mollohq/m3llo-sfu:<version>
    container_name: m3llo-sfu
    restart: unless-stopped
    # Host networking: the SFU advertises SFU_PUBLIC_IP in its ICE candidates
    # and listens on the host's ports directly.
    network_mode: host
    env_file: /etc/m3llo-sfu/sfu.env
    volumes:
      - /etc/m3llo-sfu:/certs:ro
      - sfu-data:/data
volumes:
  sfu-data:
```

```bash
docker compose up -d
curl https://sfu.example.com:8080/health
```

`/health` answers `"status": "ok"` when the SFU can set up calls. The start
log line `admin_listening` says whether the admin API is on.

## 4. Connect your backend

Give your backend:

- the private key (`SFU_JWT_PRIVATE_KEY`),
- the SFU's signaling address, `wss://sfu.example.com:8443/ws`,
- the region name you gave the SFU in `SFU_REGION`.

The region names must match. The backend puts its region in every token, and
the SFU refuses a token for another region. The backend guide in
[mollohq/m3llo](https://github.com/mollohq/m3llo) names the settings.

Then join a voice channel from two clients. The admin API shows the session:

```bash
curl -u admin:<SFU_ADMIN_PASSWORD> https://sfu.example.com:8080/admin/api/sessions
```

## More than one region

Run one SFU per region, each with its own `SFU_REGION`, `SFU_SERVER_ID` and
`SFU_PUBLIC_IP`, all with the same public key. Clients probe each region's
`/health` and the backend signs for the one they pick. Optionally list the
other SFUs' admin addresses in `SFU_PEERS`, so the admin API shows all of
them.

## Stream snapshots (optional)

The SFU can save a still frame of each live stream every few seconds to an
S3-compatible bucket, for the crew feed. Decoding the frames needs OpenH264,
which the default image does not include: H.264 is patent-encumbered, and
the license Cisco offers covers only the binaries Cisco ships. To use
snapshots, build the image yourself and check that your use is covered:

```bash
docker build --build-arg OPENH264=1 -t m3llo-sfu:snapshots .
```

Then set the `SFU_R2_*` variables in the [README](./README.md#configuration).

## Upgrading

Pull the new image tag and run `docker compose up -d`. Calls in progress
drop and the clients rejoin on their own. The sessions are in memory; the
media store on the volume stays.

## Security

Read the hardening list in [SECURITY.md](./SECURITY.md). In short: always set
`SFU_JWT_PUB_KEY` and `SFU_ADMIN_PASSWORD`, restrict `SFU_ADMIN_IPS`, never
set `SFU_DEV_MODE` on a reachable host, and leave `SFU_TEST_PASSWORD` empty
unless you run a canary.
