# Security

## Reporting a vulnerability

Report it privately through GitHub: the **Security** tab of this repository,
then **Report a vulnerability**. Do not open a public issue.

Please include:

- what an attacker can do, and what they need first (network access, a valid
  token, the admin password),
- the version or commit,
- steps or a small program that shows it.

We answer within a week, agree a fix and a date with you, and credit you in
the release notes unless you ask us not to.

## Supported versions

The latest release gets security fixes. Run it.

## Hardening a deployment

The SFU fails closed, but a deployment still needs these:

- `SFU_JWT_PUB_KEY` set to your backend's public key. Never run
  `SFU_DEV_MODE=true` on a reachable host: it accepts any token.
- `SFU_ADMIN_PASSWORD` set to a long random value, and `SFU_ADMIN_IPS` set to
  the addresses that read the admin API. The admin API serves session data,
  logs and client log bundles.
- `SFU_TEST_PASSWORD` empty unless you run soaks or a canary against this
  instance. It travels in a query string.
- `SFU_TRUSTED_PROXIES` only for proxies you run.
- Signaling over TLS (`SFU_CERT_FILE` and `SFU_KEY_FILE`, or a reverse proxy).

See [SELF-HOSTING.md](./SELF-HOSTING.md) and `01-SFU.md` section 7.
