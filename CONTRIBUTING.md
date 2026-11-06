# Contributing

Thanks for helping. A few rules keep the SFU predictable.

## Before you start

- Read `01-SFU.md`. It is the contract for signaling, media, auth, the admin
  API and error codes. A change to any of those updates `01-SFU.md` in the
  same pull request.
- Read `AGENTS.md`. It lists the invariants (two media paths, no
  transcoding, explicit renegotiation) and the files that need extra care.
- For anything larger than a fix, open an issue first and describe the
  change. It saves both of us time.

## Contributor License Agreement

Before we can merge your first pull request, you sign our Contributor License
Agreement. The CLA bot asks you on the pull request; it takes a minute. It
lets Mollo Tech AB keep licensing the SFU, including under the Apache License
2.0 that every version turns into.

## Making the change

- Go: the version in `go.mod`. Run `gofmt`, `go vet ./...` and
  `go test -race ./...`.
- If you touch the snapshot decode path, also run `./test-local.sh` (needs
  OpenH264, see the script).
- If you touch forwarding, renegotiation or the slow-peer policy, run the
  voice and stream soaks in the README and put the numbers in the pull
  request.
- Keep error codes, env var names and admin JSON fields stable. A breaking
  change says so in the commit (`BREAKING CHANGE:`).
- Commit messages follow Conventional Commits (`fix:`, `feat:`, `docs:`).

## Reporting bugs

Open an issue with the SFU version (`/health` shows it), the log lines around
the problem (`SFU_LOG_LEVEL=debug` helps) and how to reproduce it. Security
problems go through [SECURITY.md](./SECURITY.md), not issues.
