# Autobahn|Testsuite

Conformance testing against the [Autobahn|Testsuite][suite], the standard
oracle for RFC 6455 framing, fragmentation, UTF-8, and close-handshake edge
cases.

[suite]: https://github.com/crossbario/autobahn-testsuite

## How it works

`autobahn_test.go` runs the echo server and client driver **in-process** and
runs the suite's `wstest` tool from its Docker image
(`crossbario/autobahn-testsuite`) — `wstest` is Python 2 only, so Docker avoids
any local Python install. It then parses the JSON report and fails on any case
the suite marks `FAILED`.

- `TestAutobahnServer` points `wstest -m fuzzingclient` at our `Upgrade` echo
  server.
- `TestAutobahnClient` drives our `Dial` client through `wstest -m fuzzingserver`.
- `config/` holds the two suite config files.

The tests are compiled by ordinary `go test ./...` (so an API change here is a
build error) but **skip** unless explicitly enabled, since they need Docker and
take a few minutes.

## Running

Prerequisites: a Go toolchain and Docker.

```sh
AUTOBAHN=1 go test -run Autobahn -v ./autobahn/
```

Browsable per-case results land in `reports/servers/index.html` and
`reports/clients/index.html`.

## Notes on expected results

- **UTF-8.** The library deliberately does not validate UTF-8 on inbound text
  payloads — it delivers bytes verbatim and documents that applications should
  validate at their layer. The echo program does exactly that, failing the
  connection with close code 1007, which the suite's `6.*` cases require. So the
  run reflects "library + the documented application-side check."
- **permessage-deflate.** Not implemented, so the compression cases (`12.*`,
  `13.*`) report `UNIMPLEMENTED`, which is not treated as a failure.
- A genuinely `FAILED` case is a real conformance gap; open the HTML report for
  details.
