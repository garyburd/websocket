# websocket

A [WebSocket](https://datatracker.ietf.org/doc/html/rfc6455) (RFC 6455) client
and server implementation for [Go](https://go.dev/).

[![Go Reference](https://pkg.go.dev/badge/github.com/garyburd/websocket.svg)](https://pkg.go.dev/github.com/garyburd/websocket)

## Features

- No dependencies beyond the Go standard library.
- Built-in ping/pong liveness checks with per-read controls for keepalive
  cadence, pong deadlines, idle timeouts, and message progress bounds.
- Closing handshake support through the `Shutdown` API: send a close code and
  reason, then collect the peer's response from the read loop.
- Passes the complete WebSocket
  [Autobahn test suite](https://github.com/crossbario/autobahn-testsuite) for
  all supported features.

## Installation

```
go get github.com/garyburd/websocket
```

## Documentation

API documentation and examples covering clients, servers, reading, writing,
and liveness options are available on
[pkg.go.dev](https://pkg.go.dev/github.com/garyburd/websocket).

For complete applications, see these examples:

- [chat](examples/chat) — a multi-user chat server.
- [command](examples/command) — bridges a subprocess (a calculator by default)
  to a browser and demonstrates a message-scanning read loop built on the
  API.

## Roadmap

Support for the permessage-deflate compression extension
([RFC 7692](https://datatracker.ietf.org/doc/html/rfc7692)) is planned but not
yet implemented.

## Why this package?

When writing gorilla/websocket, I made several design decisions that I later
came to regret. Once those decisions became part of the public API, correcting
them would have broken existing applications.

I wanted to build a gorilla/websocket v2 that addressed those issues, but for a
long time the work was hard to justify: it was a substantial undertaking, and
I no longer needed a WebSocket package myself.

That changed when a recent project required WebSockets. LLM assistance made it
practical to turn the design into Go code, and this package is the result.
