# Chat Example

This application shows how to use the
[websocket](https://github.com/garyburd/websocket) package to implement a
simple web chat application.

This README is a roadmap for running the example and finding your way around the
source; the explanations live in comments in the code.

## Scope

This example exists to demonstrate how to interact with a websocket connection:
upgrading, reading and writing messages, keepalive, and the closing handshake.
Chat is just a familiar setting for that.

It is deliberately not a foundation for a real chat application. Things a real
app would need, such as user identities, authentication, multiple rooms,
message history, or persistence, are intentionally left out so the websocket
mechanics stay front and center.

## Running the example

The example requires a working Go installation
([Getting Started](https://go.dev/doc/install)). Run it directly:

```
go run github.com/garyburd/websocket/examples/chat@latest
```

or from a clone of the repository:

```
git clone https://github.com/garyburd/websocket
cd websocket
go run ./examples/chat
```

Then open http://localhost:8080/ in one or more browser windows. Lines typed
in one window appear in all of them. Use the `-addr` flag to listen on a
different address.

## Reading the code

The server creates one `Peer` per websocket connection; each `Peer` is a
middleman between its connection and a single shared `Hub` that broadcasts
messages to all peers. A tour of the files, and where each part is explained:

- **[main.go](main.go)** — wiring: the `/` handler serves the embedded
  `home.html`, and `/ws` hands each websocket request to `serveWs`. One `Hub` is
  created in `main` and shared by every connection.

- **[hub.go](hub.go)** — the `Hub`: the set of connected peers behind a mutex,
  with `register`, `unregister`, and `broadcast` callable from any goroutine.
  The comments cover how a full or dead peer is dropped with a non-blocking
  send (so a slow peer never blocks the hub) and how each peer's `send`
  channel is closed exactly once.

- **[peer.go](peer.go)** — the per-connection `Peer` and the `serveWs`
  handler. The `Peer` doc comment explains the two-goroutine model (one
  direction each); `readPump` owns the read loop and keepalive, while `writePump`
  owns writes, coalesces queued messages, and drives the closing handshake.

- **[home.html](home.html)** — the browser frontend, embedded with `go:embed`.
  It opens the websocket, splits incoming (possibly coalesced) lines into the
  log, and sends the input field on submit. See `appendLog` for the
  scroll-preserving behavior.
