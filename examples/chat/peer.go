package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/garyburd/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Send a keepalive ping to the peer after this much inbound silence.
	keepaliveInterval = 54 * time.Second

	// Time allowed to send a keepalive ping and receive the peer's pong.
	keepaliveTimeout = 10 * time.Second

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

var upgradeOptions = &websocket.UpgradeOptions{
	MaxMessageSize:      maxMessageSize,
	ControlReplyTimeout: writeWait, // bounds writing the automatic pong and close replies
	ShutdownTimeout:     writeWait, // budget for the closing handshake (close write + echo wait)
}

// Peer is a middleman between the websocket connection and the hub. Each
// connection runs two peer goroutines: readPump moves inbound messages to the
// hub, and writePump moves the hub's outbound messages, buffered on send, to the
// connection. Keeping all reads on readPump and all writes on writePump keeps
// message ordering predictable and the keepalive read outstanding; the package
// would serialize concurrent readers and writers, but one goroutine per
// direction is simpler to reason about.
type Peer struct {
	hub *Hub

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. Keepalive
// runs on the read side: while readPump waits for the next message, the
// ReadOptions below ping the peer after keepaliveInterval of silence and fail
// the read if the ping and the peer's pong do not complete within
// keepaliveTimeout.
func (c *Peer) readPump() {
	defer func() {
		c.hub.unregister(c)
		c.conn.Close()
	}()
	opts := &websocket.ReadOptions{
		KeepaliveInterval: keepaliveInterval,
		KeepaliveTimeout:  keepaliveTimeout,
		MatchPong:         true,
	}
	for {
		message, _, err := c.conn.ReadMessage(opts)
		if err != nil {
			// Expected ends: the peer closed (browser tab closed or
			// navigated away), the peer vanished without a close frame,
			// or this side shut down. Log anything else.
			var ce *websocket.CloseError
			if !errors.As(err, &ce) && !errors.Is(err, websocket.ErrClosed) {
				log.Printf("read: %v", err)
			}
			return
		}
		message = bytes.TrimSpace(bytes.ReplaceAll(message, newline, space))
		c.hub.broadcast(message)
	}
}

// writePump pumps messages from the hub to the websocket connection. To cut
// system calls and bytes on the wire under load, it coalesces any messages
// already queued on send into the one websocket message it is writing.
//
// A goroutine running writePump is started for each connection. The
// hub's close of the send channel ends the loop, after which writePump
// starts the closing handshake; readPump collects the peer's close echo
// and releases the connection.
func (c *Peer) writePump() {
	opts := &websocket.WriteOptions{MessageTimeout: writeWait}
	for message := range c.send {
		err := c.conn.Write(context.Background(), websocket.MessageText, opts, func(w *websocket.Writer) error {
			if _, err := w.Write(message); err != nil {
				return err
			}
			// Add queued chat messages to the current websocket message.
			for range len(c.send) {
				if _, err := w.Write(newline); err != nil {
					return err
				}
				if _, err := w.Write(<-c.send); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			// Per Conn.Write's contract a write error has already closed the
			// connection or started a graceful close, so readPump's read will
			// end and release the connection. Just stop writing; do not Close
			// here, which would race readPump's collection of the close echo.
			return
		}
	}
	// The hub closed the channel. Begin the closing handshake; the peer's
	// echo surfaces in readPump as a *CloseError (or the shutdown timeout
	// fires), and readPump closes the connection. writePump must not Close
	// here — that would race readPump's collection of the echo.
	c.conn.Shutdown(websocket.CloseNormalClosure, "")
}

// serveWs handles a websocket request: it upgrades the connection, creates a
// Peer, registers it with the hub, and starts the peer's pumps. The
// UpgradeOptions cap the inbound message size, bound writing the automatic pong
// and close replies, and bound the closing handshake.
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Upgrade(w, r, upgradeOptions)
	if err != nil {
		// Upgrade has already written an error response.
		log.Println(err)
		return
	}
	peer := &Peer{hub: hub, conn: conn, send: make(chan []byte, 256)}
	peer.hub.register(peer)

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go peer.writePump()
	go peer.readPump()
}
