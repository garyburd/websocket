// Package websocket implements RFC 6455 clients and servers with a
// message-oriented API.
//
// # Getting started
//
// A server upgrades an HTTP request with [Upgrade], and a client connects with
// [Dial]. Both return a [Conn]. An application then uses the connection in
// three parts:
//
//   - One goroutine reads messages for the connection's lifetime, even if the
//     application ignores them. See Connection management below.
//   - Any number of goroutines write messages.
//   - When the connection ends, call [Conn.Close]. To close gracefully, call
//     [Conn.Shutdown] first and keep reading until the read fails.
//
// This handler echoes each message back to the client:
//
//	func echo(w http.ResponseWriter, r *http.Request) {
//		c, err := websocket.Upgrade(w, r, nil)
//		if err != nil {
//			return // Upgrade has written an HTTP error response
//		}
//		defer c.Close()
//		for {
//			data, mt, err := c.ReadMessage(nil)
//			if err != nil {
//				return
//			}
//			if err := c.WriteMessage(context.Background(), mt, nil, data); err != nil {
//				return
//			}
//		}
//	}
//
// This client function connects to the echo handler, served at
// wss://example.com/echo, sends one message, and prints the echoed reply:
//
//	func hello(ctx context.Context) error {
//		c, _, err := websocket.Dial(ctx, "wss://example.com/echo", nil, nil)
//		if err != nil {
//			return err
//		}
//		defer c.Close()
//		if err := c.WriteMessage(ctx, websocket.MessageText, nil, []byte("hello")); err != nil {
//			return err
//		}
//		data, _, err := c.ReadMessage(nil)
//		if err != nil {
//			return err
//		}
//		fmt.Printf("%s\n", data)
//		return nil
//	}
//
// The examples use [Conn.ReadMessage] and [Conn.WriteMessage], which hold a
// whole message in memory. [Conn.Read] and [Conn.Write] stream a message
// through a callback instead.
//
// Nil options are valid everywhere and mean no limits: no timeouts, no
// keepalive, and no maximum message size. A server that accepts untrusted
// clients should set [UpgradeOptions.MaxMessageSize] and read with
// [ReadOptions] that set keepalive or an idle timeout.
//
// # Concurrency
//
// A [Conn] supports one reader and any number of writers at the same time:
//
//   - One goroutine at a time may call [Conn.Read] or [Conn.ReadMessage]. A
//     concurrent read returns [ErrInvalidArgument].
//   - Any number of goroutines may call [Conn.Write] or [Conn.WriteMessage].
//     Messages are not interleaved: a write waits until the previous message
//     is sent, and its context bounds only that wait. [WriteOptions] bound
//     sending the message itself.
//   - Any goroutine may call [Conn.Shutdown] or [Conn.Close] at any time. Close
//     unblocks reads and writes in progress.
//
// A [Reader] or [Writer] is valid only during the callback that received it.
//
// # Connection management
//
// The package does not read in the background. Keep a [Conn.Read] or
// [Conn.ReadMessage] call active for the connection's lifetime, even when the
// application only writes. Reads answer pings, process pongs and close frames,
// and drive keepalive. Without an active read:
//
//   - the peer's pings go unanswered, and the peer may drop the connection;
//   - keepalive does not run, so a dead peer goes undetected;
//   - the peer's close is not observed;
//   - [Conn.Shutdown] cannot complete, and only its timeout ends the
//     connection.
//
// An application that ignores inbound messages runs [DiscardMessages], which
// closes the connection when the read fails. Pass [ReadOptions] with keepalive
// to detect a dead peer:
//
//	go func() {
//		err := websocket.DiscardMessages(c, opts)
//		log.Printf("connection ended: %v", err)
//	}()
//
// To treat an inbound data message as a protocol violation instead, call
// [Conn.Read] once. It handles control frames while waiting, and the callback
// fails the connection when the first data message arrives:
//
//	go func() {
//		err := c.Read(opts, func(r *websocket.Reader) error {
//			return r.Abort(websocket.ClosePolicyViolation, "unexpected data message")
//		})
//		log.Printf("connection ended: %v", err)
//		c.Close()
//	}()
//
// An application that implements its own read loop must stop on the first
// terminal error, as described at [Conn.Read]; a loop that retries spins.
//
// # Errors
//
// Use errors.As for [CloseError], which reports the peer's close, and
// errors.Is for [ErrClosed] (a local close), [ErrTimeout], [ErrProtocol],
// [ErrCloseSent], and [ErrInvalidArgument]. Handshake failures match
// [ErrBadHandshake] or [ErrBadStatus]. Each method's documentation says which
// errors leave the connection usable.
package websocket
