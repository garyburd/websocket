// Package websocket implements RFC 6455 clients and servers with a
// message-oriented API.
//
// # Connections
//
// Clients use [Dial]. HTTP handlers use [Upgrade], which requires HTTP/1.x and
// rejects cross-origin requests by default. Use [AllowedOrigins] to configure
// origin policy and [SupportedSubprotocols] to select a subprotocol.
//
// # Messages
//
// [Conn.ReadMessage] and [Conn.WriteMessage] use complete byte slices.
// [Conn.Read] and [Conn.Write] stream messages through scoped callbacks.
//
// # Concurrency
//
// One goroutine may read while any number write. Concurrent reads return
// [ErrInvalidArgument]. A write context bounds only writer acquisition;
// [ReadOptions] and [WriteOptions] bound message I/O.
//
// # Connection management
//
// The package does not read in the background. Keep a [Conn.Read] or
// [Conn.ReadMessage] call active for the connection's lifetime, even when the
// application discards data messages. Reads answer pings, process pongs and
// close frames, and drive keepalive.
//
// This loop discards data messages while preserving connection management.
// Returning nil from the callback drains the current message:
//
//	func readUntilClosed(c *websocket.Conn, opts *websocket.ReadOptions) error {
//		defer c.Close()
//		for {
//			err := c.Read(opts, func(*websocket.Reader) error {
//				return nil
//			})
//			if err != nil {
//				return err
//			}
//		}
//	}
//
// [Conn.Shutdown] sends a close frame and returns without waiting. Continue
// reading until a read returns an error, then call [Conn.Close]. Without an
// active read, the peer's close response is not processed and graceful shutdown
// cannot complete.
//
// # Closing and errors
//
// [Conn.Close] immediately tears down the transport, unblocking in-flight
// reads and writes.
//
// Terminal read errors are sticky. Use errors.As for [CloseError] and errors.Is
// for [ErrClosed], [ErrTimeout], [ErrProtocol], and [ErrInvalidArgument]. Stop
// the read loop on any terminal error.
//
// A write returns [ErrCloseSent] while a closing handshake is in progress:
// stop writing data, but keep reading so the handshake completes.
//
// # Text messages
//
// Inbound text is validated as UTF-8 unless
// [ReadOptions.SkipUTF8Validation] is set. Outbound text is not validated;
// check untrusted text (e.g. utf8.Valid) before sending, or the peer may
// close the connection with 1007.
package websocket
