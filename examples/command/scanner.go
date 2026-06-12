package main

import (
	"iter"

	"github.com/garyburd/websocket"
)

// MessageScanner reads successive messages from a websocket connection with a
// range-over-func loop:
//
//	s := newMessageScanner(conn, opts)
//	for r := range s.All() {
//		// read the message body from r
//	}
//	if err := s.Err(); err != nil {
//		// the connection ended; err says why
//	}
//
// It is built entirely on the exported websocket API ([websocket.Conn.Read]),
// so it lives in this example rather than in the package: a helper like this
// needs no access to package internals.
//
// A websocket connection always ends with an error — even a graceful close
// surfaces as a [*websocket.CloseError] — so Err is non-nil after a loop that
// ran to completion. Closing the connection is the caller's job (typically a
// deferred [websocket.Conn.Close]); the scanner does not close it.
type MessageScanner struct {
	conn *websocket.Conn
	opts *websocket.ReadOptions
	err  error
}

// newMessageScanner returns a scanner over conn's inbound messages, applying
// opts to each read (opts may be nil).
func newMessageScanner(conn *websocket.Conn, opts *websocket.ReadOptions) *MessageScanner {
	return &MessageScanner{conn: conn, opts: opts}
}

// All yields a [*websocket.Reader] for each inbound message until a read fails;
// the failing error is then reported by [MessageScanner.Err]. The Reader is
// valid only for the current iteration — Read closes it when the loop body
// returns, discarding any unread payload — so do not retain it past the body.
//
// Stopping the loop early (break or return) stops reading, which also stops
// keepalive and the closing handshake; close the connection to release it.
func (s *MessageScanner) All() iter.Seq[*websocket.Reader] {
	return func(yield func(*websocket.Reader) bool) {
		// Drive the loop body from inside Conn.Read's callback, where the
		// Reader is valid: yield runs synchronously, and when it returns the
		// callback returns and Read closes the reader.
		var stop bool
		fn := func(r *websocket.Reader) error {
			stop = !yield(r)
			return nil
		}
		for {
			if err := s.conn.Read(s.opts, fn); err != nil {
				s.err = err
				return
			}
			if stop {
				return
			}
		}
	}
}

// Err returns the error that ended the iteration, or nil if the loop has not
// ended or was stopped early. It is the terminal error from
// [websocket.Conn.Read]: a [*websocket.CloseError], [websocket.ErrClosed], a
// timeout, or a transport error.
func (s *MessageScanner) Err() error { return s.err }
