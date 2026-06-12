package websocket

import (
	"fmt"
	"net/http"
	"time"
)

// DialOptions configures [Dial]. Zero values disable optional behavior;
// WriteBufferSize defaults to 4 KiB.
//
// Dial reads the options only during the call.
type DialOptions struct {
	// Transport performs the HTTP upgrade. If nil, Dial uses an
	// [http.Transport] with HTTP/2 disabled and [http.ProxyFromEnvironment].
	//
	// A custom transport must use HTTP/1.1 and return an [io.ReadWriteCloser]
	// as the body of a 101 response. The standard [http.Transport] does so.
	// Do not share a [tls.Config] (or the whole transport) with net/http: that
	// puts "h2" in NextProtos, ALPN negotiates HTTP/2, and the handshake fails.
	Transport http.RoundTripper

	// Subprotocols lists Sec-WebSocket-Protocol values in preference order.
	// Each value must be a nonempty HTTP token. See [Conn.Subprotocol].
	Subprotocols []string

	// ControlReplyTimeout bounds automatic pong and close writes. Zero waits
	// indefinitely. It does not apply to keepalive pings, [Conn.Shutdown], or
	// data messages.
	ControlReplyTimeout time.Duration

	// ShutdownTimeout bounds both sending a close and waiting for its response.
	// Expiry tears down the connection with [ErrTimeout]. Zero waits indefinitely.
	ShutdownTimeout time.Duration

	// MaxMessageSize limits one inbound message. Exceeding it sends
	// [CloseMessageTooBig] and returns [ErrProtocol]. Zero is unlimited.
	MaxMessageSize int

	// WriteBufferSize is the per-frame coalescing buffer size. Zero uses 4 KiB;
	// values below 64 use 64.
	WriteBufferSize int

	// ResponseBodyLimit limits the captured body of a failed handshake. Zero
	// discards it. A malformed 101 body is always discarded because it is the
	// upgraded connection, not an HTTP error body.
	ResponseBodyLimit int

	// ManualCloseResponse disables the automatic response to a peer close.
	// The application may finish pending writes, then call [Conn.Shutdown].
	ManualCloseResponse bool
}

// UpgradeOptions configures [Upgrade]. Zero values disable optional behavior;
// WriteBufferSize defaults to 4 KiB.
//
// Upgrade reads the options only during the call.
type UpgradeOptions struct {
	// SelectSubprotocol selects a value offered in Sec-WebSocket-Protocol.
	// Return "" for none. See [SupportedSubprotocols].
	SelectSubprotocol func(*http.Request) string

	// CheckOrigin approves the Origin header. If nil, Upgrade accepts an absent
	// Origin or one whose host equals the request Host. See [AllowedOrigins].
	// Browsers do not apply CORS to WebSocket upgrades, so a permissive check
	// invites cross-site WebSocket hijacking.
	CheckOrigin func(*http.Request) bool

	// ResponseHeader adds headers to the 101 response. Upgrade overwrites its
	// protocol-managed headers.
	ResponseHeader http.Header

	// OnError writes a rejected handshake response. err wraps
	// [ErrBadHandshake]. The callback must not hijack or write a success status.
	// If nil, Upgrade uses [http.Error].
	OnError func(w http.ResponseWriter, r *http.Request, status int, err error)

	// ControlReplyTimeout bounds automatic pong and close writes. Zero waits
	// indefinitely. It does not apply to keepalive pings, [Conn.Shutdown], or
	// data messages.
	ControlReplyTimeout time.Duration

	// ShutdownTimeout bounds both sending a close and waiting for its response.
	// Expiry tears down the connection with [ErrTimeout]. Zero waits indefinitely.
	ShutdownTimeout time.Duration

	// MaxMessageSize limits one inbound message. Exceeding it sends
	// [CloseMessageTooBig] and returns [ErrProtocol]. Zero is unlimited.
	MaxMessageSize int

	// WriteBufferSize is the per-frame coalescing buffer size. Zero uses 4 KiB;
	// values below 64 use 64.
	WriteBufferSize int

	// ManualCloseResponse disables the automatic response to a peer close.
	// The application may finish pending writes, then call [Conn.Shutdown].
	ManualCloseResponse bool
}

// ReadOptions configures one [Conn.Read] or [Conn.ReadMessage]. Nil disables
// read-side liveness checks.
//
// A read copies the options when it begins. Idle and keepalive settings apply
// before a message; message timeout, rate, and [Reader.SetReadDeadline] apply
// to its body.
type ReadOptions struct {
	// IdleTimeout bounds the wait for the next data message. Control frames do
	// not reset it. Zero waits indefinitely; expiry is terminal.
	IdleTimeout time.Duration

	// KeepaliveInterval sends a ping after this much inbound silence. Qualifying
	// inbound frames reset the interval. Zero disables keepalive.
	KeepaliveInterval time.Duration

	// KeepaliveTimeout bounds sending a keepalive ping and receiving a qualifying
	// response. Zero sends pings without requiring a response. It requires a
	// nonzero KeepaliveInterval.
	//
	// Allow enough time for any in-flight message plus the round trip. The
	// interval restarts when a check completes, so the gap between pings is
	// KeepaliveInterval plus the check's duration; to survive an external idle
	// limit (a proxy or load balancer), keep KeepaliveInterval +
	// KeepaliveTimeout below that limit.
	KeepaliveTimeout time.Duration

	// MatchPong requires a pong matching this connection's ping. Otherwise any
	// inbound control frame refreshes keepalive.
	MatchPong bool

	// MessageTimeout bounds a message body from message start. Zero is unbounded.
	// It conflicts with MinReadRate; [Reader.SetReadDeadline] overrides it.
	MessageTimeout time.Duration

	// MinReadRate is the minimum body rate in bytes per second. A read receives
	// size/rate time, so large buffers delay stall detection. Zero disables the
	// rate check. It conflicts with MessageTimeout and [Reader.SetReadDeadline].
	MinReadRate int

	// SkipUTF8Validation disables validation of inbound text messages. Invalid
	// text otherwise closes the connection with [CloseInvalidData].
	SkipUTF8Validation bool
}

// WriteOptions configures one [Conn.Write] or [Conn.WriteMessage]. Nil disables
// write-side liveness checks.
//
// A write copies the options when it begins. Its context bounds only writer
// acquisition; these options bound the message body.
type WriteOptions struct {
	// MessageTimeout bounds a message from callback entry. Zero is unbounded.
	// It conflicts with MinWriteRate; [Writer.SetWriteDeadline] overrides it.
	MessageTimeout time.Duration

	// MinWriteRate is the minimum body rate in bytes per second. A frame receives
	// size/rate time, so large frames delay stall detection. Zero disables the
	// rate check. It conflicts with MessageTimeout and [Writer.SetWriteDeadline].
	MinWriteRate int
}

var (
	errKeepaliveTimeoutWithoutPing = fmt.Errorf("%w: ReadOptions: KeepaliveTimeout requires KeepaliveInterval", ErrInvalidArgument)
	errReadOptsConflict            = fmt.Errorf("%w: ReadOptions: MessageTimeout and MinReadRate are mutually exclusive", ErrInvalidArgument)
	errWriteOptsConflict           = fmt.Errorf("%w: WriteOptions: MessageTimeout and MinWriteRate are mutually exclusive", ErrInvalidArgument)
	errReadOptsNegative            = fmt.Errorf("%w: ReadOptions: durations and rates must not be negative", ErrInvalidArgument)
	errWriteOptsNegative           = fmt.Errorf("%w: WriteOptions: durations and rates must not be negative", ErrInvalidArgument)
)

func (o *ReadOptions) validate() error {
	if o == nil {
		return nil
	}
	if o.IdleTimeout < 0 || o.KeepaliveInterval < 0 || o.KeepaliveTimeout < 0 ||
		o.MessageTimeout < 0 || o.MinReadRate < 0 {
		return errReadOptsNegative
	}
	if o.KeepaliveInterval == 0 && o.KeepaliveTimeout != 0 {
		return errKeepaliveTimeoutWithoutPing
	}
	if o.MessageTimeout != 0 && o.MinReadRate != 0 {
		return errReadOptsConflict
	}
	return nil
}

func (o *WriteOptions) validate() error {
	if o == nil {
		return nil
	}
	if o.MessageTimeout < 0 || o.MinWriteRate < 0 {
		return errWriteOptsNegative
	}
	if o.MessageTimeout != 0 && o.MinWriteRate != 0 {
		return errWriteOptsConflict
	}
	return nil
}
