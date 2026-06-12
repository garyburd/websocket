package websocket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// sinkRWC serves r on Read (then io.EOF) and discards everything written, so
// the read path's best-effort close/pong echoes never block. Close is a no-op.
type sinkRWC struct{ r *bytes.Reader }

func (s *sinkRWC) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *sinkRWC) Write(p []byte) (int, error) { return len(p), nil }
func (s *sinkRWC) Close() error                { return nil }

// captureRWC appends everything written to w; Read always reports io.EOF (the
// write side is exercised in isolation). Close is a no-op.
type captureRWC struct{ w *bytes.Buffer }

func (c *captureRWC) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *captureRWC) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c *captureRWC) Close() error                { return nil }

// fuzzConn builds a Conn over rwc with timeouts long enough that the connection's
// timers never fire during a (fast, finite) fuzz iteration.
func fuzzConn(rwc *sinkRWC, isServer bool, bufSize int) *Conn {
	return newConn(rwc, nil, connConfig{
		isServer:            isServer,
		controlReplyTimeout: time.Hour,
		shutdownTimeout:     time.Hour,
		maxMessageSize:      1 << 20,
		writeBufferSize:     bufSize,
	})
}

// FuzzReadInbound feeds arbitrary bytes to the read path and requires it never
// to panic, never to overrun MaxMessageSize, and to terminate. Each input is
// decoded both as a server (masked client frames expected) and as a client
// (unmasked server frames expected) to cover both mask disciplines.
func FuzzReadInbound(f *testing.F) {
	f.Add([]byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'})          // text "hello" (unmasked)
	f.Add([]byte{0x88, 0x02, 0x03, 0xe8})                       // close, code 1000
	f.Add([]byte{0x81, 0x82, 0x00, 0x00, 0x00, 0x00, 'h', 'i'}) // masked text "hi", zero key
	f.Add([]byte{0x82, 0x7e, 0x01, 0x00})                       // binary, 256-byte len header, truncated
	f.Add([]byte{0x01, 0x03, 'a', 'b', 'c', 0x80, 0x02, 'd', 'e'})
	f.Add([]byte{0x88, 0x04, 0x03, 0xe8, 0xff, 0xfe})                         // close, code 1000, invalid UTF-8 reason
	f.Add([]byte{0x88, 0x84, 0x01, 0x02, 0x03, 0x04, 0x02, 0xea, 0xfc, 0xfa}) // same close, masked (server mode)
	f.Add([]byte{0x01, 0x02, 'h', 'i', 0x8a, 0x00, 0x80, 0x01, '!'})          // pong interleaved between text fragments
	f.Add([]byte{0x89, 0x80, 0x01, 0x02, 0x03, 0x04})                         // masked ping, empty payload
	f.Add([]byte{0xc1, 0x00})                                                 // RSV1 set
	f.Add([]byte{0x09, 0x00})                                                 // fragmented (FIN=0) ping
	f.Add([]byte{0x81, 0x7e, 0x00, 0x7d})                                     // 16-bit length 125: non-minimal encoding
	f.Add([]byte{0x81, 0x7f, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // 64-bit length with MSB set

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, isServer := range []bool{false, true} {
			c := fuzzConn(&sinkRWC{r: bytes.NewReader(data)}, isServer, 4096)
			for {
				msg, _, err := c.ReadMessage(nil)
				if len(msg) > 1<<20 {
					t.Fatalf("isServer=%v: message len %d exceeds MaxMessageSize", isServer, len(msg))
				}
				if err != nil {
					break
				}
			}
			// The error must be sticky: once a read fails, every later read
			// must fail too, never hand back a message.
			if _, _, err := c.ReadMessage(nil); err == nil {
				t.Fatalf("isServer=%v: ReadMessage succeeded after a terminal read error", isServer)
			}
			c.Close() // stop the connection's timers; don't leak an AfterFunc per iteration
		}
	})
}

// FuzzRoundTrip writes a message through a client and reads it back through a
// server, requiring the type and payload to survive the encode/decode round
// trip across length-boundary sizes and buffer sizes. A text payload that is
// not valid UTF-8 must instead be rejected on read (RFC 6455 §8.1), so this
// also fuzzes the streaming inbound validator end to end.
func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), false, 4096)
	f.Add([]byte(nil), true, 64)
	f.Add(bytes.Repeat([]byte("x"), 125), false, 128)   // 7-bit length boundary
	f.Add(bytes.Repeat([]byte("y"), 126), true, 130)    // 16-bit length boundary
	f.Add(bytes.Repeat([]byte("z"), 65536), false, 256) // 64-bit length boundary

	f.Fuzz(func(t *testing.T, payload []byte, binary bool, bufSize int) {
		if len(payload) > 1<<20 {
			payload = payload[:1<<20] // within MaxMessageSize so the read isn't rejected
		}
		bufSize &= 0xffff // 0..64 KiB; newConn clamps anything below the minimum up

		mt := MessageText
		if binary {
			mt = MessageBinary
		}

		var buf bytes.Buffer
		client := newConn(&captureRWC{w: &buf}, nil, connConfig{
			controlReplyTimeout: time.Hour,
			shutdownTimeout:     time.Hour,
			maxMessageSize:      1 << 20,
			writeBufferSize:     bufSize,
		})
		if err := client.WriteMessage(context.Background(), mt, nil, payload); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		client.Close()

		server := fuzzConn(&sinkRWC{r: bytes.NewReader(buf.Bytes())}, true, 4096)
		gotMsg, gotType, err := server.ReadMessage(nil)
		server.Close()

		// Inbound text is validated as UTF-8 (RFC 6455 §8.1), incrementally
		// across whatever frame and buffer boundaries this payload/bufSize
		// produced. An invalid text payload must be rejected as a protocol
		// error rather than round-tripped; anything else must round-trip
		// intact. This makes the read-path validator agree with utf8.Valid
		// over fuzzed input.
		if mt == MessageText && !utf8.Valid(payload) {
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid-UTF-8 text: err = %v, want ErrProtocol", err)
			}
			return
		}
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if gotType != mt {
			t.Errorf("type = %v, want %v", gotType, mt)
		}
		if !bytes.Equal(gotMsg, payload) {
			t.Errorf("payload mismatch: got %d bytes, want %d", len(gotMsg), len(payload))
		}
	})
}

// FuzzUpgradeHeaders drives the server handshake with arbitrary values in
// every header Upgrade inspects (including the Origin parse behind the
// default host check and a glob CheckOrigin, and the subprotocol
// split) and requires it never to panic. Every outcome must be an
// ErrBadHandshake-wrapping error: a rejection produces one directly, and a
// handshake good enough to be accepted fails at the hijack, which a
// ResponseRecorder cannot satisfy.
func FuzzUpgradeHeaders(f *testing.F) {
	f.Add("Upgrade", "websocket", "13", "dGhlIHNhbXBsZSBub25jZQ==", "https://example.com", "chat, superchat")
	f.Add("keep-alive, Upgrade", "WebSocket", "8", "", "http://example.com:8080", "chat")
	f.Add("upgrade", "websocket\r\nX-Injected: 1", "13", "key", "://bad\x00origin", "a,,b,")
	f.Add("Upgrade", "websocket", "13", "key", "https://chat.example.com", "superchat") // glob origin match
	f.Add("", "", "", "", "", "")

	check := AllowedOrigins(true, "*.example.com")
	f.Fuzz(func(t *testing.T, connection, upgrade, version, key, origin, protocols string) {
		for _, opts := range []*UpgradeOptions{
			nil, // default host check
			{CheckOrigin: check, SelectSubprotocol: SupportedSubprotocols("chat", "superchat")},
		} {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Connection", connection)
			r.Header.Set("Upgrade", upgrade)
			r.Header.Set("Sec-WebSocket-Version", version)
			r.Header.Set("Sec-WebSocket-Key", key)
			r.Header.Set("Origin", origin)
			r.Header.Set("Sec-WebSocket-Protocol", protocols)

			_, err := Upgrade(httptest.NewRecorder(), r, opts)
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("Upgrade = %v, want an ErrBadHandshake-wrapping error", err)
			}
		}
	})
}

// dialBody is a canned 101 response body. It satisfies io.ReadWriteCloser the
// way a hijacked connection's body does, so Dial's writability check passes.
type dialBody struct{}

func (dialBody) Read([]byte) (int, error)    { return 0, io.EOF }
func (dialBody) Write(p []byte) (int, error) { return len(p), nil }
func (dialBody) Close() error                { return nil }

// dialResponseRT answers every request with a response built from fuzzed
// fields. echoAccept derives Sec-WebSocket-Accept from the request's actual
// challenge: the only way an input can pass Dial's accept check, since the
// challenge is freshly random on every call.
type dialResponseRT struct {
	status                      int
	upgrade, connection, accept string
	extensions, subprotocol     string
	echoAccept, writableBody    bool
}

func (rt *dialResponseRT) RoundTrip(req *http.Request) (*http.Response, error) {
	accept := rt.accept
	if rt.echoAccept {
		accept = computeAcceptKey(req.Header.Get("Sec-WebSocket-Key"))
	}
	h := make(http.Header)
	h.Set("Upgrade", rt.upgrade)
	h.Set("Connection", rt.connection)
	h.Set("Sec-WebSocket-Accept", accept)
	h.Set("Sec-WebSocket-Extensions", rt.extensions)
	h.Set("Sec-WebSocket-Protocol", rt.subprotocol)
	var body = io.NopCloser(strings.NewReader("rejected"))
	if rt.writableBody {
		body = dialBody{}
	}
	return &http.Response{StatusCode: rt.status, Header: h, Body: body, Request: req}, nil
}

// FuzzDialResponse drives the client half of the handshake (Dial vetting the
// server's response status and headers) with arbitrary values in every field
// Dial inspects. It must never panic, never return both a Conn and an error,
// only succeed on an accept key derived from the request's challenge, and
// never negotiate a subprotocol that was not offered.
func FuzzDialResponse(f *testing.F) {
	f.Add(101, "websocket", "Upgrade", "", true, "", "chat", "chat", true)
	f.Add(101, "WebSocket", "keep-alive, upgrade", "", true, "", "", "", true)
	f.Add(101, "websocket", "Upgrade", "", true, "permessage-deflate", "", "", true)
	f.Add(101, "websocket", "Upgrade", "dGhlIHNhbXBsZSBub25jZQ==", false, "", "", "", true)
	f.Add(101, "websocket", "Upgrade", "", true, "", "superchat", "chat", true) // selected an unoffered subprotocol
	f.Add(101, "websocket", "Upgrade", "", true, "", "", "", false)             // good handshake, unwritable body
	f.Add(403, "", "", "", false, "", "", "", false)

	f.Fuzz(func(t *testing.T, status int, upgrade, connection, accept string, echoAccept bool, extensions, subprotocol, offered string, writableBody bool) {
		rt := &dialResponseRT{
			status: status, upgrade: upgrade, connection: connection,
			accept: accept, extensions: extensions, subprotocol: subprotocol,
			echoAccept: echoAccept, writableBody: writableBody,
		}
		opts := &DialOptions{
			Transport:         rt,
			ResponseBodyLimit: 64,
		}
		if offered != "" {
			opts.Subprotocols = []string{offered}
		}

		c, _, err := Dial(context.Background(), "ws://example.com/", opts, nil)
		if err != nil {
			if c != nil {
				t.Fatal("Dial returned both a Conn and an error")
			}
			return
		}
		defer c.Close()
		if !echoAccept {
			t.Fatal("Dial accepted an accept key not derived from the challenge")
		}
		if sp := c.Subprotocol(); sp != "" && sp != offered {
			t.Fatalf("negotiated subprotocol %q, offered only %q", sp, offered)
		}
	})
}

// FuzzClosePayload checks the close-frame payload helpers: truncateUTF8 must
// return a UTF-8-validity-preserving prefix within the limit, buildClosePayload
// must fit the control-frame cap, and a payload built from a valid wire code
// and reason must round-trip through parseClosePayload while anything else is
// rejected.
func FuzzClosePayload(f *testing.F) {
	f.Add(uint16(1000), "normal closure")
	f.Add(uint16(1015), "")                       // receive-only code: invalid on the wire
	f.Add(uint16(3000), strings.Repeat("é", 100)) // multi-byte rune spanning the truncation cut
	f.Add(uint16(65535), "\xff\xfe")              // out-of-range code, invalid UTF-8 reason

	f.Fuzz(func(t *testing.T, code uint16, reason string) {
		trunc := truncateUTF8(reason, maxControlPayload-2)
		if len(trunc) > maxControlPayload-2 {
			t.Fatalf("truncateUTF8 returned %d bytes, limit %d", len(trunc), maxControlPayload-2)
		}
		if !strings.HasPrefix(reason, trunc) {
			t.Fatalf("truncateUTF8 result %q is not a prefix of %q", trunc, reason)
		}
		if utf8.ValidString(reason) && !utf8.ValidString(trunc) {
			t.Fatalf("truncateUTF8 broke valid UTF-8: %q", trunc)
		}

		p := buildClosePayload(CloseCode(code), reason)
		if len(p) > maxControlPayload {
			t.Fatalf("close payload is %d bytes, limit %d", len(p), maxControlPayload)
		}
		gotCode, gotReason, err := parseClosePayload(p)
		switch {
		case !validWireCloseCode(CloseCode(code)) || !utf8.ValidString(trunc):
			if err == nil {
				t.Fatalf("parseClosePayload accepted code %d, reason %q", code, trunc)
			}
		case err != nil:
			t.Fatalf("parseClosePayload: %v", err)
		case gotCode != CloseCode(code) || gotReason != trunc:
			t.Fatalf("round trip = (%d, %q), want (%d, %q)", gotCode, gotReason, code, trunc)
		}
	})
}

// FuzzMaskCopy checks maskCopy (out-of-place and in-place) against the
// naive per-byte reference (refMask) for arbitrary payloads, keys, and
// starting positions.
func FuzzMaskCopy(f *testing.F) {
	f.Add([]byte("hello, world!"), byte(0x37), byte(0xfa), byte(0x21), byte(0x3d), 0)
	f.Add(bytes.Repeat([]byte("0123456789"), 13), byte(1), byte(2), byte(3), byte(4), 2)
	f.Add([]byte(nil), byte(0), byte(0), byte(0), byte(0), 0)

	f.Fuzz(func(t *testing.T, b []byte, k0, k1, k2, k3 byte, pos int) {
		key := [4]byte{k0, k1, k2, k3}
		want := append([]byte(nil), b...)
		wantPos := refMask(want, key, pos)

		dst := make([]byte, len(b))
		if got := maskCopy(dst, b, key, pos); got != wantPos {
			t.Fatalf("position = %d, want %d", got, wantPos)
		}
		if !bytes.Equal(dst, want) {
			t.Fatal("out-of-place maskCopy disagrees with reference")
		}

		work := append([]byte(nil), b...)
		maskCopy(work, work, key, pos)
		if !bytes.Equal(work, want) {
			t.Fatal("in-place maskCopy disagrees with reference")
		}
	})
}

// FuzzUTF8Validate fuzzes the UTF-8 DFA directly against the stdlib oracle:
// whole-buffer validity must match utf8.Valid, and feeding the bytes in two
// pieces (split at every offset) must reach the same final state as feeding
// them whole, the carry-across-chunks property the streaming read path relies
// on. The split loop is O(n^2), so it is capped; the oracle check always runs.
func FuzzUTF8Validate(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte("héllo, €uro 😀"))
	f.Add([]byte{0xff})
	f.Add([]byte{0xc3, 0x28})             // bad continuation
	f.Add([]byte{0xe0, 0x80, 0xaf})       // overlong
	f.Add([]byte{0xed, 0xa0, 0x80})       // surrogate
	f.Add([]byte{0xf4, 0x90, 0x80, 0x80}) // > U+10FFFF
	f.Add([]byte{0xc3})                   // truncated lead byte

	f.Fuzz(func(t *testing.T, data []byte) {
		whole := utf8Validate(utf8Accept, data)
		if got, want := whole == utf8Accept, utf8.Valid(data); got != want {
			t.Fatalf("accept = %v, utf8.Valid = %v for %x", got, want, data)
		}
		if len(data) > 1024 {
			return // bound the O(n^2) split check on large inputs
		}
		for i := 0; i <= len(data); i++ {
			s := utf8Validate(utf8Accept, data[:i])
			if s = utf8Validate(s, data[i:]); s != whole {
				t.Fatalf("split at %d gave state %d, whole = %d for %x", i, s, whole, data)
			}
		}
	})
}
