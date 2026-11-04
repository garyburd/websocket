package websocket

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf8"
)

// sinkRWC serves r on Read (then io.EOF) and discards everything written, so
// the read path's best-effort close/pong echoes never block. Close is a no-op.
type sinkRWC struct{ r io.Reader }

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

// inboundRWC serves r and records everything the Conn writes.
type inboundRWC struct {
	r io.Reader
	w bytes.Buffer
}

func (c *inboundRWC) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *inboundRWC) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c *inboundRWC) Close() error                { return nil }

// inboundMessage is one data message.
type inboundMessage struct {
	mt      MessageType
	payload []byte
}

// inboundReference is what a conforming reader does with a frame sequence,
// up to its first violation that does not depend on read options or
// message content.
type inboundReference struct {
	messages   []inboundMessage // complete messages, in order
	pings      [][]byte         // ping payloads, in order
	peerClose  []byte           // the close frame's payload; nil if none
	closeFound bool
}

func referenceInbound(frames []wireFrame) inboundReference {
	var ref inboundReference
	var cur *inboundMessage
	for _, f := range frames {
		if f.opcode&8 != 0 {
			if !f.fin || len(f.payload) > maxControlPayload {
				return ref
			}
			switch f.opcode {
			case opPing:
				ref.pings = append(ref.pings, f.payload)
			case opPong:
			case opClose:
				ref.peerClose, ref.closeFound = f.payload, true
				return ref
			default:
				return ref
			}
			continue
		}
		switch {
		case cur == nil && (f.opcode == opText || f.opcode == opBinary):
			mt := MessageBinary
			if f.opcode == opText {
				mt = MessageText
			}
			cur = &inboundMessage{mt: mt, payload: []byte{}}
		case cur != nil && f.opcode == opContinuation:
		default:
			return ref
		}
		cur.payload = append(cur.payload, f.payload...)
		if f.fin {
			ref.messages = append(ref.messages, *cur)
			cur = nil
		}
	}
	return ref
}

// FuzzReadInbound feeds arbitrary bytes to the read path, as a server
// (masked client frames expected) and as a client (unmasked server frames
// expected), under several read options. An independent decoder of the
// input says what a conforming reader does, and the test checks the
// messages returned, the frames the Conn writes back, and the terminal
// error against it:
//
//   - the messages are a prefix of the input's complete messages, and all
//     of them when the input simply runs out;
//   - only well-formed control frames are written: pongs echo the input's
//     pings in order, and at most one close, last;
//   - a protocol error sends a close with its code, a peer close is echoed
//     with its payload, and running out of input sends no close;
//   - the error is sticky.
func FuzzReadInbound(f *testing.F) {
	f.Add([]byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'})          // text "hello" (unmasked)
	f.Add([]byte{0x88, 0x02, 0x03, 0xe8})                       // close, code 1000
	f.Add([]byte{0x81, 0x82, 0x00, 0x00, 0x00, 0x00, 'h', 'i'}) // masked text "hi", zero key
	f.Add([]byte{0x82, 0x7e, 0x01, 0x00})                       // binary, 256-byte len header, truncated
	f.Add([]byte{0x01, 0x03, 'a', 'b', 'c', 0x80, 0x02, 'd', 'e'})
	f.Add([]byte{0x88, 0x04, 0x03, 0xe8, 0xff, 0xfe})                          // close, code 1000, invalid UTF-8 reason
	f.Add([]byte{0x88, 0x84, 0x01, 0x02, 0x03, 0x04, 0x02, 0xea, 0xfc, 0xfa})  // same close, masked (server mode)
	f.Add([]byte{0x01, 0x02, 'h', 'i', 0x8a, 0x00, 0x80, 0x01, '!'})           // pong interleaved between text fragments
	f.Add([]byte{0x89, 0x80, 0x01, 0x02, 0x03, 0x04})                          // masked ping, empty payload
	f.Add([]byte{0xc1, 0x00})                                                  // RSV1 set
	f.Add([]byte{0x09, 0x00})                                                  // fragmented (FIN=0) ping
	f.Add([]byte{0x81, 0x7e, 0x00, 0x7d})                                      // 16-bit length 125: non-minimal encoding
	f.Add([]byte{0x81, 0x7f, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})  // 64-bit length with MSB set
	f.Add([]byte{0x89, 0x02, 'p', '1', 0x81, 0x01, 'a', 0x89, 0x02, 'p', '2'}) // pings around a message
	f.Add([]byte{0x81, 0x02, 0xc3, 0x28})                                      // invalid UTF-8 text
	f.Add(append([]byte{0x82, 0x7e, 0x00, 0x80}, make([]byte, 128)...))        // 128-byte binary, over the 64-byte limit

	variants := []struct {
		name       string
		opts       *ReadOptions
		maxMsg     int
		mayTimeout bool // MinReadRate's watchdog may fire if the fuzzer stalls
	}{
		{"default", nil, 1 << 20, false},
		{"skipUTF8", &ReadOptions{SkipUTF8Validation: true, MatchPong: true, MinReadRate: 1}, 1 << 20, true},
		{"limit64", nil, 64, false},
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, isServer := range []bool{false, true} {
			in, _ := parseWire(data, isServer)
			ref := referenceInbound(in)
			for _, v := range variants {
				rwc := &inboundRWC{r: bytes.NewReader(data)}
				c := newConn(rwc, nil, connConfig{
					isServer:            isServer,
					controlReplyTimeout: time.Hour,
					shutdownTimeout:     time.Hour,
					maxMessageSize:      v.maxMsg,
					writeBufferSize:     4096,
				})
				var got []inboundMessage
				var err error
				for {
					var msg []byte
					var mt MessageType
					if msg, mt, err = c.ReadMessage(v.opts); err != nil {
						break
					}
					got = append(got, inboundMessage{mt, msg})
				}
				if _, _, err2 := c.ReadMessage(v.opts); err2 == nil {
					t.Fatalf("isServer=%v %s: ReadMessage succeeded after a terminal read error", isServer, v.name)
				}
				c.Close() // stop the connection's timers; don't leak an AfterFunc per iteration
				if cerr := checkInbound(ref, got, rwc.w.Bytes(), err, isServer, v.opts, v.maxMsg, v.mayTimeout); cerr != nil {
					t.Fatalf("isServer=%v %s: %v (read error: %v)", isServer, v.name, cerr, err)
				}
			}
		}
	})
}

// checkInbound checks one FuzzReadInbound run against the reference.
func checkInbound(ref inboundReference, got []inboundMessage, out []byte, readErr error, isServer bool, opts *ReadOptions, maxMsg int, mayTimeout bool) error {
	validate := opts == nil || !opts.SkipUTF8Validation
	if len(got) > len(ref.messages) {
		return fmt.Errorf("returned %d messages, input has %d", len(got), len(ref.messages))
	}
	for i, m := range got {
		want := ref.messages[i]
		switch {
		case m.mt != want.mt || !bytes.Equal(m.payload, want.payload):
			return fmt.Errorf("message %d = %v %q, want %v %q", i, m.mt, m.payload, want.mt, want.payload)
		case len(m.payload) > maxMsg:
			return fmt.Errorf("message %d is %d bytes, over MaxMessageSize %d", i, len(m.payload), maxMsg)
		case validate && m.mt == MessageText && !utf8.Valid(m.payload):
			return fmt.Errorf("message %d is invalid UTF-8 text", i)
		}
	}

	// The Conn masks iff it is a client.
	frames, err := parseWire(out, !isServer)
	if err != nil {
		return fmt.Errorf("written frames: %v", err)
	}
	var pongs [][]byte
	var sentClose []byte
	for i, f := range frames {
		switch {
		case !f.fin || len(f.payload) > maxControlPayload:
			return fmt.Errorf("wrote invalid control frame %+v", f)
		case f.opcode == opPong && sentClose == nil:
			pongs = append(pongs, f.payload)
		case f.opcode == opClose && i == len(frames)-1:
			sentClose = f.payload
		default:
			return fmt.Errorf("wrote unexpected frame %d of %d: opcode %d", i, len(frames), f.opcode)
		}
	}
	if len(pongs) > len(ref.pings) {
		return fmt.Errorf("sent %d pongs for %d pings", len(pongs), len(ref.pings))
	}
	for i, p := range pongs {
		if !bytes.Equal(p, ref.pings[i]) {
			return fmt.Errorf("pong %d = %q, want ping payload %q", i, p, ref.pings[i])
		}
	}

	// complete requires everything before the end of input was handled.
	complete := func() error {
		if len(got) != len(ref.messages) || len(pongs) != len(ref.pings) {
			return fmt.Errorf("handled %d of %d messages and %d of %d pings", len(got), len(ref.messages), len(pongs), len(ref.pings))
		}
		return nil
	}
	var pe *protocolError
	var ce *CloseError
	switch {
	case errors.As(readErr, &pe):
		if len(sentClose) < 2 || CloseCode(sentClose[0])<<8|CloseCode(sentClose[1]) != pe.code {
			return fmt.Errorf("protocol error %d, sent close %q", pe.code, sentClose)
		}
	case errors.As(readErr, &ce) && ce.Code == CloseAbnormalClosure, errors.Is(readErr, io.ErrUnexpectedEOF):
		if sentClose != nil {
			return fmt.Errorf("input ran out, sent close %q", sentClose)
		}
		return complete()
	case errors.As(readErr, &ce):
		if !ref.closeFound || !bytes.Equal(sentClose, ref.peerClose) {
			return fmt.Errorf("peer close %q, sent close %q", ref.peerClose, sentClose)
		}
		return complete()
	case mayTimeout && errors.Is(readErr, ErrTimeout):
	default:
		return fmt.Errorf("unexpected read error type %T", readErr)
	}
	return nil
}

// chunkReader limits each Read to the next size in plan, 1 to 256 bytes,
// cycling through it, so the reader sees the short reads a slow transport
// produces. An empty plan does not limit reads.
type chunkReader struct {
	r    io.Reader
	plan []byte
	i    int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.plan) > 0 && len(p) > 0 {
		p = p[:min(1+int(c.plan[c.i%len(c.plan)]), len(p))]
		c.i++
	}
	return c.r.Read(p)
}

// wireFrame is one frame decoded by parseWire.
type wireFrame struct {
	fin     bool
	opcode  int
	payload []byte
}

// parseWire decodes the frames in b independently of the package's parser.
// masked requires client masking and removes it; otherwise frames must be
// unmasked. On error it also returns the frames decoded before it.
func parseWire(b []byte, masked bool) ([]wireFrame, error) {
	var frames []wireFrame
	for len(b) > 0 {
		if len(b) < 2 {
			return frames, errors.New("truncated header")
		}
		f := wireFrame{fin: b[0]&0x80 != 0, opcode: int(b[0] & 0x0f)}
		if b[0]&0x70 != 0 {
			return frames, errors.New("RSV bits set")
		}
		if got := b[1]&0x80 != 0; got != masked {
			return frames, fmt.Errorf("mask bit %v, want %v", got, masked)
		}
		n, rest := uint64(b[1]&0x7f), b[2:]
		switch n {
		case 126:
			if len(rest) < 2 {
				return frames, errors.New("truncated 16-bit length")
			}
			if n = uint64(rest[0])<<8 | uint64(rest[1]); n < 126 {
				return frames, errors.New("non-minimal 16-bit length")
			}
			rest = rest[2:]
		case 127:
			if len(rest) < 8 {
				return frames, errors.New("truncated 64-bit length")
			}
			n = 0
			for _, c := range rest[:8] {
				n = n<<8 | uint64(c)
			}
			if n <= 0xffff || n>>63 != 0 {
				return frames, errors.New("invalid 64-bit length")
			}
			rest = rest[8:]
		}
		var key []byte
		if masked {
			if len(rest) < 4 {
				return frames, errors.New("truncated mask key")
			}
			key, rest = rest[:4], rest[4:]
		}
		if uint64(len(rest)) < n {
			return frames, errors.New("truncated payload")
		}
		f.payload = bytes.Clone(rest[:n])
		for i := range key {
			for j := i; j < len(f.payload); j += 4 {
				f.payload[j] ^= key[i]
			}
		}
		frames = append(frames, f)
		b = rest[n:]
	}
	return frames, nil
}

// messageFromFrames checks that frames form exactly one message of the
// given opcode and returns its payload: a data opcode first, continuations
// after it, FIN on the last frame only, and no empty frame before the last.
func messageFromFrames(frames []wireFrame, opcode int) ([]byte, error) {
	if len(frames) == 0 {
		return nil, errors.New("no frames")
	}
	var payload []byte
	for i, f := range frames {
		want := opContinuation
		if i == 0 {
			want = opcode
		}
		last := i == len(frames)-1
		switch {
		case f.opcode != want:
			return nil, fmt.Errorf("frame %d: opcode %d, want %d", i, f.opcode, want)
		case f.fin != last:
			return nil, fmt.Errorf("frame %d of %d: fin %v", i, len(frames), f.fin)
		case !last && len(f.payload) == 0:
			return nil, fmt.Errorf("frame %d of %d: empty non-final frame", i, len(frames))
		}
		payload = append(payload, f.payload...)
	}
	return payload, nil
}

// writePlanned writes payload as one message, following plan two bytes per
// call: the first byte picks Write, WriteString, ReadFrom (from a source
// that returns half of each request), or Final followed by one last write
// of the rest; the second byte's square is the call's size. Once plan runs
// out, one Write sends the rest.
func writePlanned(w *Writer, payload, plan []byte) error {
	for i := 0; len(payload) > 0; i += 2 {
		if i+1 >= len(plan) {
			if n, err := w.Write(payload); err != nil || n != len(payload) {
				return fmt.Errorf("Write(%d bytes) = %d, %v", len(payload), n, err)
			}
			return nil
		}
		op, size := plan[i]%4, int(plan[i+1])*int(plan[i+1])
		if op == 3 {
			w.Final()
			size = len(payload)
		}
		chunk := payload[:min(size, len(payload))]
		payload = payload[len(chunk):]
		var n int64
		var err error
		switch {
		case op == 0 || op == 3 && plan[i+1]&1 == 0:
			var k int
			k, err = w.Write(chunk)
			n = int64(k)
		case op == 1 || op == 3:
			var k int
			k, err = w.WriteString(string(chunk))
			n = int64(k)
		default:
			n, err = w.ReadFrom(iotest.HalfReader(bytes.NewReader(chunk)))
		}
		if err != nil || n != int64(len(chunk)) {
			return fmt.Errorf("op %d (%d bytes) = %d, %v", op, len(chunk), n, err)
		}
		if op == 3 {
			return nil
		}
	}
	return nil
}

// readPlanned reads one message. Without stream it uses ReadMessage.
// Otherwise it reads the payload with Reader.Read, sizing each buffer
// (1 to 256 bytes) from plan, and a plan byte divisible by 8 reads the rest
// with WriteTo instead.
func readPlanned(c *Conn, stream bool, plan []byte) ([]byte, MessageType, error) {
	if !stream {
		return c.ReadMessage(nil)
	}
	var got []byte
	var mt MessageType
	err := c.Read(nil, func(r *Reader) error {
		mt = r.Type()
		for i := 0; ; i++ {
			size := 512
			if len(plan) > 0 {
				b := plan[len(plan)-1-i%len(plan)]
				if b%8 == 0 {
					var buf bytes.Buffer
					_, err := r.WriteTo(&buf)
					got = append(got, buf.Bytes()...)
					return err
				}
				size = 1 + int(b)
			}
			buf := make([]byte, size)
			n, err := r.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	})
	return got, mt, err
}

// FuzzRoundTrip writes a message through one endpoint and reads it back
// through the other, requiring the type and payload to survive. The input
// picks the direction, the writer's buffer size and its sequence of Write,
// WriteString, ReadFrom, and Final calls, how many bytes each transport read
// returns, and whether the reader uses ReadMessage or streams with Read and
// WriteTo. An independent decoder checks the frames the writer produced. A
// text payload that is not valid UTF-8 must instead be rejected on read
// (RFC 6455 §8.1), which fuzzes the streaming validator across frame,
// buffer, and transport-read boundaries.
func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), false, false, false, 4096, []byte(nil), []byte(nil))
	f.Add([]byte(nil), true, true, true, 64, []byte(nil), []byte{1})
	f.Add(bytes.Repeat([]byte("x"), 125), false, false, true, 128, []byte{0, 5, 1, 7, 2, 9}, []byte{0, 3})                         // 7-bit length boundary
	f.Add(bytes.Repeat([]byte("y"), 126), true, true, false, 130, []byte{2, 11, 3, 1}, []byte{200})                                // 16-bit length boundary
	f.Add(bytes.Repeat([]byte("z"), 65536), false, true, true, 256, []byte{1, 255, 0, 100}, []byte{255, 16})                       // 64-bit length boundary
	f.Add([]byte("h\xc3\xa9llo \xe2\x82\xac \xf0\x9f\x98\x80"), false, false, true, 64, []byte{0, 1, 1, 2, 2, 3}, []byte{0, 1, 2}) // UTF-8 split everywhere
	f.Add([]byte("abc\xf4\x90\x80\x80def"), false, true, true, 64, []byte{0, 2}, []byte{0, 1})                                     // invalid UTF-8 (Autobahn 6.4.3)

	f.Fuzz(func(t *testing.T, payload []byte, isBinary, fromServer, stream bool, bufSize int, writePlan, readPlan []byte) {
		if len(payload) > 1<<17 {
			payload = payload[:1<<17] // bound the cost of byte-at-a-time plans
		}
		bufSize &= 0xffff // 0..64 KiB; newConn clamps anything below the minimum up

		mt, opcode := MessageText, opText
		if isBinary {
			mt, opcode = MessageBinary, opBinary
		}

		var wire bytes.Buffer
		writer := newConn(&captureRWC{w: &wire}, nil, connConfig{
			isServer:            fromServer,
			controlReplyTimeout: time.Hour,
			shutdownTimeout:     time.Hour,
			maxMessageSize:      1 << 20,
			writeBufferSize:     bufSize,
		})
		err := writer.Write(context.Background(), mt, nil, func(w *Writer) error {
			return writePlanned(w, payload, writePlan)
		})
		writer.Close()
		if err != nil {
			t.Fatalf("Write: %v", err)
		}

		frames, err := parseWire(wire.Bytes(), !fromServer)
		if err != nil {
			t.Fatalf("parseWire: %v", err)
		}
		sent, err := messageFromFrames(frames, opcode)
		if err != nil {
			t.Fatalf("frames: %v", err)
		}
		if !bytes.Equal(sent, payload) {
			t.Fatalf("frames carry %d bytes, want the %d-byte payload", len(sent), len(payload))
		}

		reader := fuzzConn(&sinkRWC{r: &chunkReader{r: bytes.NewReader(wire.Bytes()), plan: readPlan}}, !fromServer, 4096)
		got, gotType, err := readPlanned(reader, stream, readPlan)
		reader.Close()

		if mt == MessageText && !utf8.Valid(payload) {
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid-UTF-8 text: err = %v, want ErrProtocol", err)
			}
			return
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if gotType != mt {
			t.Errorf("type = %v, want %v", gotType, mt)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
		}
	})
}

// upgradeConn is the hijacked connection in FuzzUpgradeHeaders. It records
// what Upgrade writes and reports EOF on read. Upgrade uses only Read,
// Write, and Close, so the embedded net.Conn is nil.
type upgradeConn struct {
	net.Conn
	w bytes.Buffer
}

func (c *upgradeConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *upgradeConn) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c *upgradeConn) Close() error                { return nil }

// owsTokens returns the elements of comma-separated header values, with
// optional whitespace (RFC 7230 §3.2.3: space and tab) trimmed.
func owsTokens(values []string) []string {
	var tokens []string
	for _, v := range values {
		for _, t := range strings.Split(v, ",") {
			tokens = append(tokens, strings.Trim(t, " \t"))
		}
	}
	return tokens
}

func hasTokenFold(values []string, token string) bool {
	return slices.ContainsFunc(owsTokens(values), func(t string) bool { return strings.EqualFold(t, token) })
}

// wantUpgrade states, independently of Upgrade, the status for rejecting r,
// or 0 to accept it. glob selects the AllowedOrigins(true, "*.example.com")
// check instead of the default same-host check.
func wantUpgrade(r *http.Request, glob bool) int {
	switch {
	case !hasTokenFold(r.Header.Values("Connection"), "upgrade"),
		!hasTokenFold(r.Header.Values("Upgrade"), "websocket"),
		r.Header.Get("Sec-WebSocket-Version") != "13",
		r.Header.Get("Sec-WebSocket-Key") == "":
		return http.StatusBadRequest
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return 0
	}
	u, err := url.Parse(origin)
	switch {
	case err != nil || u.Host == "":
		return http.StatusForbidden
	case strings.EqualFold(u.Host, r.Host):
		return 0
	case glob:
		if ok, _ := path.Match("*.example.com", strings.ToLower(u.Host)); ok {
			return 0
		}
	}
	return http.StatusForbidden
}

// FuzzUpgradeHeaders drives the server handshake with arbitrary values in
// every header Upgrade inspects, with the default host check and with a
// glob CheckOrigin and subprotocol selection. An independent statement of
// the rules says whether to accept. A rejection must return ErrBadHandshake
// with the expected status. An acceptance must write a 101 response that
// net/http parses, with only the handshake headers, the RFC 6455 accept key
// for the request's key, and the client's first supported subprotocol.
func FuzzUpgradeHeaders(f *testing.F) {
	f.Add("Upgrade", "websocket", "13", "dGhlIHNhbXBsZSBub25jZQ==", "https://example.com", "chat, superchat")
	f.Add("keep-alive, Upgrade", "WebSocket", "8", "", "http://example.com:8080", "chat")
	f.Add("upgrade", "websocket\r\nX-Injected: 1", "13", "key", "://bad\x00origin", "a,,b,")
	f.Add("Upgrade", "websocket", "13", "key", "https://chat.example.com", "superchat") // glob origin match
	f.Add("", "", "", "", "", "")
	f.Add("Upgrade", "websocket", "13", "key\r\nX-Injected: 1", "", "x, superchat,chat") // key reflected only as a hash
	f.Add("\rUpgrAde", "weBsoCket", "13", "0", "", "0")                                  // CR is not optional whitespace
	f.Add("upgrade\u00a0", "websocket", "13", "key", "", "\tchat\u00a0, superchat\t")    // nor is NBSP; tab is

	check := AllowedOrigins(true, "*.example.com")
	f.Fuzz(func(t *testing.T, connection, upgrade, version, key, origin, protocols string) {
		for _, glob := range []bool{false, true} {
			var opts *UpgradeOptions // default host check
			if glob {
				opts = &UpgradeOptions{CheckOrigin: check, SelectSubprotocol: SupportedSubprotocols("chat", "superchat")}
			}
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Connection", connection)
			r.Header.Set("Upgrade", upgrade)
			r.Header.Set("Sec-WebSocket-Version", version)
			r.Header.Set("Sec-WebSocket-Key", key)
			r.Header.Set("Origin", origin)
			r.Header.Set("Sec-WebSocket-Protocol", protocols)

			rec, conn := httptest.NewRecorder(), &upgradeConn{}
			c, err := Upgrade(&hijackRecorder{rec, conn}, r, opts)
			if status := wantUpgrade(r, glob); status != 0 {
				if c != nil {
					c.Close()
					t.Fatalf("glob=%v: Upgrade accepted, want status %d", glob, status)
				}
				if !errors.Is(err, ErrBadHandshake) || rec.Code != status {
					t.Fatalf("glob=%v: Upgrade = %v with status %d, want ErrBadHandshake with %d", glob, err, rec.Code, status)
				}
				continue
			}
			if err != nil {
				t.Fatalf("glob=%v: Upgrade = %v, want success", glob, err)
			}
			c.Close()

			raw := conn.w.Bytes()
			resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), r)
			if err != nil {
				t.Fatalf("glob=%v: parse 101 response %q: %v", glob, raw, err)
			}
			sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			var wantProto string
			if glob {
				for _, p := range owsTokens(r.Header.Values("Sec-WebSocket-Protocol")) {
					if p == "chat" || p == "superchat" {
						wantProto = p
						break
					}
				}
			}
			switch {
			case resp.StatusCode != http.StatusSwitchingProtocols:
				t.Fatalf("glob=%v: status %d, want 101", glob, resp.StatusCode)
			case resp.Header.Get("Upgrade") != "websocket" || resp.Header.Get("Connection") != "Upgrade":
				t.Fatalf("glob=%v: Upgrade %q, Connection %q", glob, resp.Header.Get("Upgrade"), resp.Header.Get("Connection"))
			case resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]):
				t.Fatalf("glob=%v: Sec-WebSocket-Accept %q is wrong for key %q", glob, resp.Header.Get("Sec-WebSocket-Accept"), key)
			case resp.Header.Get("Sec-WebSocket-Protocol") != wantProto:
				t.Fatalf("glob=%v: subprotocol %q, want %q for offer %q", glob, resp.Header.Get("Sec-WebSocket-Protocol"), wantProto, protocols)
			}
			for k := range resp.Header {
				switch k {
				case "Upgrade", "Connection", "Sec-Websocket-Accept", "Sec-Websocket-Protocol":
				default:
					t.Fatalf("glob=%v: unexpected response header %q in %q", glob, k, raw)
				}
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

		c, resp, err := Dial(context.Background(), "ws://example.com/", opts, nil)
		if err != nil {
			if c != nil {
				t.Fatal("Dial returned both a Conn and an error")
			}
			if resp == nil {
				return // rejected before the request, e.g. an invalid offer
			}
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("Dial err = %v, want ErrBadHandshake", err)
			}
			if got, want := errors.Is(err, ErrBadStatus), status != 101; got != want {
				t.Fatalf("status %d: errors.Is(%v, ErrBadStatus) = %v, want %v", status, err, got, want)
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
// rejected. closePayload, which Shutdown and Abort use, must turn any
// arguments into a close that round-trips, keep valid arguments, and report
// exactly the replaced ones, never as ErrInvalidArgument.
func FuzzClosePayload(f *testing.F) {
	f.Add(uint16(1000), "normal closure")
	f.Add(uint16(1015), "")                       // receive-only code: invalid on the wire
	f.Add(uint16(3000), strings.Repeat("é", 100)) // multi-byte rune spanning the truncation cut
	f.Add(uint16(65535), "\xff\xfe")              // out-of-range code, invalid UTF-8 reason
	f.Add(uint16(1005), "bye")                    // sent as a close without status; reason dropped

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

		// The uint16 input cannot express codes beyond the wire format.
		for _, in := range []CloseCode{CloseCode(code), CloseCode(code) + 1<<16} {
			wire, err := closePayload(in, reason)
			if err != nil && (!errors.Is(err, ErrCloseArgsReplaced) || errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "sent")) {
				t.Fatalf("closePayload(%d, %q) err = %v, want only ErrCloseArgsReplaced saying what was sent", in, reason, err)
			}
			if in == CloseNoStatusReceived {
				if len(wire) != 0 || (err != nil) != (reason != "") || (err != nil && !errors.Is(err, errCloseReasonDropped)) {
					t.Fatalf("closePayload(%d, %q) = (%q, %v), want an empty payload", in, reason, wire, err)
				}
				continue
			}
			c, r, perr := parseClosePayload(wire)
			validCode, validReason := validWireCloseCode(in), utf8.ValidString(reason)
			switch {
			case perr != nil:
				t.Fatalf("closePayload(%d, %q) sends %q, which does not parse: %v", in, reason, wire, perr)
			case validCode && c != in, !validCode && c != CloseInternalError:
				t.Fatalf("closePayload(%d, %q) code = %d", in, reason, c)
			case validReason && r != trunc:
				t.Fatalf("closePayload(%d, %q) reason = %q", in, reason, r)
			case errors.Is(err, errInvalidCloseCode) == validCode, errors.Is(err, errInvalidCloseReason) == validReason:
				t.Fatalf("closePayload(%d, %q) err = %v", in, reason, err)
			}
		}
	})
}

// FuzzMaskCopy checks maskCopy (out-of-place and in-place) against the
// naive per-byte reference (refMask) for arbitrary payloads, keys, and
// starting positions.
//
// The SIMD implementation is built only with GOEXPERIMENT=simd; fuzz both
// builds. It handles buffers of 64 bytes or more, so the seeds straddle 64
// and the multiples of the vector widths above it.
func FuzzMaskCopy(f *testing.F) {
	f.Add([]byte("hello, world!"), byte(0x37), byte(0xfa), byte(0x21), byte(0x3d), 0)
	f.Add(bytes.Repeat([]byte("0123456789"), 13), byte(1), byte(2), byte(3), byte(4), 2)
	f.Add([]byte(nil), byte(0), byte(0), byte(0), byte(0), 0)
	for i, n := range []int{63, 64, 65, 72, 79, 80, 95, 96, 127, 128, 129, 200} {
		b := make([]byte, n)
		for j := range b {
			b[j] = byte(j)
		}
		f.Add(b, byte(0x37), byte(0xfa), byte(0x21), byte(0x3d), i%4)
	}

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
