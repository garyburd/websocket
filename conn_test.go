package websocket

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"
)

// newTestConn builds a *Conn over rwc with conservative defaults (specific
// tests pass overrides via the variadic option) and closes it at cleanup, for
// tests that supply their own transport.
func newTestConn(t *testing.T, rwc io.ReadWriteCloser, opts ...func(*connConfig)) *Conn {
	t.Helper()
	cfg := connConfig{
		controlReplyTimeout: 10 * time.Second,
		shutdownTimeout:     testShutdownTimeout,
		maxMessageSize:      1 << 20,
		writeBufferSize:     4096,
	}
	for _, o := range opts {
		o(&cfg)
	}
	c := newConn(rwc, nil, cfg)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// connPair returns a *Conn wired up to a peer-side net.Conn the test uses
// to simulate the server.
func connPair(t *testing.T, opts ...func(*connConfig)) (*Conn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	c := newTestConn(t, a, opts...)
	// Registered after newTestConn's cleanup so it runs first: closing the
	// peer side makes any in-flight or trailing rwc.Write inside c.Close
	// fail fast instead of blocking until the write timer fires.
	t.Cleanup(func() { _ = b.Close() })
	return c, b
}

// testShutdownTimeout is the connection's ShutdownTimeout in tests where the
// handshake completes and the bound is not meant to fire.
const testShutdownTimeout = 10 * time.Second

func withServer() func(*connConfig) {
	return func(c *connConfig) { c.isServer = true }
}

func withWriteTimeout(d time.Duration) func(*connConfig) {
	return func(c *connConfig) { c.controlReplyTimeout = d }
}

func withShutdownTimeout(d time.Duration) func(*connConfig) {
	return func(c *connConfig) { c.shutdownTimeout = d }
}

func withMaxMsg(n int) func(*connConfig) {
	return func(c *connConfig) { c.maxMessageSize = n }
}

func withBufSize(n int) func(*connConfig) {
	return func(c *connConfig) { c.writeBufferSize = n }
}

func withManualCloseResponse() func(*connConfig) {
	return func(c *connConfig) { c.manualCloseResponse = true }
}

// writePeerFrame writes one unmasked frame from the peer side.
// Returns the error from the underlying writer instead of failing the
// test, so adversarial tests can ignore failures that occur after the
// conn has decided to close.
func writePeerFrame(w io.Writer, fin bool, opcode int, payload []byte) error {
	var hdr [10]byte
	hdr[0] = byte(opcode)
	if fin {
		hdr[0] |= 0x80
	}
	n := len(payload)
	var hdrLen int
	switch {
	case n <= 125:
		hdr[1] = byte(n)
		hdrLen = 2
	case n <= 0xFFFF:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		hdrLen = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		hdrLen = 10
	}
	if _, err := w.Write(hdr[:hdrLen]); err != nil {
		return err
	}
	if n > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

type peerFrame struct {
	fin     bool
	opcode  int
	payload []byte
}

// readPeerFrame reads one client-masked frame from the peer side. On
// any underlying read error it calls t.Errorf and returns the zero
// peerFrame; the caller's assertions on the returned frame then fail
// in the main test goroutine. (t.Fatal/FailNow must not be called
// from a non-test goroutine.)
func readPeerFrame(t *testing.T, r io.Reader) peerFrame {
	t.Helper()
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Errorf("peer read header: %v", err)
		return peerFrame{}
	}
	f := peerFrame{
		fin:    hdr[0]&0x80 != 0,
		opcode: int(hdr[0] & 0x0F),
	}
	if hdr[1]&0x80 == 0 {
		t.Errorf("client frame not masked")
		return peerFrame{}
	}
	length := int(hdr[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			t.Errorf("peer read ext-len: %v", err)
			return peerFrame{}
		}
		length = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			t.Errorf("peer read ext-len: %v", err)
			return peerFrame{}
		}
		length = int(binary.BigEndian.Uint64(ext[:]))
	}
	var key [4]byte
	if _, err := io.ReadFull(r, key[:]); err != nil {
		t.Errorf("peer read mask key: %v", err)
		return peerFrame{}
	}
	if length > 0 {
		f.payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.payload); err != nil {
			t.Errorf("peer read payload: %v", err)
			return peerFrame{}
		}
		for i := range f.payload {
			f.payload[i] ^= key[i%4]
		}
	}
	return f
}

// drainPeer reads and discards everything the conn writes until the
// peer connection is closed. Used in tests that just need the peer to
// not deadlock the conn.
func drainPeer(peer net.Conn) {
	var buf [512]byte
	for {
		_ = peer.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := peer.Read(buf[:]); err != nil {
			return
		}
	}
}

// fragments splits payload across len(sizes) data frames. sizes must
// sum to len(payload). The first frame carries op; the rest carry
// opContinuation; only the last has FIN=1.
func fragments(op int, payload []byte, sizes ...int) []peerFrame {
	out := make([]peerFrame, len(sizes))
	off := 0
	for i, n := range sizes {
		fop := opContinuation
		if i == 0 {
			fop = op
		}
		out[i] = peerFrame{
			fin:     i == len(sizes)-1,
			opcode:  fop,
			payload: payload[off : off+n],
		}
		off += n
	}
	return out
}

// TestServerReadsZeroMaskKey verifies the server accepts a masked frame whose
// mask key is all zeros: the identity mask is skipped and the payload is
// delivered as it appeared on the wire.
func TestServerReadsZeroMaskKey(t *testing.T) {
	c, peer := connPair(t, withServer())
	payload := "hello, world"
	go func() {
		// Text frame, fin, mask bit set, all-zero key, payload in the clear.
		frame := []byte{0x80 | opText, 0x80 | byte(len(payload)), 0, 0, 0, 0}
		frame = append(frame, payload...)
		_, _ = peer.Write(frame)
	}()

	got, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if mt != MessageText {
		t.Errorf("type = %v, want MessageText", mt)
	}
	if string(got) != payload {
		t.Errorf("payload = %q, want %q", got, payload)
	}
}

// frameBytes serializes frames as the peer would put them on the wire.
func frameBytes(frames []peerFrame) []byte {
	var buf bytes.Buffer
	for _, f := range frames {
		_ = writePeerFrame(&buf, f.fin, f.opcode, f.payload) // bytes.Buffer never errors
	}
	return buf.Bytes()
}

// deliveries are the transport segmentations every framing case runs under.
// The read path parses with io.ReadFull loops over a bufio.Reader, so the
// message delivered must not depend on how the bytes were chopped into
// writes: one write per frame (the typical arrival), the whole conversation
// in a single write (several frames sitting in the read buffer at once), and
// 3-byte chunks (frame headers, extended length fields, and multi-byte UTF-8
// runes all split across transport reads).
var deliveries = []struct {
	name  string
	write func(w io.Writer, frames []peerFrame) error
}{
	{"PerFrame", func(w io.Writer, frames []peerFrame) error {
		for _, f := range frames {
			if err := writePeerFrame(w, f.fin, f.opcode, f.payload); err != nil {
				return err
			}
		}
		return nil
	}},
	{"OneWrite", func(w io.Writer, frames []peerFrame) error {
		_, err := w.Write(frameBytes(frames))
		return err
	}},
	{"Chunked", func(w io.Writer, frames []peerFrame) error {
		for wire := frameBytes(frames); len(wire) > 0; {
			n := min(3, len(wire))
			if _, err := w.Write(wire[:n]); err != nil {
				return err
			}
			wire = wire[n:]
		}
		return nil
	}},
}

// TestReadMessageFramingVariants reads one message from hand-built peer
// frames — fragmentation, empty fragments, each length encoding, and control
// frames interleaved mid-message — delivered under every segmentation in
// deliveries, and expects the same (type, payload) from all of them.
func TestReadMessageFramingVariants(t *testing.T) {
	short := []byte("hello, world")
	thousand := bytes.Repeat([]byte("X"), 1000)
	medium := bytes.Repeat([]byte("x"), 500)   // forces 16-bit length
	large := bytes.Repeat([]byte("y"), 70_000) // forces 64-bit length
	combo := bytes.Repeat([]byte("z"), 4*30_000)

	cases := []struct {
		name        string
		frames      []peerFrame
		wantOp      MessageType
		wantPayload []byte
	}{
		{
			name:   "SingleFrame",
			frames: []peerFrame{{fin: true, opcode: opText, payload: short}},
			wantOp: MessageText, wantPayload: short,
		},
		{
			name:   "EmptySingleFrame",
			frames: []peerFrame{{fin: true, opcode: opText}},
			wantOp: MessageText, wantPayload: nil,
		},
		{
			name:   "TwoFragments",
			frames: fragments(opText, short, 5, 7),
			wantOp: MessageText, wantPayload: short,
		},
		{
			name:   "ManyFragments",
			frames: fragments(opBinary, thousand, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100),
			wantOp: MessageBinary, wantPayload: thousand,
		},
		{
			name:   "EmptyFirstFragment",
			frames: fragments(opText, short, 0, 12),
			wantOp: MessageText, wantPayload: short,
		},
		{
			name:   "EmptyMiddleFragment",
			frames: fragments(opText, short, 5, 0, 7),
			wantOp: MessageText, wantPayload: short,
		},
		{
			name:   "EmptyLastFragment",
			frames: fragments(opText, short, 12, 0),
			wantOp: MessageText, wantPayload: short,
		},
		{
			name:   "AllEmptyFragments",
			frames: fragments(opText, nil, 0, 0, 0),
			wantOp: MessageText, wantPayload: nil,
		},
		{
			name:   "SixteenBitLength",
			frames: []peerFrame{{fin: true, opcode: opBinary, payload: medium}},
			wantOp: MessageBinary, wantPayload: medium,
		},
		{
			name:   "SixtyFourBitLength",
			frames: []peerFrame{{fin: true, opcode: opBinary, payload: large}},
			wantOp: MessageBinary, wantPayload: large,
		},
		{
			name:   "BinaryArbitraryBytes",
			frames: []peerFrame{{fin: true, opcode: opBinary, payload: []byte{0x00, 0x01, 0xfe, 0xff}}},
			wantOp: MessageBinary, wantPayload: []byte{0x00, 0x01, 0xfe, 0xff},
		},
		{
			name: "PingBetweenFragments",
			frames: []peerFrame{
				{fin: false, opcode: opText, payload: []byte("hello, ")},
				{fin: true, opcode: opPing, payload: []byte("p1")},
				{fin: true, opcode: opContinuation, payload: []byte("world")},
			},
			wantOp: MessageText, wantPayload: []byte("hello, world"),
		},
		{
			name: "EmptyPingBetweenFragments",
			frames: []peerFrame{
				{fin: false, opcode: opText, payload: []byte("hello, ")},
				{fin: true, opcode: opPing},
				{fin: true, opcode: opContinuation, payload: []byte("world")},
			},
			wantOp: MessageText, wantPayload: []byte("hello, world"),
		},
		{
			// Unsolicited Pong is allowed (RFC 6455 §5.5.3).
			name: "PongBetweenFragments",
			frames: []peerFrame{
				{fin: false, opcode: opText, payload: []byte("hello, ")},
				{fin: true, opcode: opPong, payload: []byte("unsolicited")},
				{fin: true, opcode: opContinuation, payload: []byte("world")},
			},
			wantOp: MessageText, wantPayload: []byte("hello, world"),
		},
		{
			name: "MultipleControlsBetweenFragments",
			frames: []peerFrame{
				{fin: false, opcode: opText, payload: []byte("hel")},
				{fin: true, opcode: opPing, payload: []byte("p1")},
				{fin: true, opcode: opPong, payload: []byte("unsolicited")},
				{fin: true, opcode: opContinuation, payload: []byte("lo")},
			},
			wantOp: MessageText, wantPayload: []byte("hello"),
		},
		{
			// 4 fragments of 30 KiB (each requires 16-bit length on the
			// wire) with pings interleaved; total payload > 64 KiB.
			name:        "PingWithLargeFragmentedMessage",
			frames:      interleavePings(fragments(opBinary, combo, 30_000, 30_000, 30_000, 30_000)),
			wantOp:      MessageBinary,
			wantPayload: combo,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, d := range deliveries {
				t.Run(d.name, func(t *testing.T) {
					c, peer := connPair(t)
					errc := make(chan error, 1)
					go func() { errc <- d.write(peer, tc.frames) }()
					// The conn answers inbound Pings inline with ReadMessage,
					// and net.Pipe is unbuffered: drain the peer side
					// concurrently so any auto-Pong write doesn't block the
					// read path.
					go drainPeer(peer)

					got, op, err := c.ReadMessage(nil)
					if err != nil {
						t.Fatalf("ReadMessage: %v", err)
					}
					if op != tc.wantOp {
						t.Errorf("op = %v, want %v", op, tc.wantOp)
					}
					if !bytes.Equal(got, tc.wantPayload) {
						t.Errorf("payload len %d, want %d", len(got), len(tc.wantPayload))
					}
					if err := <-errc; err != nil {
						t.Errorf("peer write: %v", err)
					}
				})
			}
		})
	}
}

// interleavePings returns a frame list with a 1-byte Ping inserted
// before every data frame except the first.
func interleavePings(data []peerFrame) []peerFrame {
	out := make([]peerFrame, 0, 2*len(data)-1)
	out = append(out, data[0])
	for _, f := range data[1:] {
		out = append(out, peerFrame{fin: true, opcode: opPing, payload: []byte("p")})
		out = append(out, f)
	}
	return out
}

func TestReadMessageTooBig(t *testing.T) {
	// readFragment rejects an over-limit message on the frame header, before
	// any payload is read: a single frame declaring more than MaxMessageSize,
	// or a continuation pushing the cumulative size past it.
	tenByteFragment := append([]byte{0x02, 0x0a}, make([]byte, 10)...) // Binary, no FIN
	cases := []struct {
		name   string
		maxMsg int
		wire   []byte
	}{
		{"SingleFrame", 16, []byte{0x82, 0x20}},                        // Binary, fin, len=32
		{"CumulativeFragments", 20, append(tenByteFragment, 0x00, 30)}, // 10 sent + 30 declared > 20
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t, withMaxMsg(tc.maxMsg))
			go func() {
				// Drain in parallel so the conn's outbound CloseMessageTooBig
				// frame doesn't deadlock against the synchronous net.Pipe.
				_, _ = peer.Write(tc.wire)
				drainPeer(peer)
			}()
			if _, _, err := c.ReadMessage(nil); !errors.Is(err, ErrProtocol) {
				t.Errorf("ReadMessage err = %v, want ErrProtocol", err)
			}
		})
	}
}

// TestReaderAbort covers Reader.Abort: the read half stops without draining
// the message (the promised payload bytes are never sent), the peer receives
// a close frame with the given code, and the enclosing Read returns the
// callback's error, or an ErrClosed-wrapping error when the callback
// returns nil.
func TestReaderAbort(t *testing.T) {
	sentinel := errors.New("reject message")
	cases := []struct {
		name        string
		callbackErr error
		wantErr     error
	}{
		{"CallbackError", sentinel, sentinel},
		{"NilCallbackError", nil, ErrClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)
			errCh := make(chan error, 1)
			go func() {
				errCh <- c.Read(nil, func(r *Reader) error {
					if err := r.Abort(ClosePolicyViolation, "bad message"); err != nil {
						return err
					}
					return tc.callbackErr
				})
			}()

			// Start a message but do not send the promised payload. A
			// non-aborted Reader.close would try to drain those 5 bytes
			// before Read returns.
			if _, err := peer.Write([]byte{0x82, 0x05}); err != nil {
				t.Fatalf("peer write header: %v", err)
			}
			f := readPeerFrame(t, peer)
			if f.opcode != opClose {
				t.Fatalf("peer received opcode %d, want Close", f.opcode)
			}
			if len(f.payload) < 2 || CloseCode(binary.BigEndian.Uint16(f.payload[:2])) != ClosePolicyViolation {
				t.Fatalf("close payload = %x, want code %d", f.payload, ClosePolicyViolation)
			}
			if err := <-errCh; !errors.Is(err, tc.wantErr) {
				t.Fatalf("Read err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestPingFromPeerAnsweredWithPong(t *testing.T) {
	c, peer := connPair(t)

	go func() {
		// Peer sends a ping, reads our pong, then sends data so the
		// reader returns.
		if err := writePeerFrame(peer, true, opPing, []byte("ping data")); err != nil {
			return
		}
		f := readPeerFrame(t, peer)
		if f.opcode != opPong || string(f.payload) != "ping data" {
			t.Errorf("expected pong %q, got %+v", "ping data", f)
		}
		_ = writePeerFrame(peer, true, opText, []byte("hi"))
	}()

	data, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if mt != MessageText || string(data) != "hi" {
		t.Errorf("mt=%v data=%q", mt, data)
	}
}

// TestWriteSingleFrame covers writes whose whole message goes out as one FIN
// frame: WriteMessage for each type (FIN folded onto the data frame, no empty
// terminator), empty messages, coalescing of small Write/WriteString calls,
// and Final folding FIN onto a payload far larger than the staging buffer.
// Reading exactly one frame and requiring FIN on it proves nothing
// fragmented.
func TestWriteSingleFrame(t *testing.T) {
	large := strings.Repeat("z", 300) // >> the 64 B minimum buffer

	cases := []struct {
		name    string
		bufSize int // 0 = default
		write   func(t *testing.T, c *Conn) error
		wantOp  int
		want    string
	}{
		{
			name: "WriteMessageBinary",
			write: func(t *testing.T, c *Conn) error {
				return c.WriteMessage(context.Background(), MessageBinary, nil, []byte("hello, world"))
			},
			wantOp: opBinary, want: "hello, world",
		},
		{
			name: "WriteMessageText",
			write: func(t *testing.T, c *Conn) error {
				return c.WriteMessage(context.Background(), MessageText, nil, []byte("x"))
			},
			wantOp: opText, want: "x",
		},
		{
			// Empty WriteMessage: w.Write of an empty slice is a no-op, so
			// the writer's close emits a single empty FIN frame.
			name: "EmptyMessage",
			write: func(t *testing.T, c *Conn) error {
				return c.WriteMessage(context.Background(), MessageBinary, nil, nil)
			},
			wantOp: opBinary, want: "",
		},
		{
			// Small writes that fit the staging buffer coalesce into a
			// single final frame rather than one frame per Write.
			name: "CoalescedWrites",
			write: func(t *testing.T, c *Conn) error {
				return c.Write(context.Background(), MessageText, nil, func(w *Writer) error {
					if _, err := w.Write([]byte("part1")); err != nil {
						return err
					}
					_, err := w.Write([]byte("part2"))
					return err
				})
			},
			wantOp: opText, want: "part1part2",
		},
		{
			name: "CoalescedWriteStrings",
			write: func(t *testing.T, c *Conn) error {
				return c.Write(context.Background(), MessageText, nil, func(w *Writer) error {
					if _, err := w.WriteString("foo"); err != nil {
						return err
					}
					_, err := io.WriteString(w, "bar") // exercises the io.StringWriter path
					return err
				})
			},
			wantOp: opText, want: "foobar",
		},
		{
			// The headline of coalescing: Final() followed by one write much
			// larger than the staging buffer still goes out as exactly one
			// frame carrying the whole payload, chunked masked through the
			// buffer on this client-mode conn. If it had fragmented, this
			// frame would hold only a buffer's worth without FIN.
			name:    "FinalThenLargeWrite",
			bufSize: minWriteBufferSize,
			write: func(t *testing.T, c *Conn) error {
				return c.Write(context.Background(), MessageText, nil, func(w *Writer) error {
					w.Final()
					_, err := w.Write([]byte(large))
					return err
				})
			},
			wantOp: opText, want: large,
		},
		{
			// WriteString shares Write's Final semantics: the data through
			// this call finishes the message, and further writes are
			// rejected.
			name: "FinalThenWriteString",
			write: func(t *testing.T, c *Conn) error {
				return c.Write(context.Background(), MessageText, nil, func(w *Writer) error {
					w.Final()
					if _, err := w.WriteString("last"); err != nil {
						return err
					}
					if _, err := w.WriteString("nope"); !errors.Is(err, errWriterScope) {
						t.Errorf("WriteString after final = %v, want errWriterScope", err)
					}
					return nil
				})
			},
			wantOp: opText, want: "last",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts []func(*connConfig)
			if tc.bufSize != 0 {
				opts = append(opts, withBufSize(tc.bufSize))
			}
			c, peer := connPair(t, opts...)
			done := make(chan peerFrame, 1)
			go func() { done <- readPeerFrame(t, peer) }()
			if err := tc.write(t, c); err != nil {
				t.Fatalf("write: %v", err)
			}
			f := <-done
			if !f.fin || f.opcode != tc.wantOp || string(f.payload) != tc.want {
				t.Errorf("frame = {fin:%v opcode:%d payload:%q}, want FIN, opcode %d, payload %q",
					f.fin, f.opcode, f.payload, tc.wantOp, tc.want)
			}
		})
	}
}

// readerOnly hides any io.WriterTo on the wrapped reader (only io.Reader's
// Read is promoted through the interface field), forcing io.Copy to use
// the destination's ReadFrom.
type readerOnly struct{ io.Reader }

func TestWriterReadFrom(t *testing.T) {
	// io.Copy into a Writer uses Writer.ReadFrom. With a small staging
	// buffer it streams many frames (the io.Copy fallback would emit just
	// one data frame + terminator), which must reassemble to the source.
	c, peer := connPair(t, withBufSize(minWriteBufferSize)) // 64 B buffer

	payload := bytes.Repeat([]byte("abcdefgh"), 200) // 1600 B
	done := make(chan struct{})
	var got []byte
	var nframes int
	go func() {
		defer close(done)
		for {
			f := readPeerFrame(t, peer)
			got = append(got, f.payload...)
			nframes++
			if f.fin {
				return
			}
		}
	}()

	err := c.Write(context.Background(), MessageBinary, nil, func(w *Writer) error {
		n, err := io.Copy(w, readerOnly{bytes.NewReader(payload)})
		if err != nil {
			return err
		}
		if n != int64(len(payload)) {
			t.Errorf("io.Copy n = %d, want %d", n, len(payload))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	<-done

	if nframes <= 2 {
		t.Errorf("got %d frames, want many (ReadFrom should stream through the buffer)", nframes)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("reassembled %d bytes, want %d", len(got), len(payload))
	}
}

func TestWriterReadFromSourceErrorFailsMessage(t *testing.T) {
	// A non-EOF read error from the source must fail the message so it cannot be
	// silently completed: even when the callback discards the error and returns
	// nil, Conn.Write reports it and closes gracefully (CloseInternalError)
	// rather than FINing a truncated message.
	c, peer := connPair(t)
	sentinel := errors.New("source read failed")

	got := make(chan peerFrame, 1)
	go func() { got <- readPeerFrame(t, peer) }()

	var readFromErr error
	err := c.Write(context.Background(), MessageBinary, nil, func(w *Writer) error {
		_, readFromErr = w.ReadFrom(iotest.ErrReader(sentinel))
		return nil // swallow it; the message must still not complete
	})
	if !errors.Is(readFromErr, sentinel) {
		t.Errorf("ReadFrom err = %v, want sentinel", readFromErr)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("Write err = %v, want sentinel (message must not silently complete)", err)
	}
	f := <-got
	if f.opcode != opClose {
		t.Fatalf("peer received opcode %d, want Close (no data frame)", f.opcode)
	}
	if len(f.payload) < 2 || CloseCode(binary.BigEndian.Uint16(f.payload[:2])) != CloseInternalError {
		t.Errorf("close payload = %x, want code %d", f.payload, CloseInternalError)
	}
}

func TestWriterReadFromDataWithEOF(t *testing.T) {
	// A source that returns its final bytes together with io.EOF (the iotest
	// DataErrReader contract) must not lose that last chunk: ReadFrom adds the
	// data before treating EOF as a clean end, and the message round-trips whole.
	c, peer := connPair(t)
	payload := []byte("final chunk arrives with EOF")

	got := make(chan []byte, 1)
	go func() {
		var all []byte
		for {
			f := readPeerFrame(t, peer)
			all = append(all, f.payload...)
			if f.fin {
				got <- all
				return
			}
		}
	}()

	err := c.Write(context.Background(), MessageBinary, nil, func(w *Writer) error {
		n, err := w.ReadFrom(iotest.DataErrReader(bytes.NewReader(payload)))
		if err != nil {
			t.Errorf("ReadFrom err = %v, want nil (EOF is a clean end)", err)
		}
		if n != int64(len(payload)) {
			t.Errorf("ReadFrom n = %d, want %d", n, len(payload))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if all := <-got; !bytes.Equal(all, payload) {
		t.Errorf("peer reassembled %q, want %q", all, payload)
	}
}

func TestReaderWriteTo(t *testing.T) {
	// io.Copy out of a Reader uses Reader.WriteTo. The peer sends a
	// fragmented message; WriteTo must drain every fragment into dst.
	c, peer := connPair(t)

	part1 := bytes.Repeat([]byte("x"), 100)
	part2 := bytes.Repeat([]byte("y"), 100)
	go func() {
		_ = writePeerFrame(peer, false, opBinary, part1)
		_ = writePeerFrame(peer, true, opContinuation, part2)
	}()

	var buf bytes.Buffer
	err := c.Read(nil, func(r *Reader) error {
		n, err := io.Copy(&buf, r) // r implements io.WriterTo
		if err != nil {
			return err
		}
		if n != 200 {
			t.Errorf("io.Copy n = %d, want 200", n)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := append(append([]byte{}, part1...), part2...)
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("copied %d bytes, want %d", buf.Len(), len(want))
	}
}

func TestServerWriteLargerThanBuffer(t *testing.T) {
	// A server frame whose payload exceeds WriteBufferSize takes the
	// two-write path in writeFrameLocked: the first rwc.Write carries the
	// header plus a buffer's worth of payload; the second writes the
	// remainder straight from the caller's slice with no copy. Verify
	// the frame round-trips unmasked and intact and that the caller's
	// slice is left untouched.
	c, peer := connPair(t, withServer(), withBufSize(64))

	payload := bytes.Repeat([]byte("0123456789abcdef"), 50) // 800 bytes > 64
	orig := bytes.Clone(payload)

	errCh := make(chan error, 1)
	go func() { errCh <- c.WriteMessage(context.Background(), MessageBinary, nil, payload) }()

	// Read the server frame on the peer: unmasked, 16-bit length.
	var hdr [2]byte
	if _, err := io.ReadFull(peer, hdr[:]); err != nil {
		t.Fatalf("peer read header: %v", err)
	}
	if hdr[0] != 0x80|byte(opBinary) {
		t.Errorf("hdr[0] = %#x, want fin|binary", hdr[0])
	}
	if hdr[1]&0x80 != 0 {
		t.Errorf("server frame is masked; want unmasked")
	}
	if hdr[1]&0x7F != 126 {
		t.Fatalf("length field = %d, want 126 (16-bit form)", hdr[1]&0x7F)
	}
	var ext [2]byte
	if _, err := io.ReadFull(peer, ext[:]); err != nil {
		t.Fatalf("peer read ext-len: %v", err)
	}
	length := int(binary.BigEndian.Uint16(ext[:]))
	if length != len(payload) {
		t.Fatalf("frame length = %d, want %d", length, len(payload))
	}
	got := make([]byte, length)
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatalf("peer read payload: %v", err)
	}
	if !bytes.Equal(got, orig) {
		t.Errorf("payload round-trip mismatch")
	}
	if !bytes.Equal(payload, orig) {
		t.Errorf("writeFrameLocked mutated the caller's payload slice")
	}
	if err := <-errCh; err != nil {
		t.Errorf("WriteMessage: %v", err)
	}
}

// countingConn wraps a net.Conn and counts Write calls, so a test can assert
// how many segments a frame reaches the transport in. writes is read only
// after the writing goroutine is joined (a channel receive), so no atomics.
type countingConn struct {
	net.Conn
	writes int
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes++
	return c.Conn.Write(p)
}

// TestWriteSegmentCount pins how many Writes one large frame makes to the
// transport: a server frame goes out in exactly two (header + a buffer's
// worth of payload, then the uncopied tail in one write), and a client frame
// in the two-segment shape plus the tail chunked through the write buffer,
// which masking requires (it must not modify the caller's slice). Rate mode
// must not change the segmentation: the frame's deadline is computed from its
// payload size up front, not enforced by chunking the writes.
func TestWriteSegmentCount(t *testing.T) {
	const bufSize = 64              // capRoom = bufSize - maxHeaderLen = 50
	payload := make([]byte, 800)    // one frame: 50 in-buffer + 750 overflow tail
	clientWrites := 1 + (750+63)/64 // header segment + masked 64-byte chunks
	cases := []struct {
		name       string
		server     bool
		opts       *WriteOptions
		wantWrites int
		wireLen    int // header + payload as the peer sees it
	}{
		{"Server", true, nil, 2, 4 + 800},
		{"ServerRate", true, &WriteOptions{MinWriteRate: 100000}, 2, 4 + 800},
		{"Client", false, nil, clientWrites, 8 + 800},
		{"ClientRate", false, &WriteOptions{MinWriteRate: 100000}, clientWrites, 8 + 800},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			cc := &countingConn{Conn: a}
			opts := []func(*connConfig){withBufSize(bufSize)}
			if tc.server {
				opts = append(opts, withServer())
			}
			c := newTestConn(t, cc, opts...)
			t.Cleanup(func() { _ = b.Close() })

			errCh := make(chan error, 1)
			go func() {
				errCh <- c.WriteMessage(context.Background(), MessageBinary, tc.opts, payload)
			}()
			if _, err := io.ReadFull(b, make([]byte, tc.wireLen)); err != nil {
				t.Fatalf("peer drain: %v", err)
			}
			if err := <-errCh; err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			if cc.writes != tc.wantWrites {
				t.Errorf("transport writes = %d, want %d", cc.writes, tc.wantWrites)
			}
		})
	}
}

// TestPeerInitiatedClose covers the variants of a peer-initiated close
// frame: a well-formed payload and an empty payload (→ CloseNoStatusReceived)
// surface a *CloseError, while a malformed payload is a peer protocol error
// and surfaces ErrProtocol. In every case the conn answers with a close
// frame: the echoed payload, or a CloseProtocolError reply.
func TestPeerInitiatedClose(t *testing.T) {
	cases := []struct {
		name     string
		payload  []byte
		wantCode CloseCode // non-zero: expect *CloseError with this code; zero: expect ErrProtocol
		wantText string    // checked only when non-empty
	}{
		{
			name:     "WithCodeAndReason",
			payload:  []byte{0x03, 0xe8, 'b', 'y', 'e'}, // code 1000, reason "bye"
			wantCode: 1000,
			wantText: "bye",
		},
		{
			name:     "Empty",
			payload:  nil,
			wantCode: CloseNoStatusReceived,
		},
		{
			name:    "MalformedTooShort",
			payload: []byte{0x03}, // 1-byte payload, protocol error
		},
		{
			name:    "ReservedCode",
			payload: []byte{0x03, 0xec}, // code 1004 is reserved on the wire
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)
			go func() {
				_ = writePeerFrame(peer, true, opClose, tc.payload)
				// The conn answers with a close (echo or protocol-error reply).
				f := readPeerFrame(t, peer)
				if f.opcode != opClose {
					t.Errorf("expected close reply, got %+v", f)
				}
			}()
			_, _, err := c.ReadMessage(nil)
			if tc.wantCode == 0 {
				if !errors.Is(err, ErrProtocol) {
					t.Fatalf("expected ErrProtocol, got %T: %v", err, err)
				}
				return
			}
			var ce *CloseError
			if !errors.As(err, &ce) {
				t.Fatalf("expected *CloseError, got %T: %v", err, err)
			}
			if ce.Code != tc.wantCode {
				t.Errorf("(*CloseError).Code = %d, want %d", ce.Code, tc.wantCode)
			}
			if tc.wantText != "" && ce.Text != tc.wantText {
				t.Errorf("(*CloseError).Text = %q, want %q", ce.Text, tc.wantText)
			}
		})
	}
}

func TestShutdownThenPeerEcho(t *testing.T) {
	c, peer := connPair(t)

	go func() {
		// Read the close we sent via Shutdown and echo it back.
		f := readPeerFrame(t, peer)
		if f.opcode != opClose {
			t.Errorf("expected close, got %+v", f)
		}
		// The oversized reason was truncated on the wire to the
		// control-frame cap (2-byte code + 123 reason bytes).
		if len(f.payload) != maxControlPayload {
			t.Errorf("close payload len = %d, want %d", len(f.payload), maxControlPayload)
		}
		_ = writePeerFrame(peer, true, opClose, f.payload)
	}()

	// A reason longer than the 123-byte protocol budget is accepted and
	// truncated, not rejected.
	if err := c.Shutdown(CloseNormalClosure, strings.Repeat("x", 200)); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	_, _, err := c.ReadMessage(nil)
	var ce *CloseError
	if !errors.As(err, &ce) {
		t.Errorf("expected *CloseError after peer echoed our close, got %v", err)
	}
}

// TestManualCloseResponse covers ManualCloseResponse: the read loop reports the
// peer's close as a *CloseError but does not echo it, leaving the write half
// open so the application can flush a pending message and then respond with
// Shutdown.
func TestManualCloseResponse(t *testing.T) {
	c, peer := connPair(t, withManualCloseResponse())

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Send a close; the conn must not echo it automatically.
		if err := writePeerFrame(peer, true, opClose, []byte{0x03, 0xe8, 'b', 'y', 'e'}); err != nil {
			t.Errorf("peer write close: %v", err)
			return
		}
		// The application flushes a queued data message before responding;
		// an automatic echo would instead arrive here as a close.
		if f := readPeerFrame(t, peer); f.opcode != opText || string(f.payload) != "after close" {
			t.Errorf("peer got %+v, want text %q", f, "after close")
			return
		}
		// The application's Shutdown then sends the close response.
		if f := readPeerFrame(t, peer); f.opcode != opClose {
			t.Errorf("peer got %+v, want close response", f)
		}
	}()

	_, _, err := c.ReadMessage(nil)
	var ce *CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CloseError, got %T: %v", err, err)
	}
	if ce.Code != CloseNormalClosure || ce.Text != "bye" {
		t.Errorf("CloseError = {%d %q}, want {%d %q}", ce.Code, ce.Text, CloseNormalClosure, "bye")
	}

	// No automatic echo means frame.closeSent is clear, so the write half is
	// still open: the application can finish writing. An auto-echo would fail
	// this with ErrCloseSent.
	if err := c.WriteMessage(context.Background(), MessageText, nil, []byte("after close")); err != nil {
		t.Fatalf("WriteMessage after manual close: %v", err)
	}
	// Complete the handshake by echoing the peer's code and reason.
	if err := c.Shutdown(ce.Code, ce.Text); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	<-done
}

func TestShutdownRejectsInvalidArgument(t *testing.T) {
	// An invalid argument is rejected before anything reaches the wire, so one
	// conn serves all cases. The codes are receive-only, reserved-range, and
	// out-of-range (full classification in TestValidWireCloseCode); the reason
	// must be valid UTF-8 (RFC 6455 §8.1).
	c, _ := connPair(t)
	for _, tc := range []struct {
		name    string
		code    CloseCode
		reason  string
		wantErr error
	}{
		{"NoStatusReceived", CloseNoStatusReceived, "", errInvalidCloseCode},
		{"AbnormalClosure", CloseAbnormalClosure, "", errInvalidCloseCode},
		{"BelowRange", 999, "", errInvalidCloseCode},
		{"ReservedRange", 1016, "", errInvalidCloseCode},
		{"PrivateBelowRange", 2999, "", errInvalidCloseCode},
		{"AboveRange", 5000, "", errInvalidCloseCode},
		{"InvalidReasonUTF8", CloseNormalClosure, "\xff\xfe", errInvalidCloseReason},
	} {
		if err := c.Shutdown(tc.code, tc.reason); !errors.Is(err, tc.wantErr) {
			t.Errorf("%s: Shutdown(%d, %q) = %v, want %v", tc.name, tc.code, tc.reason, err, tc.wantErr)
		}
	}
}

func TestAfterShutdown(t *testing.T) {
	// Once our close is out, a second Shutdown is a no-op and a data write
	// is rejected with ErrCloseSent.
	c, peer := connPair(t)
	go drainPeer(peer)

	if err := c.Shutdown(CloseNormalClosure, ""); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := c.Shutdown(CloseNormalClosure, ""); err != nil {
		t.Errorf("second Shutdown: %v, want nil", err)
	}
	err := c.WriteMessage(context.Background(), MessageText, nil, []byte("nope"))
	if !errors.Is(err, ErrCloseSent) {
		t.Errorf("WriteMessage after Shutdown: %v, want ErrCloseSent", err)
	}
}

// TestInvalidPeerFramesRejected covers malformed inbound frames the conn must
// reject as protocol errors above the header-parse layer: reserved and
// fragmented control opcodes, oversized control payloads, and bad data-frame
// sequencing. Malformed frame headers (RSV, mask discipline, length encoding)
// are exercised directly against readHeader in protocol_test.go. Each case writes
// a hand-crafted byte sequence to the peer side and asserts ReadMessage returns
// an error whose message contains the named substring.
func TestInvalidPeerFramesRejected(t *testing.T) {
	cases := []struct {
		name    string
		bytes   []byte
		wantSub string
	}{
		{
			name:    "ReservedDataOpcode",
			bytes:   []byte{0x83, 0x00}, // opcode 0x3, reserved non-control
			wantSub: "expected data frame",
		},
		{
			name:    "ReservedControlOpcode",
			bytes:   []byte{0x8b, 0x00}, // opcode 0xB, reserved control
			wantSub: "unknown control opcode",
		},
		{
			name:    "FragmentedControlFrame",
			bytes:   []byte{0x09, 0x00}, // Ping with FIN clear
			wantSub: "fragmented control",
		},
		{
			name:    "OversizedControlFrame",
			bytes:   []byte{0x89, 126, 0x00, 0x7e}, // Ping claiming 126-byte payload (> 125 limit)
			wantSub: "oversized control",
		},
		{
			name:    "ContinuationAsFirstFrame",
			bytes:   []byte{0x80, 0x00}, // Continuation as the message's first frame
			wantSub: "expected data frame",
		},
		{
			name: "NonContinuationBetweenFragments",
			// A non-FIN Text fragment followed by a new Text frame where a
			// continuation is required (RFC 6455 §5.4).
			bytes:   []byte{0x01, 0x05, 'h', 'e', 'l', 'l', 'o', 0x81, 0x05, 'w', 'o', 'r', 'l', 'd'},
			wantSub: "expected continuation",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)
			go func() {
				_, _ = peer.Write(tc.bytes)
				drainPeer(peer)
			}()
			_, _, err := c.ReadMessage(nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("ReadMessage err = %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestProtocolErrorSendsCloseFrame(t *testing.T) {
	// RFC 6455 §7.1.7: on a peer protocol violation the conn fails the
	// connection with a best-effort CloseProtocolError (1002) frame.
	c, peer := connPair(t)

	got := make(chan peerFrame, 1)
	go func() {
		_, _ = peer.Write([]byte{0xc1, 0x00}) // RSV1 set, protocol error
		got <- readPeerFrame(t, peer)         // read the conn's outbound Close
	}()

	_, _, err := c.ReadMessage(nil)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("ReadMessage err = %v, want ErrProtocol", err)
	}
	f := <-got
	if f.opcode != opClose {
		t.Fatalf("peer received opcode %v, want opClose", f.opcode)
	}
	if len(f.payload) < 2 || CloseCode(binary.BigEndian.Uint16(f.payload[:2])) != CloseProtocolError {
		t.Errorf("close payload = %v, want code %d", f.payload, CloseProtocolError)
	}
}

func TestIdleTriggersPingAndPongResets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t)
		opts := &ReadOptions{KeepaliveInterval: 40 * time.Millisecond, KeepaliveTimeout: 500 * time.Millisecond}

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Expect a keepalive ping after KeepaliveInterval of silence.
			f := readPeerFrame(t, peer)
			if f.opcode != opPing {
				t.Errorf("expected ping, got %+v", f)
				return
			}
			// Pong it back. A qualifying control frame resets the cadence.
			_ = writePeerFrame(peer, true, opPong, f.payload)
			// Then send data so the read returns.
			_ = writePeerFrame(peer, true, opText, []byte("hi"))
		}()

		data, mt, err := c.ReadMessage(opts)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if mt != MessageText || string(data) != "hi" {
			t.Errorf("mt=%v data=%q", mt, data)
		}
		<-done
	})
}

func TestResponseTimeoutClosesConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t)
		defer peer.Close() // unblocks drainPeer before synctest waits for goroutines
		// Drain whatever the conn sends but never pong.
		go drainPeer(peer)

		opts := &ReadOptions{KeepaliveInterval: 30 * time.Millisecond, KeepaliveTimeout: 60 * time.Millisecond}
		_, _, err := c.ReadMessage(opts)
		if !errors.Is(err, ErrTimeout) {
			t.Errorf("ReadMessage err = %v, want ErrTimeout", err)
		}
	})
}

func TestStalledReadTimesOut(t *testing.T) {
	// A peer that sends the start of a frame and then goes silent must time out,
	// whichever bound the reader set: a wait cap while the message has not yet
	// begun, or a body deadline/rate once the body is awaited. (0x82 = binary
	// FIN; the trailing 0x64 declares a 100-byte payload that never arrives.)
	// Partial control frames park the read goroutine in handleControlFrame's
	// payload read instead, under the same watchdog: a truncated frame never
	// reaches noteControl, so it cannot refresh liveness.
	cases := []struct {
		name   string
		header []byte // bytes the peer sends before stalling
		opts   *ReadOptions
	}{
		{"IdleTimeout while waiting (partial header)", []byte{0x82}, &ReadOptions{IdleTimeout: 40 * time.Millisecond}},
		{"MessageTimeout on the body (full header)", []byte{0x82, 0x64}, &ReadOptions{MessageTimeout: 40 * time.Millisecond}},
		{"MinReadRate on the body (full header)", []byte{0x82, 0x64}, &ReadOptions{MinReadRate: 1000}},
		// Ping (0x89) declaring 5 payload bytes, only 2 arrive.
		{"IdleTimeout while waiting (partial control payload)", []byte{0x89, 0x05, 'h', 'i'}, &ReadOptions{IdleTimeout: 40 * time.Millisecond}},
		// Pong (0x8A) declaring 5 payload bytes, only 2 arrive: the truncated
		// pong must not satisfy the pong wait for the keepalive ping.
		{"KeepaliveTimeout (partial pong payload)", []byte{0x8A, 0x05, 'h', 'i'}, &ReadOptions{KeepaliveInterval: 30 * time.Millisecond, KeepaliveTimeout: 60 * time.Millisecond}},
		// A complete non-FIN binary fragment ("hi"), then a ping declaring 5
		// payload bytes of which 1 arrives: the body deadline covers a stall
		// inside an interleaved control frame.
		{"MessageTimeout with partial control frame mid-body", []byte{0x02, 0x02, 'h', 'i', 0x89, 0x05, 'x'}, &ReadOptions{MessageTimeout: 40 * time.Millisecond}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, peer := connPair(t)
				defer peer.Close()
				go func() {
					_, _ = peer.Write(tc.header)
					drainPeer(peer)
				}()
				if _, _, err := c.ReadMessage(tc.opts); !errors.Is(err, ErrTimeout) {
					t.Errorf("ReadMessage = %v, want ErrTimeout", err)
				}
			})
		})
	}
}

func TestCloseIdempotentAndSticky(t *testing.T) {
	c, peer := connPair(t)
	go drainPeer(peer)
	if err := c.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	// A read after a local Close must report ErrClosed specifically,
	// the documented signal for telling a deliberate local close apart
	// from a peer or transport failure.
	if _, _, err := c.ReadMessage(nil); !errors.Is(err, ErrClosed) {
		t.Errorf("ReadMessage after Close = %v, want ErrClosed", err)
	}
}

func TestReaderDrainsOnClose(t *testing.T) {
	c, peer := connPair(t)

	first := []byte("first message — partially consumed by user")
	second := []byte("second message")

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = writePeerFrame(peer, true, opText, first)
		_ = writePeerFrame(peer, true, opText, second)
	}()

	// Consume only the first 5 bytes of the first message, then return.
	// Conn.Read defers r.Close, which must drain the remaining bytes so
	// the next ReadMessage starts on a clean frame boundary.
	if err := c.Read(nil, func(r *Reader) error {
		buf := make([]byte, 5)
		_, err := io.ReadFull(r, buf)
		return err
	}); err != nil {
		t.Fatalf("first Read: %v", err)
	}

	data, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if mt != MessageText || !bytes.Equal(data, second) {
		t.Errorf("c.ReadMessage(nil) = (%q, %v), want Text %q", data, mt, second)
	}
	<-done
}

// TestWriteCallbackErrorClosesConn covers fn returning an error: Write cannot
// tell a clean abort from a half-written message, so any fn error closes the
// connection. The transport is healthy, so it starts a graceful closing
// handshake with CloseInternalError — whether no frame went out, or a small
// buffer already flushed one ("started").
func TestWriteCallbackErrorClosesConn(t *testing.T) {
	sentinel := errors.New("user gave up")
	cases := []struct {
		name       string
		writeFirst bool // overflow the staging buffer so a frame is on the wire
	}{
		{"BeforeAnyWrite", false},
		{"AfterFrameOnWire", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t, withBufSize(minWriteBufferSize))

			frames := make(chan peerFrame, 2)
			go func() {
				for {
					f := readPeerFrame(t, peer)
					frames <- f
					if f.opcode == opClose {
						return
					}
				}
			}()

			err := c.Write(context.Background(), MessageBinary, nil, func(w *Writer) error {
				if tc.writeFirst {
					// Larger than the staging buffer's room, so it flushes a
					// frame before the callback returns its error.
					if _, err := w.Write(bytes.Repeat([]byte("x"), 200)); err != nil {
						return err
					}
				}
				return sentinel
			})
			if !errors.Is(err, sentinel) {
				t.Fatalf("Write returned %v, want sentinel", err)
			}
			f := <-frames
			if tc.writeFirst {
				if f.opcode != opBinary {
					t.Errorf("first frame opcode = %d, want opBinary", f.opcode)
				}
				f = <-frames
			}
			if f.opcode != opClose {
				t.Fatalf("frame opcode = %d, want opClose", f.opcode)
			}
			if code := CloseCode(binary.BigEndian.Uint16(f.payload)); code != CloseInternalError {
				t.Errorf("close code = %d, want %d (CloseInternalError)", code, CloseInternalError)
			}
		})
	}
}

// closeTrackingRwc wraps an io.ReadWriteCloser and records whether
// Close was invoked, for tests that need to confirm the conn tore down
// the underlying transport.
type closeTrackingRwc struct {
	io.ReadWriteCloser
	closed atomic.Bool
}

func (c *closeTrackingRwc) Close() error {
	c.closed.Store(true)
	return c.ReadWriteCloser.Close()
}

func TestConnCloseInvokesRWCClose(t *testing.T) {
	// Conn.Close must call rwc.Close so the application's single
	// Close releases the transport. (terminateRead/terminateWrite only mark the
	// per-half error; closing the transport always goes through terminateConn,
	// here via Conn.Close.)
	a, b := net.Pipe()
	wrapped := &closeTrackingRwc{ReadWriteCloser: a}
	c := newTestConn(t, wrapped)
	t.Cleanup(func() { _ = b.Close() })

	if err := c.Close(); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
	if !wrapped.closed.Load() {
		t.Error("rwc.Close was not invoked by Conn.Close")
	}
}

// halfReadCloseRwc returns io.EOF on Read but accepts Writes,
// simulating a peer that has half-closed its write side (TCP FIN
// received) while keeping its read side open. net.Pipe can't do this
// directly (peer.Close shuts both directions), so this fake stands
// in for the asymmetric-close case the conn must handle.
type halfReadCloseRwc struct {
	mu     sync.Mutex
	writes [][]byte
	closed bool
}

func (h *halfReadCloseRwc) Read(p []byte) (int, error) {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return 0, net.ErrClosed
	}
	return 0, io.EOF
}

func (h *halfReadCloseRwc) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, net.ErrClosed
	}
	h.writes = append(h.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (h *halfReadCloseRwc) Close() error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}

func TestReadEOFLeavesWriteSideOpen(t *testing.T) {
	// A peer that cleanly half-closes its write side (we see io.EOF
	// on the transport without a WebSocket Close frame) must only
	// terminate our read side. The write side stays usable so the
	// application can finish whatever it was sending before deciding
	// to release the connection via Close.
	rwc := &halfReadCloseRwc{}
	c := newTestConn(t, rwc)

	// Read surfaces the half-close as a CloseError carrying
	// CloseAbnormalClosure (no close frame was received).
	wantAbnormal := func(err error) {
		t.Helper()
		var ce *CloseError
		if !errors.As(err, &ce) || ce.Code != CloseAbnormalClosure {
			t.Fatalf("ReadMessage err = %v, want *CloseError with code %d", err, CloseAbnormalClosure)
		}
	}
	_, _, err := c.ReadMessage(nil)
	wantAbnormal(err)

	// Write still works. Peer's read side is still open.
	if err := c.WriteMessage(context.Background(), MessageText, nil, []byte("hello after EOF")); err != nil {
		t.Fatalf("WriteMessage after read EOF: %v", err)
	}
	rwc.mu.Lock()
	wrote := len(rwc.writes) > 0
	rwc.mu.Unlock()
	if !wrote {
		t.Error("no bytes written after read EOF; want a Text frame on the wire")
	}

	// Subsequent reads are sticky on the same abnormal-closure error.
	_, _, err = c.ReadMessage(nil)
	wantAbnormal(err)
}

func TestReadEOFKinds(t *testing.T) {
	// EOF placement decides the read error: at a frame boundary it is a
	// CloseError with CloseAbnormalClosure (the peer vanished without a
	// close frame); inside a frame (header or payload cut short) it is
	// io.ErrUnexpectedEOF.
	textFrame := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'} // unmasked Text "hello", fin
	cases := []struct {
		name  string
		wire  []byte
		first bool // a whole message precedes the EOF
		want  error
	}{
		{"boundary after message", textFrame, true, &CloseError{Code: CloseAbnormalClosure}},
		{"truncated header", []byte{0x81}, false, io.ErrUnexpectedEOF},
		{"truncated payload", []byte{0x81, 0x05, 'h', 'e'}, false, io.ErrUnexpectedEOF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestConn(t, &sinkRWC{r: bytes.NewReader(tc.wire)})
			if tc.first {
				if _, _, err := c.ReadMessage(nil); err != nil {
					t.Fatalf("ReadMessage before EOF: %v", err)
				}
			}
			_, _, err := c.ReadMessage(nil)
			if wantCE, ok := errors.AsType[*CloseError](tc.want); ok {
				var ce *CloseError
				if !errors.As(err, &ce) || ce.Code != wantCE.Code {
					t.Fatalf("ReadMessage err = %v, want *CloseError with code %d", err, wantCE.Code)
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("ReadMessage err = %v, want %v", err, tc.want)
			}
		})
	}
}

// payloadErrorRWC returns a complete payload with an allowed trailing error.
type payloadErrorRWC struct {
	header     []byte
	payload    []byte
	payloadErr error
	phase      int
}

func (r *payloadErrorRWC) Read(p []byte) (int, error) {
	switch r.phase {
	case 0:
		r.phase++
		return copy(p, r.header), nil
	case 1:
		r.phase++
		return copy(p, r.payload), r.payloadErr
	default:
		return 0, io.EOF
	}
}

func (r *payloadErrorRWC) Write(p []byte) (int, error) { return len(p), nil }
func (r *payloadErrorRWC) Close() error                { return nil }

func TestReadCompletePayloadWithError(t *testing.T) {
	// Exceed the internal buffer to expose the payload and error together.
	payload := bytes.Repeat([]byte("x"), 300)
	var header [maxHeaderLen]byte
	n := buildHeader(header[:], true, opBinary, true, len(payload), [4]byte{})
	payloadErr := errors.New("transport returned data with error")
	rwc := &payloadErrorRWC{
		header:     bytes.Clone(header[:n]),
		payload:    payload,
		payloadErr: payloadErr,
	}
	c := newTestConn(t, rwc)

	got, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage = %v, want complete message despite accompanying transport error", err)
	}
	if mt != MessageBinary || !bytes.Equal(got, payload) {
		t.Errorf("ReadMessage = (%v, %d bytes), want (Binary, %d bytes)", mt, len(got), len(payload))
	}

	_, _, err = c.ReadMessage(nil)
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != CloseAbnormalClosure {
		t.Errorf("next ReadMessage = %v, want CloseAbnormalClosure at the following frame boundary", err)
	}
}

func TestZeroMaxMessageSizeUnlimited(t *testing.T) {
	// maxMsg == 0 imposes no limit beyond the protocol's: a message
	// larger than the former 1 MiB default reads in full.
	payload := bytes.Repeat([]byte("x"), 1<<20+1)
	var hdr [maxHeaderLen]byte
	n := buildHeader(hdr[:], true, opBinary, true, len(payload), [4]byte{})
	wire := append(hdr[:n:n], payload...)
	c := newTestConn(t, &sinkRWC{r: bytes.NewReader(wire)}, withMaxMsg(0))
	msg, _, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if len(msg) != len(payload) {
		t.Errorf("ReadMessage len = %d, want %d", len(msg), len(payload))
	}
}

// errReadWriteRwc fails Read with readErr and Write with writeErr on
// every call; Close is a no-op. The distinct errors make it identifiable
// which half's reason a caller ends up reporting.
type errReadWriteRwc struct {
	readErr  error
	writeErr error
}

func (e *errReadWriteRwc) Read(p []byte) (int, error)  { return 0, e.readErr }
func (e *errReadWriteRwc) Write(p []byte) (int, error) { return 0, e.writeErr }
func (e *errReadWriteRwc) Close() error                { return nil }

func TestWriteFailureClosesConnAndReadReportsReason(t *testing.T) {
	// Once a frame write is attempted and fails, writeFrame tears the whole
	// connection down with that reason (closing the transport and terminating
	// both halves), so a parked or subsequent read reports why the connection
	// closed rather than its own transport error. writeControlFrame is the
	// simplest way to trigger a bare write attempt failure.
	readErr := errors.New("synthetic read failure")
	writeErr := errors.New("synthetic write failure")
	rwc := &errReadWriteRwc{readErr: readErr, writeErr: writeErr}
	c := newTestConn(t, rwc)

	if err := c.writeControlFrame(opPing, nil, c.frame.controlDeadline()); !errors.Is(err, writeErr) {
		t.Errorf("writeControlFrame err = %v, want %v", err, writeErr)
	}
	if _, _, err := c.ReadMessage(nil); !errors.Is(err, writeErr) {
		t.Errorf("ReadMessage err = %v, want %v (read reports the write failure that closed the connection)", err, writeErr)
	}
}

// TestRetainedHandlesRejected verifies the documented scope promise: once
// the Read/Write callback returns, every method on a retained Reader or
// Writer fails with ErrInvalidArgument, and the connection itself is
// untouched — it can never reach a later message.
func TestRetainedHandlesRejected(t *testing.T) {
	c, peer := connPair(t)
	go drainPeer(peer)

	go func() { _ = writePeerFrame(peer, true, opText, []byte("hi")) }()
	var r *Reader
	if err := c.Read(nil, func(rr *Reader) error { r = rr; return nil }); err != nil {
		t.Fatalf("Read: %v", err)
	}
	var w *Writer
	if err := c.Write(context.Background(), MessageText, nil, func(ww *Writer) error { w = ww; return nil }); err != nil {
		t.Fatalf("Write: %v", err)
	}

	calls := []struct {
		name string
		call func() error
	}{
		{"Reader.Read", func() error { _, err := r.Read(make([]byte, 1)); return err }},
		{"Reader.WriteTo", func() error { _, err := r.WriteTo(io.Discard); return err }},
		{"Reader.SetReadDeadline", func() error { return r.SetReadDeadline(time.Now()) }},
		{"Reader.Abort", func() error { return r.Abort(CloseNormalClosure, "late") }},
		{"Writer.Write", func() error { _, err := w.Write([]byte("x")); return err }},
		{"Writer.WriteString", func() error { _, err := w.WriteString("x"); return err }},
		{"Writer.ReadFrom", func() error { _, err := w.ReadFrom(strings.NewReader("x")); return err }},
		{"Writer.SetWriteDeadline", func() error { return w.SetWriteDeadline(time.Now()) }},
	}
	for _, tc := range calls {
		if err := tc.call(); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s on retained handle = %v, want ErrInvalidArgument", tc.name, err)
		}
	}

	// The connection is untouched: a fresh read still delivers the next
	// message.
	go func() { _ = writePeerFrame(peer, true, opText, []byte("again")) }()
	if data, _, err := c.ReadMessage(nil); err != nil || string(data) != "again" {
		t.Errorf("ReadMessage after retained-handle calls = %q, %v; want \"again\"", data, err)
	}
}

func TestWriterWriteAfterCloseReturnsScopeError(t *testing.T) {
	c, peer := connPair(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The buffered "hi" plus the close coalesce into one FIN frame.
		f := readPeerFrame(t, peer)
		if !f.fin || string(f.payload) != "hi" {
			t.Errorf("frame = %+v, want fin \"hi\"", f)
		}
	}()

	err := c.Write(context.Background(), MessageText, nil, func(w *Writer) error {
		if _, err := w.Write([]byte("hi")); err != nil {
			return err
		}
		if err := w.close(); err != nil {
			t.Errorf("inner close: %v", err)
		}
		// Subsequent Write must fail with errWriterScope.
		_, werr := w.Write([]byte("nope"))
		if !errors.Is(werr, errWriterScope) {
			t.Errorf("w.Write after close = %v, want errWriterScope", werr)
		}
		// Second close is a no-op.
		if err := w.close(); err != nil {
			t.Errorf("second close: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	<-done
}

// writeControlFrame is the internal entry point used by the read loop
// (Pong echoes, close echoes) and the keepalive ping; well-formed output
// is covered by the keepalive and close-handshake tests. This guards its
// size validation: control payloads are capped at 125 bytes (RFC 6455 §5.5).
func TestWriteControlFrameRejectsOversizedPayload(t *testing.T) {
	c, peer := connPair(t)
	go drainPeer(peer)
	err := c.writeControlFrame(opPing, make([]byte, 200), c.frame.controlDeadline())
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Errorf("writeControlFrame(oversized) = %v, want too-large rejection", err)
	}
}

func TestReadReturnsSameErrorAfterClose(t *testing.T) {
	c, peer := connPair(t)

	go func() {
		_ = writePeerFrame(peer, true, opClose, []byte{0x03, 0xe8, 'b', 'y', 'e'})
		_ = readPeerFrame(t, peer) // echo
	}()

	_, _, err1 := c.ReadMessage(nil)
	var ce1 *CloseError
	if !errors.As(err1, &ce1) {
		t.Fatalf("first ReadMessage: %v", err1)
	}

	_, _, err2 := c.ReadMessage(nil)
	// Subsequent ReadMessage should return the same stored error.
	if err2 != err1 {
		t.Errorf("second ReadMessage returned a different error: %v vs %v", err2, err1)
	}
}

func TestWriteTimeoutFiresWhenPeerStalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// With a 50 ms message deadline and no peer reader, the conn's rwc.Write
		// blocks and the frame watchdog must fire to tear down the connection.
		c, _ := connPair(t)
		err := c.WriteMessage(context.Background(), MessageText,
			&WriteOptions{MessageTimeout: 50 * time.Millisecond}, []byte("hello"))

		if !errors.Is(err, ErrTimeout) {
			t.Errorf("WriteMessage = %v, want ErrTimeout", err)
		}
	})
}

func TestWriteTimeoutBeforeFirstFrameTearsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// An expired message deadline must not enter an unbounded shutdown.
		c, _ := connPair(t, withShutdownTimeout(0))
		done := make(chan error, 1)
		go func() {
			done <- c.Write(context.Background(), MessageText,
				&WriteOptions{MessageTimeout: 50 * time.Millisecond}, func(*Writer) error {
					time.Sleep(100 * time.Millisecond)
					return nil
				})
		}()

		select {
		case err := <-done:
			if !errors.Is(err, ErrTimeout) {
				t.Errorf("Write = %v, want ErrTimeout", err)
			}
			if terminal := c.loadTerminalErr(); !errors.Is(terminal, ErrTimeout) {
				t.Errorf("terminal error = %v, want ErrTimeout", terminal)
			}
		case <-time.After(time.Second):
			t.Fatal("Write remained blocked sending a graceful close after its message timeout")
		}
	})
}

func TestReadFragmentsTooBig(t *testing.T) {
	// First fragment fits the limit but a subsequent continuation
	// pushes the cumulative size past MaxMessageSize.
	c, peer := connPair(t, withMaxMsg(20))

	go func() {
		// 10-byte first fragment, fin=false (10 ≤ 20, allowed).
		_ = writePeerFrame(peer, false, opBinary, make([]byte, 10))
		// Continuation header claiming 30 bytes. 10 + 30 > 20, so
		// readFragment must reject the message as too big. We don't
		// actually send the 30-byte payload. The conn rejects on the
		// header.
		_, _ = peer.Write([]byte{0x00, 30})
		drainPeer(peer)
	}()

	_, _, err := c.ReadMessage(nil)
	if !errors.Is(err, ErrProtocol) {
		t.Errorf("c.ReadMessage(nil) err = %v, want ErrProtocol", err)
	}
}

func TestCloseHandshakeTimeoutFiresWhenPeerDoesNotEcho(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// We Shutdown, but the peer reads our close and never echoes. The
		// shutdown timer (armed for the shutdown timeout) terminates both halves
		// with errShutdownTimeout and closes the transport, so the parked read
		// surfaces a meaningful ErrTimeout, not a raw transport error.
		c, peer := connPair(t, withShutdownTimeout(time.Second))

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = readPeerFrame(t, peer) // our close
			drainPeer(peer)
		}()

		if err := c.Shutdown(CloseNormalClosure, ""); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		_, _, err := c.ReadMessage(nil)
		if !errors.Is(err, ErrTimeout) {
			t.Errorf("c.ReadMessage(nil) err = %v, want ErrTimeout after the close-echo timeout", err)
		}
		<-done
	})
}

// TestShutdownTimeoutUnsentCloseTearsDown covers the other way the shutdown
// budget can expire: the close frame never reaches the wire because a blocked
// data write holds the frame lock for the whole budget. Shutdown must then
// tear the connection down (its contract is teardown on expiry), so the
// parked read and the blocked write both unblock with an ErrTimeout.
func TestShutdownTimeoutUnsentCloseTearsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := connPair(t, withShutdownTimeout(time.Second))

		// net.Pipe is unbuffered and the peer does not read, so this write
		// parks inside rwc.Write holding the frame lock.
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- c.WriteMessage(context.Background(), MessageText, nil, []byte("blocked"))
		}()
		synctest.Wait() // let the write reach rwc.Write holding the lock

		readDone := make(chan error, 1)
		go func() {
			_, _, err := c.ReadMessage(nil)
			readDone <- err
		}()

		if err := c.Shutdown(CloseNormalClosure, ""); !errors.Is(err, ErrTimeout) {
			t.Errorf("Shutdown = %v, want ErrTimeout", err)
		}
		if err := <-readDone; !errors.Is(err, ErrTimeout) {
			t.Errorf("parked read = %v, want ErrTimeout", err)
		}
		if err := <-writeDone; !errors.Is(err, ErrTimeout) {
			t.Errorf("blocked write = %v, want ErrTimeout", err)
		}
	})
}

// TestReadCallbackPanicReleasesReader checks that a panicking Read callback
// tears the connection down and releases the reader as the panic propagates:
// a caller that recovers (as net/http does for handler panics) sees the
// teardown reason on the next read, not a wedged reader.
func TestReadCallbackPanicReleasesReader(t *testing.T) {
	c, peer := connPair(t)
	go func() {
		_ = writePeerFrame(peer, true, opText, []byte("boom"))
	}()

	func() {
		defer func() {
			if v := recover(); v != "boom" {
				t.Errorf("recovered %v, want the callback's panic value", v)
			}
		}()
		_ = c.Read(nil, func(*Reader) error { panic("boom") })
	}()

	if _, _, err := c.ReadMessage(nil); !errors.Is(err, errCallbackPanic) {
		t.Errorf("ReadMessage after panic = %v, want errCallbackPanic", err)
	}
}

// TestWriteCallbackPanicReleasesWriter is the write-side counterpart: the
// panic must release the writer without flushing a truncated final frame,
// and later writes must report the teardown reason rather than block on
// writer.mu.
func TestWriteCallbackPanicReleasesWriter(t *testing.T) {
	c, _ := connPair(t)

	func() {
		defer func() {
			if v := recover(); v != "boom" {
				t.Errorf("recovered %v, want the callback's panic value", v)
			}
		}()
		_ = c.Write(context.Background(), MessageText, nil, func(*Writer) error { panic("boom") })
	}()

	if got := c.loadTerminalErr(); !errors.Is(got, errCallbackPanic) {
		t.Errorf("terminalErr = %v, want errCallbackPanic", got)
	}
	err := c.WriteMessage(context.Background(), MessageText, nil, []byte("x"))
	if !errors.Is(err, errCallbackPanic) {
		t.Errorf("WriteMessage after panic = %v, want errCallbackPanic", err)
	}
}

func TestPongStillSentDuringCloseHandshake(t *testing.T) {
	// RFC 6455 §5.5.2: a Pong MUST be sent in response to a Ping
	// unless the peer's close has already been received. Specifically,
	// a Ping that arrives between our Shutdown and the peer's echoed
	// close still needs to be answered.
	c, peer := connPair(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Read our close (sent via Shutdown).
		f1 := readPeerFrame(t, peer)
		if f1.opcode != opClose {
			t.Errorf("expected our close, got %+v", f1)
			return
		}
		// Send a ping before echoing the close.
		if err := writePeerFrame(peer, true, opPing, []byte("p")); err != nil {
			t.Errorf("peer write ping: %v", err)
			return
		}
		// We expect a Pong even though we're mid-close-handshake.
		f2 := readPeerFrame(t, peer)
		if f2.opcode != opPong || string(f2.payload) != "p" {
			t.Errorf("expected Pong with payload \"p\", got %+v", f2)
		}
		// Now echo the close so the conn finishes the handshake.
		_ = writePeerFrame(peer, true, opClose, f1.payload)
	}()

	if err := c.Shutdown(CloseNormalClosure, "bye"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	_, _, err := c.ReadMessage(nil)
	var ce *CloseError
	if !errors.As(err, &ce) {
		t.Errorf("ReadMessage = %v, want *CloseError", err)
	}
	<-done
}

func TestReaderBlockedOnFrameMuKilledByWriteTimer(t *testing.T) {
	// If a user write is stuck holding the frame lock (peer not reading)
	// and the reader receives a Ping that needs a Pong response, the
	// reader will block on the frame lock. The frame timer armed by the
	// stuck user write must fire and kill the connection so the reader
	// unblocks.
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t, withWriteTimeout(60*time.Millisecond))

		userWrite := make(chan error, 1)
		go func() {
			// Peer never drains this; rwc.Write will block until the frame
			// watchdog (the message deadline) fires and tears the conn down.
			userWrite <- c.WriteMessage(context.Background(), MessageText,
				&WriteOptions{MessageTimeout: 60 * time.Millisecond}, []byte("stuck"))
		}()
		synctest.Wait() // let the write reach rwc.Write holding the frame lock

		// Peer sends a Ping. The reader will read it, try to send a Pong,
		// and block on the frame lock held by the user write above.
		go func() { _ = writePeerFrame(peer, true, opPing, []byte("p")) }()

		// The reader ignores the pong reply's timeout (benign) and then fails
		// on the transport the frame watchdog closed; what matters is that it
		// unblocks with an error rather than parking on the frame lock forever.
		_, _, err := c.ReadMessage(nil)
		if err == nil {
			t.Error("ReadMessage err = nil, want an error after teardown")
		}
		if werr := <-userWrite; !errors.Is(werr, ErrTimeout) {
			t.Errorf("user WriteMessage err = %v, want ErrTimeout", werr)
		}
	})
}

// errRead returns the stored read error, non-nil exactly when the read side is
// terminal. reader.terminalErr is guarded by reader.mu; acquire it (the test calls
// this with no read in progress).
func (c *Conn) errRead() error {
	c.reader.mu <- struct{}{}
	defer func() { <-c.reader.mu }()
	return c.reader.terminalErr
}

func TestContendedFrameLockTimesOutWithoutTeardown(t *testing.T) {
	// A write that cannot acquire frameMu within its budget must return
	// errWriteTimeout and leave the connection untouched: nothing went on the
	// wire, so the conn stays usable. This is distinct from a mid-write stall
	// (TestWriteTimeoutFiresWhenPeerStalls), where rwc.Write itself overruns
	// its deadline and the frame watchdog tears the transport down.
	// A 50ms control write timeout keeps the contended lock wait short.
	c, peer := connPair(t, withWriteTimeout(50*time.Millisecond))

	// Occupy frameMu as if another writer held it (frameMu is a 1-slot channel
	// used as a mutex). A real holder would be parked in rwc.Write; taking the
	// slot directly is equivalent for the contention path and avoids arming
	// that holder's own frame timer, which would muddy the assertion.
	c.frame.mu <- struct{}{}

	// writeControlFrame is bounded by the control write timeout; 50ms can't
	// win the lock.
	err := c.writeControlFrame(opPing, []byte("blocked"), c.frame.controlDeadline())
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("contended writeControlFrame = %v, want ErrTimeout", err)
	}
	// The FSM is untouched: write half still open, read half not terminal.
	if c.frame.closeSent {
		t.Error("close marked sent after a contended lock timeout")
	}
	// The test still holds frame.mu (occupied above), so read the frame-guarded
	// terminal error directly rather than re-acquiring.
	if werr := c.frame.terminalErr; werr != nil {
		t.Errorf("write half dead (%v) after a contended lock timeout", werr)
	}
	if rerr := c.errRead(); rerr != nil {
		t.Errorf("read half terminal (%v) after a contended lock timeout", rerr)
	}

	// Release the lock; the connection must still be fully usable.
	<-c.frame.mu

	got := make(chan peerFrame, 1)
	go func() { got <- readPeerFrame(t, peer) }()
	if err := c.WriteMessage(context.Background(), MessageText, nil, []byte("still alive")); err != nil {
		t.Fatalf("WriteMessage after recovered contention = %v, want nil", err)
	}
	if f := <-got; f.opcode != opText || !f.fin || string(f.payload) != "still alive" {
		t.Errorf("recovered frame = %+v, want Text FIN \"still alive\"", f)
	}
}

func TestFailedPongReplyDisarmsReadWatchdog(t *testing.T) {
	// A pong-reply failure that aborts nextReader (here a transport
	// failure, which unlike a timeout does propagate) must not leak an
	// armed read watchdog: after Read returns there is no read in progress,
	// so a later fire would tear down the connection with nobody reading.
	c, peer := connPair(t)

	// Deliver a ping, then close the peer side so the pong reply fails
	// with a transport error.
	go func() {
		_ = writePeerFrame(peer, true, opPing, []byte("p"))
		peer.Close()
	}()

	_, _, err := c.ReadMessage(&ReadOptions{IdleTimeout: time.Hour})
	if err == nil || errors.Is(err, ErrTimeout) {
		t.Fatalf("ReadMessage err = %v, want a transport error", err)
	}
	m := c.lockedMonitor()
	armed := !m.fireAt.IsZero()
	m.unlock()
	c.reader.mu <- struct{}{}
	terminal := c.reader.terminalErr != nil
	<-c.reader.mu
	if armed && !terminal {
		t.Error("read watchdog left armed with no read in progress")
	}
}

func TestTimersStoppedAfterClose(t *testing.T) {
	// Close must leave no AfterFunc timer pending: the read watchdog, the frame
	// timer, and the shutdown timer. A timer left armed would fire its callback on
	// an already-dead connection and, worse, keep the Conn reachable from the
	// runtime timer heap until it fires (up to an hour for the watchdog),
	// defeating GC. terminateConn stops the shutdown timer directly but does not
	// touch the watchdog, which relies on the read exit (endRead) to disarm,
	// so arm it via a parked read first, to prove that path runs.
	c, _ := connPair(t)

	readDone := make(chan error, 1)
	go func() {
		readDone <- c.Read(&ReadOptions{IdleTimeout: time.Hour}, func(*Reader) error { return nil })
	}()

	// Wait until the parked read has armed the watchdog.
	deadline := time.Now().Add(2 * time.Second)
	for {
		m := c.lockedMonitor()
		armed := !m.fireAt.IsZero()
		m.unlock()
		if armed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("read watchdog never armed")
		}
		time.Sleep(time.Millisecond)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-readDone // the read exits and disarms the watchdog before returning

	m := c.lockedMonitor()
	fireAt := m.fireAt
	m.unlock()
	if !fireAt.IsZero() {
		t.Errorf("read watchdog still armed after Close (fireAt = %v)", fireAt)
	}
	// Timer.Stop reports false when the timer is already stopped or fired, i.e.
	// not pending; it doubles as cleanup, since nothing should fire after it.
	if c.monitor.timer.Stop() {
		t.Error("read watchdog timer still pending after Close")
	}
	if c.frame.timer.Stop() {
		t.Error("frame timer still pending after Close")
	}
	if c.shutdownTimer.Stop() {
		t.Error("shutdown timer still pending after Close")
	}
}

func TestPongReplyTimeoutDoesNotFailRead(t *testing.T) {
	// A pong reply that cannot win the frame lock within the control-write
	// budget is benign: nothing reached the wire and the peer is not
	// necessarily waiting for it. The read must carry on and deliver the
	// next message rather than fail.
	c, peer := connPair(t, withWriteTimeout(50*time.Millisecond))

	// Occupy frame.mu so the pong reply times out with nothing on the wire.
	c.frame.mu <- struct{}{}
	defer func() { <-c.frame.mu }()

	go func() {
		_ = writePeerFrame(peer, true, opPing, []byte("p"))
		_ = writePeerFrame(peer, true, opText, []byte("data"))
	}()

	data, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage err = %v, want nil (pong timeout is benign)", err)
	}
	if mt != MessageText || string(data) != "data" {
		t.Errorf("got %v %q, want TEXT \"data\"", mt, data)
	}
}

func TestCloseEchoFailureStillReportsCloseError(t *testing.T) {
	// The echo's outcome never changes the read result: even when the echo
	// hits a dead transport, the read that consumed the peer's close (and
	// every later one) reports the CloseError: the peer's close code is
	// more useful to the application than the echo's transport error.
	c, peer := connPair(t)

	go func() {
		_ = writePeerFrame(peer, true, opClose, buildClosePayload(CloseNormalClosure, "bye"))
		peer.Close()
	}()

	for i := range 2 {
		_, _, err := c.ReadMessage(nil)
		var ce *CloseError
		if !errors.As(err, &ce) || ce.Code != CloseNormalClosure || ce.Text != "bye" {
			t.Errorf("ReadMessage #%d err = %v, want CloseError(1000, \"bye\")", i+1, err)
		}
	}
}

func TestWriteCallbackErrorParkedReadTerminates(t *testing.T) {
	// Conn.Write's contract on a callback error: either the transport is closed
	// and the reader is unblocked, or a graceful shutdown is in progress and the
	// reader terminates. Here the transport is healthy, so Write starts a close
	// handshake; a parked read terminates with the CloseError once the peer
	// echoes the close.
	c, peer := connPair(t, withBufSize(minWriteBufferSize))

	// Peer drains frames and echoes the close so the handshake completes.
	go func() {
		for {
			f := readPeerFrame(t, peer)
			if f.opcode == opClose {
				_ = writePeerFrame(peer, true, opClose, f.payload)
				return
			}
		}
	}()

	readErr := make(chan error, 1)
	go func() {
		_, _, err := c.ReadMessage(nil)
		readErr <- err
	}()
	// Let the reader park in the transport read.
	time.Sleep(10 * time.Millisecond)

	boom := errors.New("boom")
	err := c.Write(context.Background(), MessageBinary, nil, func(w *Writer) error {
		// 100 B overflows the 64 B staging buffer: a frame goes out.
		if _, err := w.Write(make([]byte, 100)); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Write err = %v, want %v", err, boom)
	}
	select {
	case err := <-readErr:
		var ce *CloseError
		if !errors.As(err, &ce) {
			t.Errorf("parked read err = %v, want *CloseError from the handshake", err)
		} else if ce.Code != CloseInternalError {
			t.Errorf("parked read close code = %d, want %d (CloseInternalError)", ce.Code, CloseInternalError)
		}
	case <-time.After(5 * time.Second):
		t.Error("parked read did not terminate after graceful shutdown")
	}
}

func TestShutdownAfterEchoConsumedClosesCleanly(t *testing.T) {
	// Shutdown can lose the race with the peer's close echo: the read goroutine may
	// consume the echo (reader.terminalErr = CloseError) before Shutdown arms the
	// close-echo timer. Shutdown arms unconditionally and only consults the
	// connection-wide terminalErr (still nil after a clean read-half close), so it
	// does arm, but the app's Close then tears the connection down with ErrClosed
	// and stops the timer, so no bogus errShutdownTimeout fires.
	c, peer := connPair(t, withShutdownTimeout(time.Hour))
	go drainPeer(peer) // consume the close frame

	c.terminateRead(&CloseError{Code: CloseNormalClosure})
	if err := c.Shutdown(CloseNormalClosure, "bye"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Close won the teardown: the connection-wide reason is ErrClosed, never a bogus
	// shutdown timeout, and Close stopped the timer (Stop reports it inactive).
	if got := c.loadTerminalErr(); !errors.Is(got, ErrClosed) {
		t.Errorf("terminalErr = %v, want ErrClosed", got)
	}
	if c.shutdownTimer.Stop() {
		t.Error("close-echo timer still armed after Close")
	}
}

// TestWriteRejectsInvalidArgument checks that a write rejects a bad argument
// (an invalid message type or conflicting options) with ErrInvalidArgument
// before touching the connection: the specific reason is reported, nothing
// reaches the wire, and the connection stays usable, so a valid write still
// succeeds afterward. (The zero value MessageType is MessageBinary, which is
// legal, so a forgotten type is not rejected.)
func TestWriteRejectsInvalidArgument(t *testing.T) {
	for _, tc := range []struct {
		name string
		mt   MessageType
		opts *WriteOptions
		want error
	}{
		{"ControlOpcodeType", MessageType(opClose), nil, errInvalidMessageType},
		{"ArbitraryType", MessageType(99), nil, errInvalidMessageType},
		{"ConflictingOptions", MessageText, &WriteOptions{MessageTimeout: time.Second, MinWriteRate: 1000}, errWriteOptsConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)

			err := c.WriteMessage(context.Background(), tc.mt, tc.opts, []byte("x"))
			if !errors.Is(err, tc.want) {
				t.Fatalf("WriteMessage err = %v, want %v", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("WriteMessage err = %v, want it to wrap ErrInvalidArgument", err)
			}

			// The connection is untouched and retryable: a valid write still
			// succeeds and the peer receives exactly it.
			done := make(chan peerFrame, 1)
			go func() { done <- readPeerFrame(t, peer) }()
			if err := c.WriteMessage(context.Background(), MessageText, nil, []byte("ok")); err != nil {
				t.Fatalf("WriteMessage after rejection: %v", err)
			}
			if f := <-done; f.opcode != opText || string(f.payload) != "ok" {
				t.Errorf("peer got %+v, want text %q", f, "ok")
			}
		})
	}
}

// TestReadRejectsInvalidArgument mirrors TestWriteRejectsInvalidArgument for the
// read side: a conflicting ReadOptions is rejected with ErrInvalidArgument
// before touching the connection, and the connection stays usable, so a valid
// read still succeeds afterward.
func TestReadRejectsInvalidArgument(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *ReadOptions
		want error
	}{
		{"ConflictingOptions", &ReadOptions{MessageTimeout: time.Second, MinReadRate: 1000}, errReadOptsConflict},
		{"TimeoutWithoutInterval", &ReadOptions{KeepaliveTimeout: time.Second}, errKeepaliveTimeoutWithoutPing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)

			if _, _, err := c.ReadMessage(tc.opts); !errors.Is(err, tc.want) {
				t.Fatalf("ReadMessage err = %v, want %v", err, tc.want)
			} else if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("ReadMessage err = %v, want it to wrap ErrInvalidArgument", err)
			}

			// The connection is untouched and retryable: a valid read still
			// succeeds and receives the peer's message.
			go func() { _ = writePeerFrame(peer, true, opText, []byte("ok")) }()
			data, mt, err := c.ReadMessage(nil)
			if err != nil {
				t.Fatalf("ReadMessage after rejection: %v", err)
			}
			if mt != MessageText || string(data) != "ok" {
				t.Errorf("got (%v, %q), want (Text, %q)", mt, data, "ok")
			}
		})
	}
}

// writeRecipes hand the same payload to a Writer in different shapes: one
// call or many, bytes or strings or a reader, chunk sizes that straddle
// buffer boundaries, and Final marked or not. Whatever the shape, the peer
// must reassemble exactly the payload.
var writeRecipes = []struct {
	name  string
	write func(w *Writer, p []byte) error
}{
	{"OneWrite", func(w *Writer, p []byte) error {
		_, err := w.Write(p)
		return err
	}},
	{"FinalThenOneWrite", func(w *Writer, p []byte) error {
		w.Final()
		_, err := w.Write(p)
		return err
	}},
	{"SmallByteChunks", func(w *Writer, p []byte) error {
		for len(p) > 0 {
			n := min(7, len(p))
			if _, err := w.Write(p[:n]); err != nil {
				return err
			}
			p = p[n:]
		}
		return nil
	}},
	{"SmallStringChunks", func(w *Writer, p []byte) error {
		for len(p) > 0 {
			n := min(5, len(p))
			if _, err := w.WriteString(string(p[:n])); err != nil {
				return err
			}
			p = p[n:]
		}
		return nil
	}},
	{"MixedBytesAndStrings", func(w *Writer, p []byte) error {
		for i := 0; len(p) > 0; i++ {
			n := min(7, len(p))
			var err error
			if i%2 == 0 {
				_, err = w.Write(p[:n])
			} else {
				_, err = io.WriteString(w, string(p[:n])) // the io.StringWriter path
			}
			if err != nil {
				return err
			}
			p = p[n:]
		}
		return nil
	}},
	{"OverflowingByteChunks", func(w *Writer, p []byte) error {
		// Each 100-byte chunk overflows the minimum staging buffer.
		for len(p) > 0 {
			n := min(100, len(p))
			if _, err := w.Write(p[:n]); err != nil {
				return err
			}
			p = p[n:]
		}
		return nil
	}},
	{"FinalBeforeLastChunk", func(w *Writer, p []byte) error {
		for len(p) > 7 {
			if _, err := w.Write(p[:7]); err != nil {
				return err
			}
			p = p[7:]
		}
		w.Final()
		_, err := w.Write(p)
		return err
	}},
	{"ReadFrom", func(w *Writer, p []byte) error {
		_, err := w.ReadFrom(bytes.NewReader(p))
		return err
	}},
}

// TestWriteRecipesReassemble writes one fixed payload via every recipe in
// writeRecipes, under a buffer that coalesces everything and one that
// fragments heavily. Frame boundaries are the recipe's and the buffer's
// business; the invariants are the frame shape (message opcode first,
// continuations after) and that the frame payloads concatenate back to the
// original.
func TestWriteRecipesReassemble(t *testing.T) {
	payload := make([]byte, 1000) // ragged against the 5/7/100-byte chunks and both buffers
	for i := range payload {
		payload[i] = byte(i)
	}
	bufSizes := []struct {
		name string
		size int
	}{
		{"DefaultBuffer", 0},              // 4 KiB: everything coalesces
		{"MinBuffer", minWriteBufferSize}, // 64 B: everything fragments
	}
	for _, r := range writeRecipes {
		t.Run(r.name, func(t *testing.T) {
			for _, bs := range bufSizes {
				t.Run(bs.name, func(t *testing.T) {
					var opts []func(*connConfig)
					if bs.size != 0 {
						opts = append(opts, withBufSize(bs.size))
					}
					c, peer := connPair(t, opts...)

					done := make(chan struct{})
					var frames []peerFrame
					go func() {
						defer close(done)
						for {
							f := readPeerFrame(t, peer)
							frames = append(frames, f)
							if f.fin {
								return
							}
						}
					}()

					err := c.Write(context.Background(), MessageBinary, nil, func(w *Writer) error {
						return r.write(w, payload)
					})
					if err != nil {
						t.Fatalf("Write: %v", err)
					}
					<-done

					// The collector stops at the FIN frame, so FIN-on-last is
					// structural; a premature FIN surfaces below as a short
					// reassembled message.
					if frames[0].opcode != opBinary {
						t.Errorf("first frame opcode = %d, want opBinary", frames[0].opcode)
					}
					for i, f := range frames[1:] {
						if f.opcode != opContinuation {
							t.Errorf("frame %d opcode = %d, want opContinuation", i+1, f.opcode)
						}
					}
					var got []byte
					for _, f := range frames {
						got = append(got, f.payload...)
					}
					if !bytes.Equal(got, payload) {
						t.Errorf("reassembled %d bytes, want %d", len(got), len(payload))
					}
				})
			}
		})
	}
}
