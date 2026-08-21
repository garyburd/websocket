package websocket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func TestReadOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		o       *ReadOptions
		wantErr bool
	}{
		{"nil", nil, false},
		{"empty", &ReadOptions{}, false},
		{"interval and timeout", &ReadOptions{KeepaliveInterval: time.Second, KeepaliveTimeout: time.Second}, false},
		{"soft keepalive (interval, no timeout)", &ReadOptions{KeepaliveInterval: time.Second}, false},
		{"deadline only", &ReadOptions{MessageTimeout: time.Second}, false},
		{"rate only", &ReadOptions{MinReadRate: 1000}, false},
		{"timeout without interval", &ReadOptions{KeepaliveTimeout: time.Second}, true},
		{"deadline and rate", &ReadOptions{MessageTimeout: time.Second, MinReadRate: 1000}, true},
		{"negative idle timeout", &ReadOptions{IdleTimeout: -1}, true},
		{"negative keepalive interval", &ReadOptions{KeepaliveInterval: -1}, true},
		{"negative keepalive timeout", &ReadOptions{KeepaliveTimeout: -1}, true},
		{"negative message timeout", &ReadOptions{MessageTimeout: -1}, true},
		{"negative read rate", &ReadOptions{MinReadRate: -1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.o.validate(); (err != nil) != tc.wantErr {
				t.Errorf("validate() = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestWriteOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		o       *WriteOptions
		wantErr bool
	}{
		{"nil", nil, false},
		{"empty", &WriteOptions{}, false},
		{"deadline only", &WriteOptions{MessageTimeout: time.Second}, false},
		{"rate only", &WriteOptions{MinWriteRate: 1000}, false},
		{"deadline and rate", &WriteOptions{MessageTimeout: time.Second, MinWriteRate: 1000}, true},
		{"negative message timeout", &WriteOptions{MessageTimeout: -1}, true},
		{"negative write rate", &WriteOptions{MinWriteRate: -1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.o.validate(); (err != nil) != tc.wantErr {
				t.Errorf("validate() = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestWriteCtxCancelBeforeAcquire(t *testing.T) {
	c, peer := connPair(t)

	// Occupy the writer lock so the next Write must wait to acquire it.
	c.writer.mu <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.WriteMessage(ctx, MessageText, nil, []byte("x"))
	if !errors.Is(err, errWriteUnstarted) {
		t.Errorf("cancelled ctx: err = %v, want errWriteUnstarted", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled", err)
	}

	// The acquire wait is non-destructive: releasing the lock leaves the
	// connection fully usable.
	<-c.writer.mu
	done := make(chan struct{})
	go func() { defer close(done); _ = readPeerFrame(t, peer) }()
	if err := c.WriteMessage(context.Background(), MessageText, nil, []byte("ok")); err != nil {
		t.Errorf("WriteMessage after cancelled acquire = %v, want nil", err)
	}
	<-done
}

func TestConcurrentReadRejected(t *testing.T) {
	c, peer := connPair(t)

	// Occupy the reader lock to simulate a read already in progress. A
	// connection has a single read loop, so a second concurrent Read is a
	// caller bug: it fails fast rather than blocking behind the in-flight read.
	c.reader.mu <- struct{}{}

	_, _, err := c.ReadMessage(nil)
	if !errors.Is(err, errConcurrentRead) {
		t.Errorf("concurrent read: err = %v, want errConcurrentRead", err)
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("err = %v, want it to wrap ErrInvalidArgument", err)
	}

	// The rejection is non-destructive: releasing the lock leaves the
	// connection fully usable.
	<-c.reader.mu
	go func() { _ = writePeerFrame(peer, true, opText, []byte("ok")) }()
	data, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage after releasing the reader: %v", err)
	}
	if mt != MessageText || string(data) != "ok" {
		t.Errorf("mt=%v data=%q, want Text/ok", mt, data)
	}
}

func TestMatchPongRejectsNonMatchingControl(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t)
		opts := &ReadOptions{
			KeepaliveInterval: 40 * time.Millisecond,
			KeepaliveTimeout:  100 * time.Millisecond,
			MatchPong:         true,
		}
		go func() {
			f := readPeerFrame(t, peer) // our keepalive ping
			if f.opcode == opPing {
				// A pong carrying a different payload does not echo our ping,
				// so with MatchPong it must NOT satisfy the wait.
				_ = writePeerFrame(peer, true, opPong, []byte("nope"))
			}
			drainPeer(peer)
		}()
		if _, _, err := c.ReadMessage(opts); !errors.Is(err, ErrTimeout) {
			t.Errorf("ReadMessage = %v, want ErrTimeout (non-matching pong must not reset)", err)
		}
	})
}

func TestSoftKeepaliveNoPongRequired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t)
		opts := &ReadOptions{KeepaliveInterval: 40 * time.Millisecond} // KeepaliveTimeout 0 → soft
		go func() {
			// The conn pings on cadence; we never pong, and soft keepalive must
			// not tear the connection down. After a few pings, send data.
			for i := range 3 {
				if f := readPeerFrame(t, peer); f.opcode != opPing {
					t.Errorf("frame %d opcode = %d, want ping", i, f.opcode)
					return
				}
			}
			_ = writePeerFrame(peer, true, opText, []byte("ok"))
		}()
		data, mt, err := c.ReadMessage(opts)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if mt != MessageText || string(data) != "ok" {
			t.Errorf("mt=%v data=%q, want Text/ok", mt, data)
		}
	})
}

// TestSoftKeepaliveMissedSendNotFatal verifies that when a soft-keepalive ping
// cannot be placed on the wire in time (here a blocked application write holds
// the frame lock for the whole interval) the connection is not torn down: the
// ping is skipped and the connection stays usable.
func TestSoftKeepaliveMissedSendNotFatal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t)
		opts := &ReadOptions{KeepaliveInterval: 40 * time.Millisecond} // soft: no KeepaliveTimeout

		// net.Pipe is unbuffered and the peer does not read, so this WriteMessage
		// parks inside rwc.Write holding the frame lock; every keepalive ping that
		// fires while it is parked will fail to acquire the lock and time out.
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- c.WriteMessage(context.Background(), MessageText, nil, []byte("blocked"))
		}()
		synctest.Wait() // let the write reach rwc.Write holding the lock

		go func() {
			// Several intervals pass with the lock held, so the soft ping send
			// repeatedly times out. A fatal send timeout would tear the connection
			// down here; soft keepalive must survive. Then prove it is still usable
			// by sending data, and drain the held write so its goroutine finishes.
			time.Sleep(200 * time.Millisecond)
			_ = writePeerFrame(peer, true, opText, []byte("ok"))
			drainPeer(peer)
		}()

		data, mt, err := c.ReadMessage(opts)
		if err != nil {
			t.Fatalf("ReadMessage: %v (a soft-keepalive send timeout must not be fatal)", err)
		}
		if mt != MessageText || string(data) != "ok" {
			t.Errorf("mt=%v data=%q, want Text/ok", mt, data)
		}
		if err := <-writeDone; err != nil {
			t.Errorf("WriteMessage: %v", err)
		}
	})
}

func TestSetReadDeadlineBoundsBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t)
		defer peer.Close()
		go func() {
			_, _ = peer.Write([]byte{0x82, 0x64}) // binary FIN, 100 bytes; no payload follows
			drainPeer(peer)
		}()
		err := c.Read(&ReadOptions{}, func(r *Reader) error {
			if e := r.SetReadDeadline(time.Now().Add(40 * time.Millisecond)); e != nil {
				return e
			}
			_, e := io.ReadAll(r)
			return e
		})
		if !errors.Is(err, ErrTimeout) {
			t.Errorf("Read = %v, want ErrTimeout", err)
		}
	})
}

func TestMessageTimeoutStopsAtBodyEnd(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"consumed", []byte("complete")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, peer := connPair(t)
				defer peer.Close()
				go func() {
					_ = writePeerFrame(peer, true, opText, tc.payload)
				}()

				err := c.Read(&ReadOptions{MessageTimeout: 50 * time.Millisecond}, func(r *Reader) error {
					if _, err := io.ReadAll(r); err != nil {
						return err
					}
					// MessageTimeout bounds body I/O, not application work after
					// the complete body has been consumed.
					synctest.Sleep(100 * time.Millisecond)
					return nil
				})
				if err != nil {
					t.Fatalf("Read: %v", err)
				}
				if err := c.loadTerminalErr(); err != nil {
					t.Fatalf("terminal error after complete body = %v", err)
				}
			})
		})
	}

	t.Run("buffered unread", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			payload := []byte("complete")
			var hdr [maxHeaderLen]byte
			n := buildHeader(hdr[:], true, opText, true, len(payload), [4]byte{})
			wire := append(hdr[:n:n], payload...)
			c := newTestConn(t, &sinkRWC{r: bytes.NewReader(wire)})

			err := c.Read(&ReadOptions{MessageTimeout: 50 * time.Millisecond}, func(*Reader) error {
				if got := c.br.Buffered(); got < len(payload) {
					t.Fatalf("buffered payload = %d bytes, want at least %d", got, len(payload))
				}
				// The payload is already buffered from the transport but remains
				// unread by the application.
				synctest.Sleep(100 * time.Millisecond)
				return nil
			})
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if err := c.loadTerminalErr(); err != nil {
				t.Fatalf("terminal error after buffered body = %v", err)
			}
		})
	})
}

func TestRateAllowanceDoesNotOverflow(t *testing.T) {
	if got, want := rateAllowance(1_000, 1_500), 1500*time.Millisecond; got != want {
		t.Fatalf("rateAllowance(1000, 1500) = %v, want %v", got, want)
	}
	if strconv.IntSize < 64 {
		t.Skip("large frame sizes require a 64-bit int")
	}

	// The intermediate product overflows int64; the quotient is representable.
	const largeN, largeRate int64 = 10_000_000_000, 20_000_000_000
	if got, want := rateAllowance(int(largeRate), int(largeN)), 500*time.Millisecond; got != want {
		t.Errorf("large rateAllowance = %v, want %v", got, want)
	}

	// Unrepresentable allowances saturate.
	if got, want := rateAllowance(1, int(largeN)), time.Duration(1<<63-1); got != want {
		t.Errorf("saturating rateAllowance = %v, want %v", got, want)
	}
}

// TestMinReadRatePacing checks that MinReadRate is a progress floor, not a
// fixed deadline: a body arriving above the floor keeps rolling the deadline
// forward and succeeds even though the transfer far outlives the initial
// 1-second grace, while one whose progress stops mid-body times out. Reading
// via ReadMessage matters: io.ReadAll's buffers grow far past one grace
// window's worth of the floor (rate × 1 s), so this passes only because each
// fill is budgeted the requested bytes' allowance, not just the grace window.
func TestMinReadRatePacing(t *testing.T) {
	const total = 20000
	cases := []struct {
		name    string
		chunks  int // 500-byte payload chunks delivered, one per 250 ms (2000 B/s)
		wantErr error
	}{
		{"SteadyAboveFloor", total / 500, nil}, // the whole body over 10 s
		{"StallsMidBody", 4, ErrTimeout},       // 2 KB arrives, then silence
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, peer := connPair(t)
				go func() {
					var hdr [maxHeaderLen]byte
					n := buildHeader(hdr[:], true, opBinary, true, total, [4]byte{})
					if _, err := peer.Write(hdr[:n]); err != nil {
						return
					}
					chunk := make([]byte, 500)
					for range tc.chunks {
						time.Sleep(250 * time.Millisecond)
						if _, err := peer.Write(chunk); err != nil {
							return
						}
					}
				}()

				data, _, err := c.ReadMessage(&ReadOptions{MinReadRate: 1000})
				if tc.wantErr != nil {
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("ReadMessage err = %v, want %v", err, tc.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("ReadMessage: %v", err)
				}
				if len(data) != total {
					t.Errorf("read %d bytes, want %d", len(data), total)
				}
			})
		})
	}
}

// TestMinWriteRatePacing is the write-side mirror of TestMinReadRatePacing:
// MinWriteRate is a progress floor, not a fixed deadline. Each frame's
// deadline extends by its payload's allowance at the floor, so both a
// multi-frame message and a single frame far larger than rate × grace outlive
// the initial 1-second grace while the peer drains above the floor; a peer
// that stops draining — mid-message or from the start — times the write out.
func TestMinWriteRatePacing(t *testing.T) {
	// streamed goes out as ten 498-byte frames (8-byte headers) plus a
	// 20-byte final frame (6-byte header); single is one 20000-byte frame
	// (8-byte header masked / 4-byte unmasked): chunked through the 512-byte
	// buffer for masking on the client, one large write on the server.
	const streamedWire = 10*(498+8) + (20 + 6)
	streamed := func(c *Conn) error {
		return c.Write(context.Background(), MessageBinary,
			&WriteOptions{MinWriteRate: 500}, func(w *Writer) error {
				chunk := make([]byte, 500)
				for range 10 {
					if _, err := w.Write(chunk); err != nil {
						return err
					}
				}
				return nil
			})
	}
	single := func(c *Conn) error {
		return c.WriteMessage(context.Background(), MessageBinary,
			&WriteOptions{MinWriteRate: 500}, make([]byte, 20000))
	}
	cases := []struct {
		name       string
		server     bool
		write      func(*Conn) error
		drainBytes int // bytes the peer drains, ≤500 per 250 ms, before stopping
		wantErr    error
	}{
		{"SteadyAboveFloor", false, streamed, streamedWire, nil},
		{"SingleLargeFrame", false, single, 20000 + 8, nil},
		{"SingleLargeFrameServer", true, single, 20000 + 4, nil},
		{"DrainStopsMidMessage", false, streamed, 2000, ErrTimeout},
		{"NoDrain", false, streamed, 0, ErrTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// A small buffer so progress is observed in ~500-byte chunks.
				opts := []func(*connConfig){withBufSize(512)}
				if tc.server {
					opts = append(opts, withServer())
				}
				c, peer := connPair(t, opts...)
				defer peer.Close() // unblocks a drain still parked in Read before synctest waits
				go func() {
					buf := make([]byte, 500)
					for got := 0; got < tc.drainBytes; {
						time.Sleep(250 * time.Millisecond)
						n, err := peer.Read(buf)
						got += n
						if err != nil {
							return
						}
					}
				}()

				err := tc.write(c)
				if tc.wantErr != nil {
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("write err = %v, want %v", err, tc.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("write: %v", err)
				}
			})
		})
	}
}

// TestSetWriteDeadlineExtendsBody covers SetWriteDeadline's supported use:
// extending the deadline forward mid-message so a transmission that would
// miss the seeded MessageTimeout completes.
func TestSetWriteDeadlineExtendsBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer := connPair(t)
		defer peer.Close() // unblocks a drain still parked in Read before synctest waits
		// Drain the frame (1000-byte payload, 8-byte header) slowly: it takes
		// about a second to transmit, far past the 250 ms seeded deadline.
		go func() {
			buf := make([]byte, 100)
			for got := 0; got < 1008; {
				time.Sleep(100 * time.Millisecond)
				n, err := peer.Read(buf)
				got += n
				if err != nil {
					return
				}
			}
		}()

		err := c.Write(context.Background(), MessageBinary,
			&WriteOptions{MessageTimeout: 250 * time.Millisecond}, func(w *Writer) error {
				if err := w.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return err
				}
				_, err := w.Write(make([]byte, 1000))
				return err
			})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
	})
}

// TestSetDeadlineRateConflict covers the rate-mode conflicts: in rate mode
// the integrator owns the deadline, so SetReadDeadline / SetWriteDeadline
// are rejected with ErrInvalidArgument and the message proceeds untouched.
func TestSetDeadlineRateConflict(t *testing.T) {
	c, peer := connPair(t)
	go drainPeer(peer)

	err := c.Write(context.Background(), MessageText,
		&WriteOptions{MinWriteRate: 1 << 20}, func(w *Writer) error {
			if err := w.SetWriteDeadline(time.Now().Add(time.Hour)); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("SetWriteDeadline in rate mode = %v, want ErrInvalidArgument", err)
			}
			_, err := w.Write([]byte("hi"))
			return err
		})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	go func() { _ = writePeerFrame(peer, true, opText, []byte("hi")) }()
	err = c.Read(&ReadOptions{MinReadRate: 1 << 20}, func(r *Reader) error {
		if err := r.SetReadDeadline(time.Now().Add(time.Hour)); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("SetReadDeadline in rate mode = %v, want ErrInvalidArgument", err)
		}
		_, err := io.ReadAll(r)
		return err
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
}
