package websocket

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

// utf8Cases are byte slices spanning the well-formed and malformed UTF-8 the
// RFC 6455 §8.1 check must accept and reject.
var utf8Cases = []struct {
	name  string
	bytes []byte
}{
	{"Empty", nil},
	{"ASCII", []byte("hello, world")},
	{"TwoByte", []byte("héllo")},                     // é = C3 A9
	{"ThreeByte", []byte("€uro")},                    // € = E2 82 AC
	{"FourByte", []byte("😀!")},                       // 😀 = F0 9F 98 80
	{"MaxRune", []byte{0xF4, 0x8F, 0xBF, 0xBF}},      // U+10FFFF
	{"LoneContinuation", []byte{0x80}},               //
	{"Overlong2", []byte{0xC0, 0x80}},                // overlong NUL
	{"Overlong3", []byte{0xE0, 0x80, 0xAF}},          // overlong '/'
	{"Surrogate", []byte{0xED, 0xA0, 0x80}},          // U+D800
	{"AboveMaxRune", []byte{0xF4, 0x90, 0x80, 0x80}}, // U+110000
	{"TruncatedTwoByte", []byte{0xC3}},
	{"TruncatedFourByte", []byte{0xF0, 0x9F, 0x98}},
	{"BadContinuation", []byte{0xC3, 0x28}},
	{"FE", []byte{0xFE}},
	{"FF", []byte{0xFF}},
}

// TestUTF8ValidateMatchesStdlib checks the DFA against utf8.Valid for complete
// inputs, and that feeding the bytes in two pieces (split at every offset)
// reaches the same final state as feeding them all at once, the property the
// streaming validators rely on.
func TestUTF8ValidateMatchesStdlib(t *testing.T) {
	for _, tc := range utf8Cases {
		t.Run(tc.name, func(t *testing.T) {
			whole := utf8Validate(utf8Accept, tc.bytes)
			if got, want := whole == utf8Accept, utf8.Valid(tc.bytes); got != want {
				t.Errorf("accept = %v, utf8.Valid = %v for %x", got, want, tc.bytes)
			}
			for i := 0; i <= len(tc.bytes); i++ {
				s := utf8Validate(utf8Accept, tc.bytes[:i])
				if s = utf8Validate(s, tc.bytes[i:]); s != whole {
					t.Errorf("split at %d gave state %d, whole = %d for %x", i, s, whole, tc.bytes)
				}
			}
		})
	}
}

// TestReadInvalidUTF8ClosesWithInvalidData covers rejecting a text message
// that is not valid UTF-8 (RFC 6455 §8.1): a malformed byte mid-stream, and a
// multi-byte rune left incomplete at end of message. Both fail the read with
// ErrProtocol and answer with a CloseInvalidData close frame.
func TestReadInvalidUTF8ClosesWithInvalidData(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		wantSub string
	}{
		{"InvalidByte", []byte{0xff}, "invalid UTF-8"},
		{"IncompleteAtEnd", []byte("ab\xc3"), "incomplete UTF-8"}, // ends mid 2-byte rune
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)
			got := make(chan peerFrame, 1)
			go func() {
				_ = writePeerFrame(peer, true, opText, tc.payload)
				got <- readPeerFrame(t, peer) // the conn's outbound Close
			}()

			_, _, err := c.ReadMessage(nil)
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("ReadMessage err = %v, want ErrProtocol with %q", err, tc.wantSub)
			}
			f := <-got
			if f.opcode != opClose {
				t.Fatalf("peer opcode %v, want opClose", f.opcode)
			}
			if len(f.payload) < 2 || CloseCode(binary.BigEndian.Uint16(f.payload[:2])) != CloseInvalidData {
				t.Errorf("close payload = %x, want code %d", f.payload, CloseInvalidData)
			}
		})
	}
}

func TestReadUTF8SplitAcrossFrames(t *testing.T) {
	c, peer := connPair(t)
	go func() {
		// "€" = E2 82 AC, split across a text frame and its continuation.
		_ = writePeerFrame(peer, false, opText, []byte{0xE2})
		_ = writePeerFrame(peer, true, opContinuation, []byte{0x82, 0xAC})
		drainPeer(peer)
	}()

	data, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if mt != MessageText || string(data) != "€" {
		t.Errorf("mt=%v data=%q, want text \"€\"", mt, data)
	}
}

func TestReadUTF8SplitAcrossReadChunks(t *testing.T) {
	c, peer := connPair(t)
	go func() {
		_ = writePeerFrame(peer, true, opText, []byte("€")) // 3 bytes, one frame
		drainPeer(peer)
	}()

	err := c.Read(nil, func(r *Reader) error {
		var got []byte
		buf := make([]byte, 1) // 1-byte reads force the rune across chunk boundaries
		for {
			n, err := r.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}
		if string(got) != "€" {
			t.Errorf("got %q, want \"€\"", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
}

// TestReadUnvalidatedBytesPassThrough confirms bytes that are not valid UTF-8
// are delivered verbatim when the RFC 6455 §8.1 text check does not apply: a
// binary message (never validated) and a text message read with
// SkipUTF8Validation.
func TestReadUnvalidatedBytesPassThrough(t *testing.T) {
	bad := []byte{0xff, 0xfe, 0x80, 0xc0} // not valid UTF-8
	for _, tc := range []struct {
		name   string
		opcode int
		opts   *ReadOptions
		want   MessageType
	}{
		{"TextWithSkipValidation", opText, &ReadOptions{SkipUTF8Validation: true}, MessageText},
		{"Binary", opBinary, nil, MessageBinary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)
			go func() {
				_ = writePeerFrame(peer, true, tc.opcode, bad)
				drainPeer(peer)
			}()

			data, mt, err := c.ReadMessage(tc.opts)
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			if mt != tc.want || !bytes.Equal(data, bad) {
				t.Errorf("mt=%v data=%x, want %v %x", mt, data, tc.want, bad)
			}
		})
	}
}

// TestWriteTextNotValidated confirms outbound text is sent verbatim: the
// package does not validate it, so both well-formed multi-byte text and
// invalid UTF-8 go out as-is.
func TestWriteTextNotValidated(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"ValidMultibyte", []byte("héllo, €uro 😀")},
		{"InvalidBytes", []byte{0xff, 0xfe}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peer := connPair(t)
			done := make(chan peerFrame, 1)
			go func() { done <- readPeerFrame(t, peer) }()

			if err := c.WriteMessage(context.Background(), MessageText, nil, tc.payload); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			f := <-done
			if f.opcode != opText || !bytes.Equal(f.payload, tc.payload) {
				t.Errorf("frame = {op:%d payload:%x}, want text %x", f.opcode, f.payload, tc.payload)
			}
		})
	}
}
