package websocket

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"unicode/utf8"
)

// TestBuildHeaderBytes locks down the exact wire encoding for a few headers.
func TestBuildHeaderBytes(t *testing.T) {
	key := [4]byte{0xAA, 0xBB, 0xCC, 0xDD}
	cases := []struct {
		name       string
		isServer   bool
		op         int
		fin        bool
		payloadLen int
		want       []byte
	}{
		{"server small fin", true, opBinary, true, 5, []byte{0x82, 0x05}},
		{"server small nofin", true, opContinuation, false, 5, []byte{0x00, 0x05}},
		{"server text", true, opText, true, 125, []byte{0x81, 0x7D}},
		{"server 16-bit", true, opBinary, true, 126, []byte{0x82, 126, 0x00, 0x7E}},
		{"server 16-bit max", true, opBinary, true, 0xFFFF, []byte{0x82, 126, 0xFF, 0xFF}},
		{"server 64-bit", true, opBinary, true, 0x10000, []byte{0x82, 127, 0, 0, 0, 0, 0, 0x01, 0x00, 0x00}},
		{"client small", false, opBinary, true, 5, []byte{0x82, 0x85, 0xAA, 0xBB, 0xCC, 0xDD}},
		{"client 16-bit", false, opBinary, true, 200, []byte{0x82, 126 | 0x80, 0x00, 0xC8, 0xAA, 0xBB, 0xCC, 0xDD}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf [maxHeaderLen]byte
			n := buildHeader(buf[:], tc.isServer, tc.op, tc.fin, tc.payloadLen, key)
			if !bytes.Equal(buf[:n], tc.want) {
				t.Errorf("buildHeader = % x, want % x", buf[:n], tc.want)
			}
		})
	}
}

// TestRoundTrip builds a header and parses it back. The peer reads the frame,
// so the receiver's role is the opposite of the sender's: a client-built
// (masked) header is parsed with isServer=true and vice versa. OneByteReader
// exercises the io.ReadFull loops one byte at a time.
func TestRoundTrip(t *testing.T) {
	key := [4]byte{0x01, 0x02, 0x03, 0x04}
	for _, payloadLen := range []int{0, 1, 125, 126, 0xFFFF, 0x10000, 1 << 30} {
		for _, senderIsServer := range []bool{false, true} {
			var buf [maxHeaderLen]byte
			n := buildHeader(buf[:], senderIsServer, opBinary, true, payloadLen, key)

			receiverIsServer := !senderIsServer
			h, err := readHeader(iotest.OneByteReader(bytes.NewReader(buf[:n])), receiverIsServer)
			if err != nil {
				t.Fatalf("payloadLen=%d senderIsServer=%v: readHeader: %v", payloadLen, senderIsServer, err)
			}
			if !h.fin || h.opcode != opBinary || h.length != payloadLen {
				t.Errorf("payloadLen=%d senderIsServer=%v: header = %+v", payloadLen, senderIsServer, h)
			}
			// Only a server-mode parse (client-sent, masked frame) captures the key.
			wantKey := [4]byte{}
			if receiverIsServer {
				wantKey = key
			}
			if h.maskKey != wantKey {
				t.Errorf("payloadLen=%d senderIsServer=%v: maskKey = % x, want % x", payloadLen, senderIsServer, h.maskKey, wantKey)
			}
		}
	}
}

func TestReadHeaderProtocolError(t *testing.T) {
	cases := []struct {
		name     string
		isServer bool
		bytes    []byte
		wantSub  string
	}{
		{"RSV bits", false, []byte{0x42, 0x00}, "RSV bits set"},
		{"unmasked from client", true, []byte{0x82, 0x00}, "client sent unmasked frame"},
		{"masked from server", false, []byte{0x82, 0x80}, "server sent masked frame"},
		{"non-minimal 16-bit", false, []byte{0x82, 126, 0x00, 0x05}, "non-minimal length encoding"},
		{"non-minimal 64-bit", false, []byte{0x82, 127, 0, 0, 0, 0, 0, 0, 0x00, 0x05}, "non-minimal length encoding"},
		{"64-bit MSB set", false, []byte{0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0}, "64-bit length has MSB set"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readHeader(iotest.OneByteReader(bytes.NewReader(tc.bytes)), tc.isServer)
			var pe *protocolError
			if !errors.As(err, &pe) {
				t.Fatalf("readHeader err = %v, want *protocolError", err)
			}
			if !strings.Contains(pe.text, tc.wantSub) {
				t.Errorf("protocolError = %q, want substring %q", pe.text, tc.wantSub)
			}
		})
	}
}

// TestReadHeaderTruncated checks that a transport cut short surfaces the raw
// I/O error (never a *protocolError), at each point ReadFull is used: io.EOF
// only at a clean frame boundary, io.ErrUnexpectedEOF once the frame has
// begun.
func TestReadHeaderTruncated(t *testing.T) {
	cases := []struct {
		name     string
		isServer bool
		bytes    []byte
		want     error
	}{
		{"empty", false, nil, io.EOF},
		{"partial fixed", false, []byte{0x82}, io.ErrUnexpectedEOF},
		{"missing 16-bit ext", false, []byte{0x82, 126}, io.ErrUnexpectedEOF},
		{"partial 16-bit ext", false, []byte{0x82, 126, 0x00}, io.ErrUnexpectedEOF},
		{"missing 64-bit ext", false, []byte{0x82, 127, 0, 0, 0, 0}, io.ErrUnexpectedEOF},
		{"missing mask key", true, []byte{0x82, 0x85}, io.ErrUnexpectedEOF},
		{"partial mask key", true, []byte{0x82, 0x85, 0xAA, 0xBB}, io.ErrUnexpectedEOF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readHeader(iotest.OneByteReader(bytes.NewReader(tc.bytes)), tc.isServer)
			if _, ok := errors.AsType[*protocolError](err); ok {
				t.Fatalf("readHeader err = %v, want I/O error not *protocolError", err)
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("readHeader err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestTruncateUTF8 covers the rune-boundary truncation used to fit a
// close reason into the control-frame limit.
func TestTruncateUTF8(t *testing.T) {
	cases := []struct {
		name string
		s    string
		max  int
		want string
	}{
		{"Empty", "", 5, ""},
		{"ZeroMax", "abc", 0, ""},
		{"ShorterThanMax", "abc", 5, "abc"},
		{"EqualToMax", "hello", 5, "hello"},
		{"AsciiTruncated", "hello world", 5, "hello"},
		// "€" is 3 bytes; "é" is 2 bytes.
		{"MultiByteWhole", "a€", 4, "a€"},
		{"MultiByteOnBoundary", "€a", 3, "€"},
		{"ThreeByteSplit", "ab€", 3, "ab"},
		{"TwoByteSplit", "aé", 2, "a"},
		{"AllMultiByteCut", "€€", 4, "€"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateUTF8(tc.s, tc.max)
			if got != tc.want {
				t.Errorf("truncateUTF8(%q, %d) = %q, want %q", tc.s, tc.max, got, tc.want)
			}
			if len(got) > tc.max {
				t.Errorf("result %q length %d exceeds max %d", got, len(got), tc.max)
			}
			if !utf8.ValidString(got) {
				t.Errorf("result %q is not valid UTF-8", got)
			}
		})
	}
}

func TestTokenListHas(t *testing.T) {
	cases := []struct {
		values []string
		token  string
		want   bool
	}{
		{[]string{"websocket"}, "websocket", true},
		{[]string{"WebSocket"}, "websocket", true},
		{[]string{"upgrade, websocket"}, "websocket", true},
		{[]string{"a, b ", " c, d"}, "c", true},
		{[]string{"a", "b"}, "c", false},
		{nil, "anything", false},
	}
	for _, tc := range cases {
		if got := tokenListHas(tc.values, tc.token); got != tc.want {
			t.Errorf("tokenListHas(%v, %q) = %v, want %v", tc.values, tc.token, got, tc.want)
		}
	}
}

func TestComputeAcceptKey(t *testing.T) {
	// RFC 6455 §1.3 example.
	got := computeAcceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got != want {
		t.Errorf("computeAcceptKey = %q, want %q", got, want)
	}
}

func TestValidWireCloseCode(t *testing.T) {
	cases := []struct {
		code CloseCode
		want bool
	}{
		// RFC 6455 §7.4.1 wire-valid send codes.
		{1000, true}, {1001, true}, {1002, true}, {1003, true},
		{1007, true}, {1008, true}, {1009, true}, {1010, true},
		{1011, true}, {1014, true},
		// Private-use range (§7.4.2).
		{3000, true}, {3999, true}, {4000, true}, {4999, true},
		// Receive-only / reserved / out-of-range.
		{0, false}, {999, false},
		{1004, false}, {1005, false}, {1006, false}, {1015, false},
		{1016, false}, {2999, false}, {5000, false}, {65535, false},
	}
	for _, tc := range cases {
		if got := validWireCloseCode(tc.code); got != tc.want {
			t.Errorf("validWireCloseCode(%d) = %v, want %v", tc.code, got, tc.want)
		}
	}
}

func TestParseClosePayload(t *testing.T) {
	cases := []struct {
		name       string
		payload    []byte
		wantCode   CloseCode
		wantReason string
		wantErr    bool
	}{
		{name: "Empty", payload: nil, wantCode: CloseNoStatusReceived},
		{name: "OneByte", payload: []byte{0x03}, wantErr: true},
		{name: "Valid", payload: []byte{0x03, 0xe8, 'h', 'i'}, wantCode: 1000, wantReason: "hi"},
		// 0x03ec = 1004 (reserved).
		{name: "ReservedCode", payload: []byte{0x03, 0xec}, wantErr: true},
		// Valid code, invalid UTF-8 reason.
		{name: "BadUTF8", payload: []byte{0x03, 0xe8, 0xff, 0xfe}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, reason, err := parseClosePayload(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseClosePayload(%v) err = nil, want non-nil", tc.payload)
				}
				return
			}
			if err != nil {
				t.Errorf("parseClosePayload(%v) err = %v, want nil", tc.payload, err)
			}
			if code != tc.wantCode {
				t.Errorf("code = %d, want %d", code, tc.wantCode)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestBuildClosePayload(t *testing.T) {
	got := buildClosePayload(1000, "hi")
	want := []byte{0x03, 0xe8, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// Reason gets truncated to fit within the control-payload limit.
	long := strings.Repeat("x", 200)
	got = buildClosePayload(1001, long)
	if len(got) != maxControlPayload {
		t.Errorf("len = %d, want %d", len(got), maxControlPayload)
	}
	if int(binary.BigEndian.Uint16(got[:2])) != 1001 {
		t.Errorf("code = %d", binary.BigEndian.Uint16(got[:2]))
	}
}

// refMask is a naive per-byte reference implementation of payload masking,
// used to verify maskCopy independently of its word-at-a-time machinery.
func refMask(b []byte, key [4]byte, pos int) int {
	pos &= 3
	for i := range b {
		b[i] ^= key[(pos+i)&3]
	}
	return (pos + len(b)) & 3
}

// TestMaskCopy verifies the fused copy-and-mask (out-of-place and in-place)
// against the naive reference, across word/byte boundaries and key positions.
func TestMaskCopy(t *testing.T) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	for _, n := range []int{0, 1, 7, 8, 9, 16, 31, 4096} {
		src := bytes.Repeat([]byte("0123456789abcdef"), n/16+1)[:n]
		for pos := range 4 {
			want := append([]byte(nil), src...)
			wantPos := refMask(want, key, pos)
			dst := make([]byte, n)
			if gotPos := maskCopy(dst, src, key, pos); gotPos != wantPos {
				t.Errorf("n=%d pos=%d: pos = %d, want %d", n, pos, gotPos, wantPos)
			}
			if !bytes.Equal(dst, want) {
				t.Errorf("n=%d pos=%d: out-of-place result mismatch", n, pos)
			}
			inPlace := append([]byte(nil), src...)
			maskCopy(inPlace, inPlace, key, pos)
			if !bytes.Equal(inPlace, want) {
				t.Errorf("n=%d pos=%d: in-place result mismatch", n, pos)
			}
		}
	}
}
