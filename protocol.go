package websocket

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// computeAcceptKey derives Sec-WebSocket-Accept from Sec-WebSocket-Key.
// RFC 6455 requires SHA-1; it protects no secret here.
func computeAcceptKey(challenge string) string {
	h := sha1.New()
	h.Write([]byte(challenge))
	h.Write([]byte(wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// randFill fills p; crypto/rand.Read is documented never to fail.
func randFill(p []byte) { rand.Read(p) }

// generateChallengeKey returns a Sec-WebSocket-Key value.
func generateChallengeKey() string {
	var p [16]byte
	randFill(p[:])
	return base64.StdEncoding.EncodeToString(p[:])
}

// tokenListHas searches comma-separated header values case-insensitively.
func tokenListHas(values []string, token string) bool {
	for _, v := range values {
		for part := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// isToken reports whether s is a nonempty HTTP token.
func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

func isControlOp(op int) bool { return op&0x8 != 0 }

func isDataOp(op int) bool { return op == opText || op == opBinary }

const (
	maxHeaderLen      = 14
	maxControlPayload = 125 // RFC 6455 §5.5
)

type frameHeader struct {
	fin    bool
	opcode int

	// readHeader guarantees length fits in int.
	length int

	maskKey [4]byte
}

// protocolError carries the close response for a peer violation.
type protocolError struct {
	code CloseCode
	text string
}

func (e *protocolError) Error() string { return "websocket: protocol error: " + e.text }
func (e *protocolError) Unwrap() error { return ErrProtocol }

func peerError(code CloseCode, format string, args ...any) *protocolError {
	return &protocolError{code: code, text: fmt.Sprintf(format, args...)}
}

// buildHeader encodes a frame header into buf and returns its length.
func buildHeader(buf []byte, isServer bool, op int, fin bool, payloadLen int, key [4]byte) int {
	var maskBit byte
	if !isServer {
		maskBit = 0x80
	}
	buf[0] = byte(op)
	if fin {
		buf[0] |= 0x80
	}
	n := 2
	switch {
	case payloadLen <= 125:
		buf[1] = maskBit | byte(payloadLen)
	case payloadLen <= 0xFFFF:
		buf[1] = maskBit | 126
		binary.BigEndian.PutUint16(buf[2:4], uint16(payloadLen))
		n = 4
	default:
		buf[1] = maskBit | 127
		binary.BigEndian.PutUint64(buf[2:10], uint64(payloadLen))
		n = 10
	}
	if !isServer {
		n += copy(buf[n:n+4], key[:])
	}
	return n
}

// readFull maps every premature EOF to io.ErrUnexpectedEOF.
func readFull(r io.Reader, p []byte) (int, error) {
	n, err := io.ReadFull(r, p)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

// readHeader parses and validates one frame header.
func readHeader(r io.Reader, isServer bool) (frameHeader, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frameHeader{}, err
	}
	fin := hdr[0]&0x80 != 0
	rsv := hdr[0] & 0x70
	op := int(hdr[0] & 0x0F)
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7F)

	if rsv != 0 {
		return frameHeader{}, peerError(CloseProtocolError, "RSV bits set")
	}
	if isServer && !masked {
		return frameHeader{}, peerError(CloseProtocolError, "client sent unmasked frame")
	}
	if !isServer && masked {
		return frameHeader{}, peerError(CloseProtocolError, "server sent masked frame")
	}

	switch length {
	case 126:
		var ext [2]byte
		if _, err := readFull(r, ext[:]); err != nil {
			return frameHeader{}, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
		if length < 126 {
			return frameHeader{}, peerError(CloseProtocolError, "non-minimal length encoding")
		}
	case 127:
		var ext [8]byte
		if _, err := readFull(r, ext[:]); err != nil {
			return frameHeader{}, err
		}
		length = binary.BigEndian.Uint64(ext[:])
		if length <= 0xFFFF {
			return frameHeader{}, peerError(CloseProtocolError, "non-minimal length encoding")
		}
		if length&(1<<63) != 0 {
			return frameHeader{}, peerError(CloseProtocolError, "64-bit length has MSB set")
		}
	}

	h := frameHeader{fin: fin, opcode: op}
	if isServer {
		if _, err := readFull(r, h.maskKey[:]); err != nil {
			return frameHeader{}, err
		}
	}
	if length > math.MaxInt {
		return frameHeader{}, peerError(CloseProtocolError, "frame too large (%d)", length)
	}
	h.length = int(length)
	return h, nil
}

// CloseCode is an RFC 6455 close status code.
//
// [CloseNoStatusReceived] and [CloseAbnormalClosure] are receive-only.
type CloseCode int

const (
	CloseNormalClosure   CloseCode = 1000
	CloseGoingAway       CloseCode = 1001
	CloseProtocolError   CloseCode = 1002
	CloseUnsupportedData CloseCode = 1003
	CloseInvalidData     CloseCode = 1007
	ClosePolicyViolation CloseCode = 1008
	CloseMessageTooBig   CloseCode = 1009
	CloseInternalError   CloseCode = 1011

	CloseNoStatusReceived CloseCode = 1005
	CloseAbnormalClosure  CloseCode = 1006
)

// String returns the code name or its decimal value.
func (c CloseCode) String() string {
	switch c {
	case CloseNormalClosure:
		return "NormalClosure"
	case CloseGoingAway:
		return "GoingAway"
	case CloseProtocolError:
		return "ProtocolError"
	case CloseUnsupportedData:
		return "UnsupportedData"
	case CloseInvalidData:
		return "InvalidData"
	case ClosePolicyViolation:
		return "PolicyViolation"
	case CloseMessageTooBig:
		return "MessageTooBig"
	case CloseInternalError:
		return "InternalError"
	case CloseNoStatusReceived:
		return "NoStatusReceived"
	case CloseAbnormalClosure:
		return "AbnormalClosure"
	default:
		return strconv.Itoa(int(c))
	}
}

// CloseError reports a peer close. Use errors.As to inspect Code and Text.
// [CloseAbnormalClosure] means the transport ended without a close frame.
type CloseError struct {
	// Code is the peer status, [CloseNoStatusReceived], or
	// [CloseAbnormalClosure].
	Code CloseCode
	// Text is the UTF-8 close reason.
	Text string
}

// Error formats the close code and reason.
func (e *CloseError) Error() string {
	if e.Text == "" {
		return fmt.Sprintf("websocket: peer closed (%d)", e.Code)
	}
	return fmt.Sprintf("websocket: peer closed (%d): %s", e.Code, e.Text)
}

// validWireCloseCode reports whether code may appear on the wire.
func validWireCloseCode(code CloseCode) bool {
	if code >= 3000 && code <= 4999 {
		return true
	}
	if code < 1000 || code > 1014 {
		return false
	}
	switch code {
	case 1004, 1005, 1006:
		return false
	}
	return true
}

// parseClosePayload validates an inbound close payload.
func parseClosePayload(payload []byte) (CloseCode, string, error) {
	if len(payload) == 0 {
		return CloseNoStatusReceived, "", nil
	}
	if len(payload) < 2 {
		return 0, "", peerError(CloseProtocolError, "close payload shorter than 2 bytes")
	}
	code := CloseCode(binary.BigEndian.Uint16(payload[:2]))
	if !validWireCloseCode(code) {
		return 0, "", peerError(CloseProtocolError, "invalid inbound close code %d", code)
	}
	reason := payload[2:]
	if !utf8.Valid(reason) {
		return 0, "", peerError(CloseProtocolError, "invalid UTF-8 in close reason")
	}
	return code, string(reason), nil
}

// truncateUTF8 truncates s to max bytes at a rune boundary.
func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	s = s[:max]
	for len(s) > 0 {
		// An incomplete trailing sequence decodes as (RuneError, 1); a
		// genuine encoded U+FFFD has size > 1 and is a valid boundary.
		if r, size := utf8.DecodeLastRuneInString(s); r != utf8.RuneError || size > 1 {
			break
		}
		s = s[:len(s)-1]
	}
	return s
}

// buildClosePayload encodes a status and a UTF-8-truncated reason.
func buildClosePayload(code CloseCode, reason string) []byte {
	reason = truncateUTF8(reason, maxControlPayload-2)
	p := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(p[:2], uint16(code))
	copy(p[2:], reason)
	return p
}
