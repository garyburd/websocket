package websocket

import "unicode/utf8"

// Streaming UTF-8 validation using Björn Höhrmann's DFA.

const (
	utf8Accept = 0
	utf8Reject = 12
)

// utf8d contains byte classes followed by state transitions.
var utf8d = [...]uint8{
	// Byte classes.
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00..0f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10..1f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20..2f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30..3f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 40..4f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 50..5f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60..6f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 70..7f
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 80..8f
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, // 90..9f
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, // a0..af
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, // b0..bf
	8, 8, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // c0..cf
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // d0..df
	10, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 3, // e0..ef
	11, 6, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, // f0..ff

	// State transitions.
	0, 12, 24, 36, 60, 96, 84, 12, 12, 12, 48, 72, // state 0  (accept)
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, // state 12 (reject)
	12, 0, 12, 12, 12, 12, 12, 0, 12, 0, 12, 12, // state 24
	12, 24, 12, 12, 12, 12, 12, 24, 12, 24, 12, 12, // state 36
	12, 12, 12, 12, 12, 12, 12, 24, 12, 12, 12, 12, // state 48
	12, 24, 12, 12, 12, 12, 12, 12, 12, 24, 12, 12, // state 60
	12, 12, 12, 12, 12, 12, 12, 36, 12, 36, 12, 12, // state 72
	12, 36, 12, 12, 12, 12, 12, 36, 12, 36, 12, 12, // state 84
	12, 36, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, // state 96
}

// utf8Validate advances the DFA state through p.
//
// The DFA costs two dependent table loads per byte, so it runs only at the
// edges of p: to finish a rune split from the previous chunk, and on a rune
// that may continue into the next one. utf8.Valid checks the complete runes
// between, eight ASCII bytes at a time.
func utf8Validate(state uint32, p []byte) uint32 {
	for len(p) > 0 && state != utf8Accept {
		state = uint32(utf8d[256+state+uint32(utf8d[p[0]])])
		if state == utf8Reject {
			return state
		}
		p = p[1:]
	}
	// A rune that may be incomplete starts at the last byte that is not a
	// continuation byte, if that byte is among the last three. Any valid
	// decoding starts a rune there, so the bytes before it must be valid by
	// themselves.
	tail := len(p)
	for i := len(p) - 1; i >= 0 && i >= len(p)-3; i-- {
		if b := p[i]; b&0xC0 != 0x80 {
			if b >= 0xC0 {
				tail = i
			}
			break
		}
	}
	if !utf8.Valid(p[:tail]) {
		return utf8Reject
	}
	for _, b := range p[tail:] {
		state = uint32(utf8d[256+state+uint32(utf8d[b])])
		if state == utf8Reject {
			break
		}
	}
	return state
}
