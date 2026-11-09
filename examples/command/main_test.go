package main

import (
	"bufio"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestScanLinesSplitsLongLines(t *testing.T) {
	// A line several times maxLineSize, built from 3-byte runes so that piece
	// boundaries fall inside runes unless scanLines backs off.
	long := strings.Repeat("€", maxLineSize)
	input := "short\n" + long + "\n\nlast"

	sc := bufio.NewScanner(strings.NewReader(input))
	sc.Buffer(make([]byte, 0, 4096), maxLineSize)
	sc.Split(scanLines)
	var got []string
	for sc.Scan() {
		tok := sc.Text()
		if len(tok) > maxLineSize {
			t.Errorf("token of %d bytes exceeds maxLineSize", len(tok))
		}
		if !utf8.ValidString(tok) {
			t.Errorf("token is not valid UTF-8: %q...", tok[:8])
		}
		got = append(got, tok)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n := len(got); n < 6 || got[0] != "short" || got[n-2] != "" || got[n-1] != "last" {
		t.Fatalf("got %d tokens, first %q, last two %q; want short ... \"\" last", n, got[0], got[n-2:])
	}
	if joined := strings.Join(got[1:len(got)-2], ""); joined != long {
		t.Errorf("pieces of the long line do not reassemble (%d bytes, want %d)", len(joined), len(long))
	}
}
