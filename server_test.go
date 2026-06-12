package websocket

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestUpgradeRejectsBadHandshake covers every pre-hijack rejection: each
// case breaks one requirement of an otherwise valid upgrade request. The
// rejections happen before the hijack, so a ResponseRecorder works.
func TestUpgradeRejectsBadHandshake(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(r *http.Request)
		wantStatus int
		// wantVersion: RFC 6455 §4.2.2 requires a version-mismatch
		// rejection to advertise the versions the server speaks.
		wantVersion bool
	}{
		{"WrongMethod", func(r *http.Request) { r.Method = http.MethodPost }, http.StatusMethodNotAllowed, false},
		{"MissingConnectionUpgrade", func(r *http.Request) { r.Header.Del("Connection") }, http.StatusBadRequest, false},
		{"MissingUpgradeWebsocket", func(r *http.Request) { r.Header.Del("Upgrade") }, http.StatusBadRequest, false},
		{"VersionMismatch", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "8") }, http.StatusBadRequest, true},
		{"MissingKey", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Key") }, http.StatusBadRequest, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Connection", "Upgrade")
			r.Header.Set("Upgrade", "websocket")
			r.Header.Set("Sec-WebSocket-Version", "13")
			r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
			tc.mutate(r)
			w := httptest.NewRecorder()

			_, err := Upgrade(w, r, nil)
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("Upgrade err = %v, want ErrBadHandshake", err)
			}
			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if tc.wantVersion {
				if got := w.Header().Get("Sec-WebSocket-Version"); got != "13" {
					t.Errorf("Sec-WebSocket-Version = %q, want \"13\"", got)
				}
			}
		})
	}
}

func TestUpgradeHijackFailureDoesNotCommitSwitchingProtocols(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	w := httptest.NewRecorder()

	_, err := Upgrade(w, r, nil)
	if !errors.Is(err, ErrBadHandshake) {
		t.Fatalf("Upgrade err = %v, want ErrBadHandshake", err)
	}
	if w.Code == http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want a handshake failure status", w.Code)
	}
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
}

func TestSupportedSubprotocols(t *testing.T) {
	cases := []struct {
		name      string
		offered   string   // client's Sec-WebSocket-Protocol header
		supported []string // server's offer list
		want      string
	}{
		// Client lists "wamp.2.cbor, wamp.2.json"; server offers in
		// reverse order. RFC 6455 §4.1: pick the client's most-preferred
		// match, not the server's.
		{"PicksClientPreference", "wamp.2.cbor, wamp.2.json", []string{"wamp.2.json", "wamp.2.cbor"}, "wamp.2.cbor"},
		{"NoOverlap", "chat.v3", []string{"chat.v1", "chat.v2"}, ""},
		// Subprotocol identifiers are case-sensitive tokens.
		{"CaseSensitive", "Chat.V1", []string{"chat.v1"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Sec-WebSocket-Protocol", tc.offered)
			if got := SupportedSubprotocols(tc.supported...)(r); got != tc.want {
				t.Errorf("SupportedSubprotocols(%q) with offered %q = %q, want %q", tc.supported, tc.offered, got, tc.want)
			}
		})
	}
}

func TestAllowedOrigins(t *testing.T) {
	cases := []struct {
		name      string
		pattern   string
		allowSame bool
		host      string
		origin    string
		want      bool
	}{
		// Host pattern: matches by host, ignoring scheme.
		{"HostPatternMatch", "*.example.com", false, "api.example.com", "https://app.example.com", true},
		{"SameHostAllowed", "other.test", true, "api.example.com", "https://api.example.com", true},
		{"SameHostNotImplied", "other.test", false, "api.example.com", "https://api.example.com", false},
		{"DisallowedHost", "*.example.com", false, "api.example.com", "https://evil.com", false},
		{"NoOriginAllowed", "*.example.com", false, "api.example.com", "", true},
		{"ApexNotUnderWildcard", "*.example.com", false, "api.example.com", "https://example.com", false},
		{"SubdomainMatch", "*.example.com", false, "api.example.com", "https://malicious.example.com", true},
		// Scheme pattern: only matches when the scheme also equals.
		{"SchemeMismatchDenied", "https://app.example.com", false, "other.example.com", "http://app.example.com", false},
		{"SchemeMatchAllowed", "https://app.example.com", false, "other.example.com", "https://app.example.com", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			check := AllowedOrigins(tc.allowSame, tc.pattern)
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Host = tc.host
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if got := check(r); got != tc.want {
				t.Errorf("AllowedOrigins(%v, %q)(Host=%s, Origin=%s) = %v, want %v", tc.allowSame, tc.pattern, tc.host, tc.origin, got, tc.want)
			}
		})
	}
}

func TestAllowedOriginsInvalidPatternPanics(t *testing.T) {
	// "[" opens a character class that's never closed; path.Match
	// returns ErrBadPattern. AllowedOrigins should refuse it loudly
	// at construction time rather than silently denying every
	// request.
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("AllowedOrigins with malformed glob did not panic")
		}
	}()
	AllowedOrigins(false, "[unclosed")
}

func TestUpgradeDefaultHostCheckRejectsOtherHost(t *testing.T) {
	// With CheckOrigin == nil, Upgrade enforces the default host check.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := Upgrade(w, r, nil)
		if !errors.Is(err, ErrBadHandshake) {
			t.Errorf("Upgrade err = %v, want ErrBadHandshake", err)
		}
	}))
	defer srv.Close()

	// Dial with a foreign Origin set via the header argument.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	header := http.Header{}
	header.Set("Origin", "https://evil.example.com")
	_, resp, err := Dial(context.Background(), wsURL, nil, header)
	if !errors.Is(err, ErrBadHandshake) {
		t.Fatalf("Dial err = %v, want ErrBadHandshake", err)
	}
	if resp == nil {
		t.Fatalf("Dial resp = nil, want non-nil for inspection")
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestUpgradeOnErrorRendersCustomResponse(t *testing.T) {
	// OnError lets the caller render the rejection body, here a JSON
	// payload (à la RFC 9457) instead of http.Error's plain text, while
	// keeping the status Upgrade chose and the ErrBadHandshake error.
	const body = `{"error":"forbidden origin"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := Upgrade(w, r, &UpgradeOptions{
			OnError: func(w http.ResponseWriter, r *http.Request, status int, err error) {
				if !errors.Is(err, ErrBadHandshake) {
					t.Errorf("OnError err = %v, want ErrBadHandshake", err)
				}
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(status)
				io.WriteString(w, body)
			},
		})
		if !errors.Is(err, ErrBadHandshake) {
			t.Errorf("Upgrade err = %v, want ErrBadHandshake", err)
		}
	}))
	defer srv.Close()

	// Reject a request whose Origin host differs from the request Host
	// (default host check).
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	header := http.Header{}
	header.Set("Origin", "https://evil.example.com")
	_, resp, err := Dial(context.Background(), wsURL, &DialOptions{ResponseBodyLimit: 1024}, header)
	if !errors.Is(err, ErrBadHandshake) {
		t.Fatalf("Dial err = %v, want ErrBadHandshake", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(got) != body {
		t.Errorf("body = %q, want %q", got, body)
	}
}
