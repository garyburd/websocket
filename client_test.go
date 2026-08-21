package websocket

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestServer starts Go 1.27's in-memory test network and returns the
// transport Dial must use to reach it.
func newTestServer(t testing.TB, handler http.Handler) (*httptest.Server, *http.Transport) {
	t.Helper()
	srv := httptest.NewTestServer(t, handler)
	return srv, srv.Client().Transport.(*http.Transport)
}

func TestDialRejectsSecWebSocketHeader(t *testing.T) {
	// Sec-WebSocket-* headers carry handshake semantics Dial owns;
	// supplying one is a caller error caught before any network I/O,
	// so this needs no server.
	for _, key := range []string{
		"Sec-WebSocket-Key",
		"Sec-WebSocket-Version",
		"Sec-WebSocket-Protocol",
		"Sec-WebSocket-Extensions",
	} {
		h := http.Header{}
		h.Set(key, "x")
		_, _, err := Dial(context.Background(), "ws://example.invalid/", nil, h)
		if err == nil {
			t.Errorf("Dial with header %q = nil error, want rejection", key)
			continue
		}
		if !strings.Contains(err.Error(), "Sec-WebSocket") {
			t.Errorf("Dial with header %q error = %v, want it to mention Sec-WebSocket", key, err)
		}
	}
}

func TestDialRejectsInvalidSubprotocol(t *testing.T) {
	// Subprotocol names are joined into one comma-separated header value, so
	// a name that is not an HTTP token (a comma or space inside it, an empty
	// string, a non-ASCII rune) would corrupt the offer list. Caught before
	// any network I/O, so this needs no server.
	for _, proto := range []string{"", "chat,superchat", "chat v2", "chat;v2", "café"} {
		opts := &DialOptions{Subprotocols: []string{proto}}
		_, _, err := Dial(context.Background(), "ws://example.invalid/", opts, nil)
		if err == nil || !strings.Contains(err.Error(), "subprotocol") {
			t.Errorf("Dial with subprotocol %q err = %v, want subprotocol rejection", proto, err)
		}
	}
}

func TestDialSubprotocolCaseSensitive(t *testing.T) {
	// Subprotocol identifiers are case-sensitive tokens: a selection that
	// matches the offer exactly is accepted; one differing only in case was
	// not offered, and Dial must fail the handshake (browsers validate the
	// server's selection the same way).
	for _, tc := range []struct {
		name     string
		selected string
		wantOK   bool
	}{
		{"ExactMatchAccepted", "chat", true},
		{"CaseDifferenceRejected", "CHAT", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, transport := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := Upgrade(w, r, &UpgradeOptions{
					SelectSubprotocol: func(*http.Request) string { return tc.selected },
				})
				if err == nil {
					c.Close()
				}
			}))
			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

			c, _, err := Dial(context.Background(), wsURL, &DialOptions{
				Transport:    transport,
				Subprotocols: []string{"chat"},
			}, nil)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Dial err = %v, want success", err)
				}
				defer c.Close()
				if got := c.Subprotocol(); got != tc.selected {
					t.Errorf("Subprotocol = %q, want %q", got, tc.selected)
				}
				return
			}
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("Dial err = %v, want ErrBadHandshake", err)
			}
		})
	}
}

func TestDialSchemeCaseInsensitive(t *testing.T) {
	// URL schemes are case-insensitive (RFC 3986 §3.1): WS:// and WSS://
	// must be translated for net/http just like their lowercase forms.
	srv, transport := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade: %v", err)
			return
		}
		c.Close()
	}))
	wsURL := "WS" + strings.TrimPrefix(srv.URL, "http")

	c, _, err := Dial(context.Background(), wsURL, &DialOptions{Transport: transport}, nil)
	if err != nil {
		t.Fatalf("Dial(%q) = %v, want success", wsURL, err)
	}
	c.Close()
}

func TestDialCapturesFailureBody(t *testing.T) {
	// On a handshake failure the server's response body is normally the
	// hijacked connection, gone once Dial closes it. ResponseBodyLimit
	// snapshots up to N bytes into a bytes.Reader so the caller can read
	// the error body off the returned response without owning the conn.
	const body = "denied: token expired"
	srv, transport := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, body)
	}))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	for _, tc := range []struct {
		name  string
		limit int
		want  string
	}{
		{"whole body within limit", 1024, body},
		{"truncated to limit", 6, body[:6]},
		{"zero discards body", 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, resp, err := Dial(context.Background(), wsURL, &DialOptions{
				Transport:         transport,
				ResponseBodyLimit: tc.limit,
			}, nil)
			if c != nil {
				c.Close()
				t.Fatal("Dial returned a non-nil Conn on handshake failure")
			}
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("Dial error = %v, want ErrBadHandshake", err)
			}
			if resp == nil {
				t.Fatal("Dial returned a nil response on handshake failure")
			}
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("resp.StatusCode = %d, want %d", resp.StatusCode, http.StatusForbidden)
			}
			// The caller must be able to read the body without closing it.
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read captured body: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("captured body = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDialRefusesRedirect(t *testing.T) {
	// The default client must not follow a 3xx (it cannot carry the 101
	// upgrade and may cross origins). The redirect surfaces as an
	// ErrBadHandshake with the 3xx status, not a followed request.
	var hits int
	srv, transport := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, resp, err := Dial(context.Background(), wsURL, &DialOptions{Transport: transport}, nil)
	if c != nil {
		c.Close()
		t.Fatal("Dial returned a non-nil Conn for a redirect")
	}
	if !errors.Is(err, ErrBadHandshake) {
		t.Fatalf("Dial err = %v, want ErrBadHandshake", err)
	}
	if resp == nil || resp.StatusCode != http.StatusFound {
		t.Fatalf("resp = %v, want status %d", resp, http.StatusFound)
	}
	if hits != 1 {
		t.Errorf("server hit %d times, want 1 (redirect must not be followed)", hits)
	}
}

func TestDialHTTP2Hint(t *testing.T) {
	// A transport that advertises "h2" in TLSClientConfig.NextProtos (the
	// telltale of a tls.Config shared with net/http) gets a diagnostic hint
	// appended to a failed handshake. A transport advertising only HTTP/1.1
	// does not. The hint never displaces ErrBadHandshake.
	srv, transport := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	for _, tc := range []struct {
		name      string
		nextProto []string
		wantHint  bool
	}{
		{"h2 advertised", []string{"h2", "http/1.1"}, true},
		{"http/1.1 only", []string{"http/1.1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := transport.Clone()
			tr.TLSClientConfig = &tls.Config{NextProtos: tc.nextProto}
			opts := &DialOptions{Transport: tr}
			c, _, err := Dial(context.Background(), wsURL, opts, nil)
			if c != nil {
				c.Close()
				t.Fatal("Dial returned a non-nil Conn for a 400")
			}
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("Dial err = %v, want ErrBadHandshake", err)
			}
			if got := strings.Contains(err.Error(), "HTTP/2"); got != tc.wantHint {
				t.Errorf("err = %q; HTTP/2 hint present = %v, want %v", err, got, tc.wantHint)
			}
		})
	}
}

// TestDialMalformed101 covers 101 responses that fail Dial's vetting: each is
// a handshake failure matching ErrBadHandshake, and the body — the upgraded
// connection, not an HTTP error body — is never captured even with
// ResponseBodyLimit set, since reading it could block past the handshake.
func TestDialMalformed101(t *testing.T) {
	cases := []struct {
		name string
		rt   *dialResponseRT
	}{
		{"BadAcceptKey", &dialResponseRT{status: 101, upgrade: "websocket", connection: "Upgrade", accept: "bogus"}},
		{"UnwritableBody", &dialResponseRT{status: 101, upgrade: "websocket", connection: "Upgrade", echoAccept: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := &DialOptions{Transport: tc.rt, ResponseBodyLimit: 64}
			c, resp, err := Dial(context.Background(), "ws://example.com/", opts, nil)
			if c != nil {
				c.Close()
				t.Fatal("Dial returned a non-nil Conn for a malformed 101")
			}
			if !errors.Is(err, ErrBadHandshake) {
				t.Fatalf("Dial err = %v, want ErrBadHandshake", err)
			}
			if got, err := io.ReadAll(resp.Body); err != nil || len(got) != 0 {
				t.Errorf("captured body = %q, %v; want empty (101 bodies are never read)", got, err)
			}
		})
	}
}
