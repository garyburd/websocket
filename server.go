package websocket

import (
	"bufio"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
)

// Upgrade accepts a WebSocket handshake and takes ownership of the underlying
// HTTP/1.x connection.
//
// If the client's handshake has a protocol error, Upgrade writes an HTTP error
// response and returns an error that wraps [ErrBadHandshake]. On success, the
// returned [Conn] owns the connection. Either way, the handler must not use w
// after Upgrade returns.
//
// A handler that serves both WebSocket and ordinary HTTP requests can use
// [IsUpgradeRequest] to decide whether to call Upgrade.
func Upgrade(w http.ResponseWriter, r *http.Request, opts *UpgradeOptions) (*Conn, error) {
	if opts == nil {
		opts = &UpgradeOptions{}
	}

	reject := func(status int, err error, body string) (*Conn, error) {
		if opts.OnError != nil {
			opts.OnError(w, r, status, err)
		} else {
			http.Error(w, body, status)
		}
		return nil, err
	}
	fail := func(status int, reason string) (*Conn, error) {
		return reject(status, fmt.Errorf("%w: %s", ErrBadHandshake, reason), reason)
	}

	// Check the options before the request so a misconfigured server rejects
	// every handshake, not only those that pass request validation. The
	// client gets a generic body: the fault is in the server's configuration.
	if err := opts.validate(); err != nil {
		return reject(http.StatusInternalServerError, err, http.StatusText(http.StatusInternalServerError))
	}
	for k := range opts.ResponseHeader {
		if isManagedHeader(k) {
			err := fmt.Errorf("%w: %w: ResponseHeader must not set %q, which Upgrade manages",
				ErrBadHandshake, ErrInvalidArgument, k)
			return reject(http.StatusInternalServerError, err, http.StatusText(http.StatusInternalServerError))
		}
	}

	if r.Method != http.MethodGet {
		return fail(http.StatusMethodNotAllowed, fmt.Sprintf("method %q not allowed", r.Method))
	}
	if !IsUpgradeRequest(r) {
		return fail(http.StatusBadRequest, "not a WebSocket upgrade request (requires Connection: upgrade and Upgrade: websocket)")
	}
	if r.Header.Get("Sec-Websocket-Version") != "13" {
		// RFC 6455 §4.2.2 requires the supported version in the rejection.
		w.Header().Set("Sec-Websocket-Version", "13")
		return fail(http.StatusBadRequest, "unsupported Sec-WebSocket-Version")
	}
	challenge := r.Header.Get("Sec-Websocket-Key")
	if challenge == "" {
		return fail(http.StatusBadRequest, "missing Sec-WebSocket-Key")
	}
	// Malformed keys are accepted for compatibility; they are not trusted input.

	check := opts.CheckOrigin
	if check == nil {
		check = sameHost
	}
	if !check(r) {
		return fail(http.StatusForbidden, "forbidden origin")
	}

	var subprotocol string
	if opts.SelectSubprotocol != nil {
		subprotocol = opts.SelectSubprotocol(r)
	}

	h := make(http.Header)
	for k, vs := range opts.ResponseHeader {
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	h.Set("Upgrade", "websocket")
	h.Set("Connection", "Upgrade")
	h.Set("Sec-Websocket-Accept", computeAcceptKey(challenge))
	if subprotocol != "" {
		h.Set("Sec-Websocket-Protocol", subprotocol)
	}

	// Hijack before committing the 101 so failure remains reportable over HTTP.
	netConn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		status := http.StatusInternalServerError
		if r.ProtoMajor != 1 {
			status = http.StatusBadRequest
		}
		return fail(status, fmt.Sprintf("hijack: %v", err))
	}
	if err := writeSwitchingProtocols(brw.Writer, h); err != nil {
		netConn.Close()
		return nil, fmt.Errorf("%w: write upgrade response: %v", ErrBadHandshake, err)
	}
	// Preserve bytes buffered beyond the request headers.
	c := newConn(netConn, brw.Reader, connConfig{
		isServer:            true,
		controlReplyTimeout: opts.ControlReplyTimeout,
		shutdownTimeout:     opts.ShutdownTimeout,
		maxMessageSize:      opts.MaxMessageSize,
		writeBufferSize:     opts.WriteBufferSize,
		manualCloseResponse: opts.ManualCloseResponse,
	})
	c.subprotocol = subprotocol
	return c, nil
}

// IsUpgradeRequest reports whether r asks to switch to the WebSocket protocol:
// its Connection header includes the token "upgrade" and its Upgrade header
// includes the token "websocket", ignoring case. It does not validate the
// rest of the handshake. If it reports true, pass r to [Upgrade], which
// responds to a malformed request with the error the client expects.
func IsUpgradeRequest(r *http.Request) bool {
	return tokenListHas(r.Header.Values("Connection"), "upgrade") &&
		tokenListHas(r.Header.Values("Upgrade"), "websocket")
}

// writeSwitchingProtocols writes the 101 response. It modifies h.
func writeSwitchingProtocols(w *bufio.Writer, h http.Header) error {
	useRFCHeaderKeys(h)
	// bufio.Writer retains the first error until Flush.
	w.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	h.Write(w)
	w.WriteString("\r\n")
	return w.Flush()
}

// SupportedSubprotocols returns a selector for the client's first offered
// value present in supported. Matching is case-sensitive.
func SupportedSubprotocols(supported ...string) func(*http.Request) string {
	supported = slices.Clone(supported)
	return func(r *http.Request) string {
		for _, hdr := range r.Header.Values("Sec-Websocket-Protocol") {
			for c := range strings.SplitSeq(hdr, ",") {
				if c = trimOWS(c); slices.Contains(supported, c) {
					return c
				}
			}
		}
		return ""
	}
}

// AllowedOrigins returns an origin check using case-insensitive [path.Match]
// patterns. Patterns containing "://" match scheme://host; others match only
// the host. allowSameHost accepts an Origin host equal to the request Host.
// An absent Origin is accepted.
//
// Examples:
//
//	AllowedOrigins(true, "*.example.com")              // request host, or any host under example.com
//	AllowedOrigins(false, "https://app.example.com")   // exactly this scheme and host
//	AllowedOrigins(false, "https://*.example.com:443") // scheme + host + port
//
// AllowedOrigins panics on a malformed pattern.
func AllowedOrigins(allowSameHost bool, patterns ...string) func(*http.Request) bool {
	pats := make([]string, len(patterns))
	for i, p := range patterns {
		if _, err := path.Match(strings.ToLower(p), "websocket-origin-probe"); err != nil {
			panic(fmt.Sprintf("websocket: AllowedOrigins: invalid pattern %q: %v", p, err))
		}
		pats[i] = strings.ToLower(p)
	}
	return func(r *http.Request) bool {
		u, err := ParseOrigin(r)
		if err != nil {
			return false
		}
		if u == nil {
			return true // no Origin header
		}
		if allowSameHost && strings.EqualFold(r.Host, u.Host) {
			return true
		}
		for _, p := range pats {
			target := u.Host
			if strings.Contains(p, "://") {
				target = u.Scheme + "://" + u.Host
			}
			ok, _ := path.Match(p, strings.ToLower(target))
			if ok {
				return true
			}
		}
		return false
	}
}

// ParseOrigin parses the request Origin. It returns (nil, nil) when the header
// is absent and an error when it is malformed or has no host.
func ParseOrigin(r *http.Request) (*url.URL, error) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil, nil
	}
	u, err := url.Parse(origin)
	if err != nil {
		return nil, fmt.Errorf("parse Origin %q: %w", origin, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("missing host in Origin %q", origin)
	}
	return u, nil
}

// sameHost accepts an absent Origin or one whose host equals r.Host.
func sameHost(r *http.Request) bool {
	u, err := ParseOrigin(r)
	if err != nil {
		return false
	}
	if u == nil {
		return true
	}
	return strings.EqualFold(r.Host, u.Host)
}
