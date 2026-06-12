package websocket

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

// Dial opens a WebSocket connection. header may contain application headers
// such as Origin, Cookie, and Authorization.
//
// Dial rejects Sec-WebSocket-* headers, which it manages. Configure
// subprotocols through [DialOptions.Subprotocols].
//
// ctx bounds only the handshake. Use [Conn.Shutdown], [Conn.Close],
// [ReadOptions], and [WriteOptions] after Dial returns.
//
// Dial does not follow redirects. A response other than 101 returns
// [ErrBadHandshake] and the response with up to
// [DialOptions.ResponseBodyLimit] body bytes. On success, the [Conn] owns the
// response body. The caller need not close either returned body.
func Dial(ctx context.Context, rawURL string, opts *DialOptions, header http.Header) (*Conn, *http.Response, error) {
	if opts == nil {
		opts = &DialOptions{}
	}
	for k := range header {
		if strings.HasPrefix(strings.ToLower(k), "sec-websocket") {
			return nil, nil, fmt.Errorf("websocket: header must not set %q; Sec-WebSocket-* headers are managed by Dial", k)
		}
	}
	for _, p := range opts.Subprotocols {
		if !isToken(p) {
			return nil, nil, fmt.Errorf("websocket: subprotocol %q is not a valid token", p)
		}
	}
	rt := opts.Transport
	if rt == nil {
		rt = &http.Transport{
			Proxy:             http.ProxyFromEnvironment,
			DisableKeepAlives: true,
			ForceAttemptHTTP2: false,
			TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
		}
	}

	challenge := generateChallengeKey()

	// net/http requires HTTP scheme names.
	switch {
	case len(rawURL) >= 5 && strings.EqualFold(rawURL[:5], "ws://"):
		rawURL = "http://" + rawURL[5:]
	case len(rawURL) >= 6 && strings.EqualFold(rawURL[:6], "wss://"):
		rawURL = "https://" + rawURL[6:]
	}

	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("websocket: build request: %w", err)
	}
	if header != nil {
		req.Header = header.Clone()
	}
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-Websocket-Key", challenge)
	req.Header.Set("Sec-Websocket-Version", "13")
	if len(opts.Subprotocols) > 0 {
		req.Header.Set("Sec-Websocket-Protocol", strings.Join(opts.Subprotocols, ", "))
	}

	resp, err := rt.RoundTrip(req)
	if err != nil {
		return nil, nil, http2Hint(rt, fmt.Errorf("websocket: handshake: %w", err))
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		captureResponseBody(resp, opts.ResponseBodyLimit)
		return nil, resp, http2Hint(rt, fmt.Errorf("%w: status %d", ErrBadHandshake, resp.StatusCode))
	}
	if !tokenListHas(resp.Header.Values("Upgrade"), "websocket") ||
		!tokenListHas(resp.Header.Values("Connection"), "upgrade") ||
		resp.Header.Get("Sec-Websocket-Accept") != computeAcceptKey(challenge) {
		captureResponseBody(resp, opts.ResponseBodyLimit)
		return nil, resp, fmt.Errorf("%w: status %d", ErrBadHandshake, resp.StatusCode)
	}
	// No WebSocket extensions are implemented.
	if ext := resp.Header.Get("Sec-Websocket-Extensions"); ext != "" {
		captureResponseBody(resp, opts.ResponseBodyLimit)
		return nil, resp, fmt.Errorf("%w: unsupported extension %q", ErrBadHandshake, ext)
	}
	subprotocol := resp.Header.Get("Sec-Websocket-Protocol")
	if subprotocol != "" && !slices.Contains(opts.Subprotocols, subprotocol) {
		captureResponseBody(resp, opts.ResponseBodyLimit)
		return nil, resp, fmt.Errorf("%w: server selected subprotocol %q which was not offered", ErrBadHandshake, subprotocol)
	}
	rwc, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		captureResponseBody(resp, opts.ResponseBodyLimit)
		return nil, resp, fmt.Errorf("%w: response body is not writable", ErrBadHandshake)
	}

	c := newConn(rwc, nil, connConfig{
		controlReplyTimeout: opts.ControlReplyTimeout,
		shutdownTimeout:     opts.ShutdownTimeout,
		maxMessageSize:      opts.MaxMessageSize,
		writeBufferSize:     opts.WriteBufferSize,
		manualCloseResponse: opts.ManualCloseResponse,
	})
	c.subprotocol = subprotocol
	return c, resp, nil
}

// http2Hint identifies transports that may negotiate unsupported HTTP/2.
func http2Hint(rt http.RoundTripper, err error) error {
	t, ok := rt.(*http.Transport)
	if !ok || t.TLSClientConfig == nil {
		return err
	}
	for _, proto := range t.TLSClientConfig.NextProtos {
		if proto != "http/1.1" {
			return fmt.Errorf("%w (Transport's TLSClientConfig.NextProtos advertises %q; "+
				"a tls.Config shared with net/http negotiates HTTP/2, which cannot carry the WebSocket upgrade)", err, proto)
		}
	}
	return err
}

// captureResponseBody replaces a failed response body with up to limit bytes.
// A 101 body is an upgraded connection and is never read.
func captureResponseBody(resp *http.Response, limit int) {
	var buf []byte
	if limit > 0 && resp.StatusCode != http.StatusSwitchingProtocols {
		buf, _ = io.ReadAll(io.LimitReader(resp.Body, int64(limit)))
	}
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(buf))
}
