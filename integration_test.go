package websocket

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestUpgradeAndExchange(t *testing.T) {
	// End-to-end round trip: in-memory HTTP server, Dial handshakes, then
	// each side reads what the other writes through the framing layer
	// (server unmasked outbound, client masked outbound).
	serverDone := make(chan error, 1)
	srv, transport := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r, &UpgradeOptions{
			SelectSubprotocol: SupportedSubprotocols("chat.v1", "chat.v2"),
		})
		if err != nil {
			serverDone <- err
			return
		}
		defer c.Close()
		data, mt, err := c.ReadMessage(nil)
		if err != nil {
			serverDone <- err
			return
		}
		if mt != MessageText || string(data) != "ping from client" {
			serverDone <- fmt.Errorf("server got (%v, %q), want (Text, ping from client)", mt, data)
			return
		}
		if err := c.WriteMessage(context.Background(), MessageText, nil, []byte("pong from server")); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, _, err := Dial(context.Background(), wsURL, &DialOptions{
		Transport:    transport,
		Subprotocols: []string{"chat.v2"},
	}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	if got := c.Subprotocol(); got != "chat.v2" {
		t.Errorf("client Subprotocol() = %q, want chat.v2", got)
	}
	if err := c.WriteMessage(context.Background(), MessageText, nil, []byte("ping from client")); err != nil {
		t.Fatalf("client WriteMessage: %v", err)
	}
	data, mt, err := c.ReadMessage(nil)
	if err != nil {
		t.Fatalf("client ReadMessage: %v", err)
	}
	if mt != MessageText || string(data) != "pong from server" {
		t.Errorf("client ReadMessage = (%v, %q), want (Text, pong from server)", mt, data)
	}
	if err := <-serverDone; err != nil {
		t.Errorf("server side: %v", err)
	}
}
