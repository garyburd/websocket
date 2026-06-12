package websocket_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/garyburd/websocket"
)

// A server that expects the client to send a message immediately after
// connecting and then messages at any interval uses two *ReadOptions: a 10s
// IdleTimeout (a hard cap on receiving the first message), and for the steady
// state no cap. Keepalive instead detects a dead client. IdleTimeout is
// per-call precisely because the first read and the steady reads differ.
func ExampleReadOptions_perReadIdleTimeout() {
	var c *websocket.Conn // obtained from websocket.Upgrade

	first := &websocket.ReadOptions{IdleTimeout: 10 * time.Second}
	steady := &websocket.ReadOptions{
		KeepaliveInterval: 30 * time.Second,
		KeepaliveTimeout:  10 * time.Second,
		MatchPong:         true,
	}

	if _, _, err := c.ReadMessage(first); err != nil {
		return // client stayed silent within 10s
	}
	for {
		_, _, err := c.ReadMessage(steady)
		if err != nil {
			return
		}
		// ... handle the message ...
	}
}

// When a buffered writer is layered on the [websocket.Writer], call
// [websocket.Writer.Final] just before flushing it. The flush then emits the
// message's last bytes as the final frame (FIN set), instead of the message
// ending with a separate empty terminating frame when the callback returns.
func ExampleWriter_Final() {
	var c *websocket.Conn // obtained from websocket.Dial or websocket.Upgrade

	err := c.Write(context.Background(), websocket.MessageText, nil, func(w *websocket.Writer) error {
		bw := bufio.NewWriter(w)
		// ... write the message to bw, e.g. with a json.Encoder ...
		bw.WriteString("hello, world")

		w.Final()         // the next flush ends the message
		return bw.Flush() // sends the buffered bytes as the final frame
	})
	if err != nil {
		return // ... handle write error ...
	}
}

// A roundtrip between a server built with [websocket.Upgrade] and a client built
// with [websocket.Dial]. The server echoes each message back; the client sends one
// message and prints the echo.
func Example_echo() {
	// Server: echo every message back to the client until it disconnects.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Upgrade(w, r, nil)
		if err != nil {
			return // Upgrade has already written an error response
		}
		defer c.Close()
		for {
			data, mt, err := c.ReadMessage(nil)
			if err != nil {
				return
			}
			if err := c.WriteMessage(context.Background(), mt, nil, data); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	// Client: httptest serves http://, so swap in the ws:// scheme to dial.
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, _, err := websocket.Dial(context.Background(), url, nil, nil)
	if err != nil {
		fmt.Println("dial:", err)
		return
	}
	defer c.Close()

	if err := c.WriteMessage(context.Background(), websocket.MessageText, nil, []byte("hello")); err != nil {
		fmt.Println("write:", err)
		return
	}
	data, _, err := c.ReadMessage(nil)
	if err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Printf("%s\n", data)

	// Output: hello
}

// A read loop ends on the first error. Use errors.As to recover the peer's close
// code and reason from a [websocket.CloseError]; a [websocket.CloseAbnormalClosure]
// code means the peer vanished without sending a close message. A local
// [websocket.Conn.Close] reports [websocket.ErrClosed], and other failures
// (timeouts, protocol errors) end the loop the same way.
func ExampleCloseError() {
	var c *websocket.Conn // obtained from websocket.Dial or websocket.Upgrade
	defer c.Close()

	for {
		_, _, err := c.ReadMessage(nil)
		if err == nil {
			continue // ... handle the message ...
		}

		var ce *websocket.CloseError
		switch {
		case errors.As(err, &ce):
			if ce.Code == websocket.CloseAbnormalClosure {
				log.Print("peer vanished without a close message")
			} else {
				log.Printf("peer closed: code=%s reason=%q", ce.Code, ce.Text)
			}
		case errors.Is(err, websocket.ErrClosed):
			log.Print("connection closed locally")
		default:
			log.Printf("connection failed: %v", err)
		}
		return
	}
}

// An application that only sends data must still keep a read outstanding, so the
// package can answer the peer's pings and observe its close. Run a discarding read
// loop in its own goroutine alongside the writer; the keepalive options on the read
// detect a peer that has gone away.
func ExampleConn_Read_writeOnly() {
	var c *websocket.Conn // obtained from websocket.Dial or websocket.Upgrade

	// Discard inbound data, but let the package handle control frames and surface
	// the peer's close. The first error ends the loop and closes the connection.
	go func() {
		opts := &websocket.ReadOptions{
			KeepaliveInterval: 30 * time.Second,
			KeepaliveTimeout:  10 * time.Second,
			MatchPong:         true,
		}
		for {
			if err := c.Read(opts, func(*websocket.Reader) error { return nil }); err != nil {
				c.Close()
				return
			}
		}
	}()

	// Send the time once a second until the connection goes away.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for range tick.C {
		msg := []byte(time.Now().Format(time.RFC3339))
		if err := c.WriteMessage(context.Background(), websocket.MessageText, nil, msg); err != nil {
			return // the read goroutine has already closed the connection
		}
	}
}

// To close gracefully, call [websocket.Conn.Shutdown] to send the close message,
// then keep reading until the read loop drains the peer's echo and returns an error,
// and finally call [websocket.Conn.Close]. The ShutdownTimeout option bounds the
// wait for the echo.
func ExampleConn_Shutdown() {
	var c *websocket.Conn // obtained from websocket.Dial or websocket.Upgrade
	defer c.Close()

	if err := c.Shutdown(websocket.CloseNormalClosure, "bye"); err != nil {
		return // ... handle error ...
	}
	// Drain until the closing handshake completes (or the connection fails); the
	// peer's echoed close surfaces here as the terminal read error.
	for {
		if _, _, err := c.ReadMessage(nil); err != nil {
			break
		}
	}
}

// The package has no JSON helpers; use encoding/json directly over the streaming
// [websocket.Writer] and [websocket.Reader]. Sending encodes into the writer;
// receiving decodes from the reader within the read callback. The server here
// decodes each message and replies with a JSON message of its own.
func Example_json() {
	type message struct {
		Greeting string `json:"greeting"`
		N        int    `json:"n"`
	}

	// Server: decode each message, then reply with a JSON message.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Upgrade(w, r, nil)
		if err != nil {
			return // Upgrade has already written an error response
		}
		defer c.Close()
		for {
			var in message
			err := c.Read(nil, func(rd *websocket.Reader) error {
				return json.NewDecoder(rd).Decode(&in)
			})
			if err != nil {
				return
			}
			reply := message{Greeting: "hi " + in.Greeting, N: in.N + 1}
			err = c.Write(context.Background(), websocket.MessageText, nil, func(wr *websocket.Writer) error {
				return json.NewEncoder(wr).Encode(reply)
			})
			if err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, _, err := websocket.Dial(context.Background(), url, nil, nil)
	if err != nil {
		fmt.Println("dial:", err)
		return
	}
	defer c.Close()

	// Send: encode a value as one text message.
	err = c.Write(context.Background(), websocket.MessageText, nil, func(w *websocket.Writer) error {
		return json.NewEncoder(w).Encode(message{Greeting: "there", N: 1})
	})
	if err != nil {
		fmt.Println("write:", err)
		return
	}

	// Receive: decode the reply into a value.
	var in message
	err = c.Read(nil, func(r *websocket.Reader) error {
		return json.NewDecoder(r).Decode(&in)
	})
	if err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Printf("%s %d\n", in.Greeting, in.N)

	// Output: hi there 2
}
