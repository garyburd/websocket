// Command command runs a subprocess and bridges its standard input, output,
// and error streams to a websocket connection, demonstrating the websocket
// package and a [MessageScanner] read loop. See README.md for running
// instructions and a guide to the source.
//
// Security: the server runs a subprocess driven by network input. It is a
// demonstration only; do not expose it on an untrusted network.
package main

import (
	"bufio"
	"context"
	_ "embed"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/garyburd/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Send a keepalive ping to the peer after this much inbound silence.
	keepaliveInterval = 54 * time.Second

	// Time allowed to send a keepalive ping and receive the peer's pong.
	keepaliveTimeout = 10 * time.Second

	// Maximum message size allowed from the peer.
	maxMessageSize = 8192
)

// Outbound messages are prefixed with a stream tag so the browser can style
// the command's standard output and standard error differently.
const (
	tagStdout = '1'
	tagStderr = '2'
)

// Inbound messages are likewise prefixed with a tag byte: the rest is a line
// for the command's stdin, or the message is an end-of-input control.
const (
	tagInput      = '0' // message body is a line for the command's stdin
	tagCloseInput = 'X' // end of input: close the command's stdin
)

var upgradeOptions = &websocket.UpgradeOptions{
	MaxMessageSize:      maxMessageSize,
	ControlReplyTimeout: writeWait, // bounds writing the automatic pong and close replies
	ShutdownTimeout:     writeWait, // budget for the closing handshake (close write + echo wait)
}

// The command to run for each connection, set from the command line.
var (
	commandName string
	commandArgs []string
)

// home.html is embedded so the example runs from any working directory.
//
//go:embed home.html
var homeHTML []byte

func serveHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(homeHTML)
}

// serveWs upgrades the request, launches the command, and bridges it to the
// websocket: inbound messages go to the command's stdin, and the command's
// stdout and stderr come back as messages.
func serveWs(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Upgrade(w, r, upgradeOptions)
	if err != nil {
		// Upgrade has already written an error response.
		log.Println(err)
		return
	}

	cmd := exec.Command(commandName, commandArgs...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Printf("stdin pipe: %v", err)
		conn.Close()
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("stdout pipe: %v", err)
		conn.Close()
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("stderr pipe: %v", err)
		conn.Close()
		return
	}
	if err := cmd.Start(); err != nil {
		log.Printf("start %q: %v", commandName, err)
		conn.Shutdown(websocket.CloseInternalError, "command failed to start")
		conn.Close()
		return
	}

	// Pump stdout and stderr to the browser, each tagged so the browser can
	// style them apart; the package allows concurrent writers, so both pumps
	// write while inputPump reads. When both streams close — the command has
	// exited — a third goroutine begins the closing handshake; inputPump
	// collects the peer's echo and serveWs closes the connection below.
	var streams sync.WaitGroup
	streams.Add(2)
	go func() { defer streams.Done(); streamPump(conn, stdout, tagStdout) }()
	go func() { defer streams.Done(); streamPump(conn, stderr, tagStderr) }()
	go func() {
		streams.Wait()
		conn.Shutdown(websocket.CloseNormalClosure, "")
	}()

	inputPump(conn, stdin)

	// The read loop ended, so the connection is finished. Stop the command
	// (closing stdin lets a well-behaved command exit; Kill covers the rest),
	// wait for the stream pumps to finish reading before reaping the process
	// (required by StdoutPipe/StderrPipe), then release the connection.
	stdin.Close()
	cmd.Process.Kill()
	streams.Wait()
	cmd.Wait()
	conn.Close()
}

// inputPump dispatches each inbound message on its leading tag byte: tagInput
// copies the rest into the command's stdin as one line; tagCloseInput closes
// stdin so the command sees EOF (some commands flush final output or exit on
// EOF), while the read loop keeps running so that output still streams back.
//
// It owns the connection's single read loop, configured here with keepalive:
// while it waits for the next message, a ping is sent after keepaliveInterval
// of silence and the read fails if the ping and the peer's pong do not complete
// within keepaliveTimeout. Browsers answer pings automatically, so an idle but
// live connection stays open while a vanished one is detected.
func inputPump(conn *websocket.Conn, stdin io.WriteCloser) {
	opts := &websocket.ReadOptions{
		KeepaliveInterval: keepaliveInterval,
		KeepaliveTimeout:  keepaliveTimeout,
		MatchPong:         true,
	}
	stdinOpen := true
	s := newMessageScanner(conn, opts)
	for r := range s.All() {
		var tag [1]byte
		if _, err := io.ReadFull(r, tag[:]); err != nil {
			continue // empty message; no tag to act on
		}
		if tag[0] == tagCloseInput {
			// Close stdin once, then keep reading so output still streams back.
			if stdinOpen {
				stdin.Close()
				stdinOpen = false
			}
			continue
		}
		if tag[0] != tagInput || !stdinOpen {
			continue // unknown tag, or input already ended; ignore
		}
		// Stream the rest of the message straight into stdin (no intermediate
		// buffer), then a newline so the command reads one line per message.
		if _, err := io.Copy(stdin, r); err != nil {
			break
		}
		if _, err := io.WriteString(stdin, "\n"); err != nil {
			break
		}
	}
	// The loop ended on a read error (or a stdin write failed, in which case
	// Err is nil). A *CloseError (the peer closed, or echoed the Shutdown sent
	// after the command exited) and ErrClosed are expected; log anything else.
	if err := s.Err(); err != nil {
		var ce *websocket.CloseError
		if !errors.As(err, &ce) && !errors.Is(err, websocket.ErrClosed) {
			log.Printf("read: %v", err)
		}
	}
}

// streamPump reads r a line at a time and sends each line as a text message,
// prefixed with tag so the browser can tell the command's stdout (tagStdout)
// and stderr (tagStderr) apart and style them differently.
//
// Text messages assume the command's output is valid UTF-8, as the example
// commands produce. Output with non-UTF-8 bytes would form an invalid text
// message and the browser would close the connection with status 1007; handling
// arbitrary bytes would need binary messages or scrubbing instead.
func streamPump(conn *websocket.Conn, r io.Reader, tag byte) {
	opts := &websocket.WriteOptions{MessageTimeout: writeWait}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 4096), maxMessageSize)
	for sc.Scan() {
		msg := append([]byte{tag}, sc.Bytes()...)
		if err := conn.WriteMessage(context.Background(), websocket.MessageText, opts, msg); err != nil {
			return // connection gone; inputPump owns Close
		}
	}
}

func main() {
	addr := flag.String("addr", "localhost:8080", "http service address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		// Default: an interactive calculator. -i keeps bc running and reporting
		// errors (on stderr) after a bad expression; -l loads the math library.
		args = []string{"bc", "-i", "-l"}
	}
	commandName, commandArgs = args[0], args[1:]

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", serveHome)
	mux.HandleFunc("GET /ws", serveWs)

	log.Printf("listening on http://%s, running %q", *addr, commandName)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
