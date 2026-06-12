package websocket

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestMain runs the package tests and then fails the run if any non-harness
// goroutine is still alive. The package starts no goroutines of its own (its
// watchdogs are time.AfterFunc timers, which share the runtime timer goroutine),
// so a straggler means either a Conn began leaking a goroutine or a test left a
// peer goroutine blocked because the connection was never torn down. Done
// in-package, by scanning runtime.Stack, to avoid a test-only module dependency.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if gs := leakedGoroutines(); len(gs) > 0 {
			fmt.Fprintf(os.Stderr, "found %d leaked goroutine(s):\n\n%s\n",
				len(gs), strings.Join(gs, "\n\n"))
			code = 1
		}
	}
	os.Exit(code)
}

// leakedGoroutines returns the stacks of goroutines still running that are not
// part of the test harness or runtime. It retries briefly: a goroutine the final
// test's t.Cleanup just unblocked may take a moment to exit.
func leakedGoroutines() []string {
	for try := 0; ; try++ {
		gs := interestingGoroutines()
		if len(gs) == 0 || try >= 10 {
			return gs
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// interestingGoroutines returns the stack of every goroutine except the test
// harness, the runtime's own goroutines, and this leak check itself.
func interestingGoroutines() []string {
	var gs []string
	for g := range strings.SplitSeq(string(stackDump()), "\n\n") {
		_, stack, ok := strings.Cut(g, "\n")
		if !ok {
			continue
		}
		stack = strings.TrimSpace(stack)
		if stack == "" ||
			strings.Contains(stack, "testing.") ||
			strings.Contains(stack, "runtime.goexit") ||
			strings.Contains(stack, "os/signal.signal_recv") ||
			strings.Contains(stack, "created by runtime.") ||
			strings.Contains(stack, "websocket.TestMain") ||
			strings.Contains(stack, "websocket.leakedGoroutines") ||
			strings.Contains(stack, "websocket.interestingGoroutines") {
			continue
		}
		gs = append(gs, g)
	}
	sort.Strings(gs)
	return gs
}

// stackDump returns runtime.Stack for all goroutines, growing the buffer until
// the dump is not truncated.
func stackDump() []byte {
	buf := make([]byte, 64<<10)
	for {
		if n := runtime.Stack(buf, true); n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
