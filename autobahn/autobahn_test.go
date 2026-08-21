// Package autobahn runs this WebSocket package against the Autobahn|Testsuite
// (https://github.com/crossbario/autobahn-testsuite), the standard conformance
// oracle for RFC 6455 framing, fragmentation, UTF-8, and close handshakes.
//
// The suite's wstest tool is Python 2 only, so it is run from its Docker image
// (crossbario/autobahn-testsuite); nothing Python is installed locally. The
// echo server and client driver run in-process; only wstest runs in Docker.
//
// The tests are opt-in (they need Docker and take minutes). Enable with:
//
//	AUTOBAHN=1 go test -run Autobahn -v ./autobahn/
//
// TestAutobahnServer points `wstest -m fuzzingclient` at our Upgrade echo
// server; TestAutobahnClient drives our Dial client through
// `wstest -m fuzzingserver`. Browsable results land in
// reports/{servers,clients}/index.html.
package autobahn

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/garyburd/websocket"
)

const (
	agent = "garyburd-websocket"
	port  = 9001
	image = "crossbario/autobahn-testsuite"
)

// The suite's 9.* cases exchange messages up to 16 MiB, so allow that much
// inbound and use a large write buffer.
var (
	dialOpts = &websocket.DialOptions{
		MaxMessageSize:  16 << 20,
		WriteBufferSize: 64 << 10,
	}
	upgradeOpts = &websocket.UpgradeOptions{
		MaxMessageSize:  16 << 20,
		WriteBufferSize: 64 << 10,
	}
)

// requireSuite skips unless AUTOBAHN is set and Docker is available.
//
// An env-var gate is used instead of testing.Short() because the constraint
// is an external dependency, not duration: -short semantics would make
// Docker required by default and skipped on request, when running the suite
// must be the deliberate act.
func requireSuite(t *testing.T) {
	if os.Getenv("AUTOBAHN") == "" {
		t.Skip("set AUTOBAHN=1 to run the Autobahn|Testsuite (needs Docker)")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found in PATH")
	}
}

// TestAutobahnServer runs `wstest -m fuzzingclient` against our echo server.
func TestAutobahnServer(t *testing.T) {
	requireSuite(t)
	reports := reportsDir(t)

	srv := &http.Server{Handler: echoHandler()}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })

	var args []string
	if runtime.GOOS == "linux" {
		// host.docker.internal is built in on Docker Desktop; Linux needs it.
		args = append(args, "--add-host=host.docker.internal:host-gateway")
	}
	args = append(args,
		"-v", confDir(t)+":/config",
		"-v", reports+":/reports",
		image, "wstest", "-m", "fuzzingclient", "-s", "/config/fuzzingclient.json")
	dockerRun(t, args...)

	checkReport(t, filepath.Join(reports, "servers", "index.json"))
}

// TestAutobahnClient drives our Dial client through `wstest -m fuzzingserver`.
func TestAutobahnClient(t *testing.T) {
	requireSuite(t)
	reports := reportsDir(t)

	const name = "autobahn-fuzzingserver"
	exec.Command("docker", "rm", "-f", name).Run() // remove any stale container
	cmd := exec.Command("docker", "run", "--rm", "--name", name,
		"-p", fmt.Sprintf("%d:%d", port, port),
		"-v", confDir(t)+":/config",
		"-v", reports+":/reports",
		image, "wstest", "-m", "fuzzingserver", "-s", "/config/fuzzingserver.json")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		exec.Command("docker", "stop", name).Run()
		cmd.Wait()
	})

	driveClient(t, fmt.Sprintf("ws://127.0.0.1:%d", port))
	time.Sleep(time.Second) // let the server flush its report

	checkReport(t, filepath.Join(reports, "clients", "index.json"))
}

// echoHandler upgrades and echoes; it's what the fuzzingclient connects to.
func echoHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Upgrade(w, r, upgradeOpts)
		if err != nil {
			return
		}
		echo(c)
	})
}

// echo reads messages and writes them back until the peer closes. The library
// validates UTF-8 on text messages itself, failing an invalid one with a 1007
// close (the suite's 6.* cases require this), so the read error just ends the
// loop here.
func echo(c *websocket.Conn) {
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
}

func drain(c *websocket.Conn) {
	for {
		if _, _, err := c.ReadMessage(nil); err != nil {
			return
		}
	}
}

// driveClient runs every case the fuzzingserver offers, then asks it to write
// its report.
func driveClient(t *testing.T, server string) {
	n := caseCount(t, server)
	t.Logf("running %d cases", n)
	for i := 1; i <= n; i++ {
		echo(dial(t, fmt.Sprintf("%s/runCase?case=%d&agent=%s", server, i, url.QueryEscape(agent))))
	}
	c := dial(t, fmt.Sprintf("%s/updateReports?agent=%s", server, url.QueryEscape(agent)))
	drain(c)
	c.Close()
}

// caseCount asks the fuzzingserver how many cases it offers, retrying the
// handshake until the server answers. A successful TCP connect to the published
// port isn't enough on its own: Docker's port proxy accepts the connection
// before wstest inside the container has bound its listener, so the first
// handshakes get reset (the gap is wider under amd64-on-arm64 emulation).
func caseCount(t *testing.T, server string) int {
	deadline := time.Now().Add(60 * time.Second)
	for {
		n, err := tryCaseCount(server)
		if err == nil {
			return n
		}
		if time.Now().After(deadline) {
			t.Fatalf("getCaseCount: fuzzingserver not ready: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tryCaseCount(server string) (int, error) {
	c, _, err := websocket.Dial(context.Background(), server+"/getCaseCount", dialOpts, nil)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	data, _, err := c.ReadMessage(nil)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(data))
}

func dial(t *testing.T, rawurl string) *websocket.Conn {
	// No context deadline: the suite is local and Dial uses the context only
	// for the handshake (the Conn does not retain it).
	c, _, err := websocket.Dial(context.Background(), rawurl, dialOpts, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", rawurl, err)
	}
	return c
}

// checkReport fails the test for any case the suite reports as FAILED.
func checkReport(t *testing.T, indexPath string) {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report map[string]map[string]struct {
		Behavior      string `json:"behavior"`
		BehaviorClose string `json:"behaviorClose"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse report: %v", err)
	}
	cases := report[agent]
	if len(cases) == 0 {
		t.Fatalf("report %s has no cases for agent %q", indexPath, agent)
	}

	ids := make([]string, 0, len(cases))
	for id := range cases {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return caseLess(ids[i], ids[j]) })

	counts := map[string]int{}
	var failed int
	for _, id := range ids {
		r := cases[id]
		counts[r.Behavior]++
		if r.Behavior == "FAILED" || r.BehaviorClose == "FAILED" {
			failed++
			t.Errorf("case %s: behavior=%s behaviorClose=%s", id, r.Behavior, r.BehaviorClose)
		}
	}
	t.Logf("%d cases (%s); see %s", len(cases), summarize(counts),
		filepath.Join(filepath.Dir(indexPath), "index.html"))
}

func summarize(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, counts[k])
	}
	return strings.Join(parts, " ")
}

// caseLess orders dotted case ids ("1.2.11") numerically.
func caseLess(a, b string) bool {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		ai, _ := strconv.Atoi(as[i])
		bi, _ := strconv.Atoi(bs[i])
		if ai != bi {
			return ai < bi
		}
	}
	return len(as) < len(bs)
}

func dockerRun(t *testing.T, args ...string) {
	cmd := exec.Command("docker", append([]string{"run", "--rm"}, args...)...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker run: %v", err)
	}
}

func confDir(t *testing.T) string { return absDir(t, "config") }

func reportsDir(t *testing.T) string {
	dir := absDir(t, "reports")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func absDir(t *testing.T, name string) string {
	p, err := filepath.Abs(name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
