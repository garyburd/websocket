package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// MessageType identifies a binary or text message. Its zero value is
// [MessageBinary].
type MessageType int

const (
	MessageBinary MessageType = iota
	MessageText
)

// String returns the type name or its decimal value.
func (m MessageType) String() string {
	switch m {
	case MessageBinary:
		return "Binary"
	case MessageText:
		return "Text"
	default:
		return strconv.Itoa(int(m))
	}
}

const (
	defaultWriteBufferSize = 4096
	rateGrace              = time.Second
	minWriteBufferSize     = 64 // must exceed maxHeaderLen
)

var (
	// ErrClosed reports local teardown by [Conn.Close], [Reader.Abort], or a
	// panicking callback. A timed-out [Conn.Shutdown] reports [ErrTimeout].
	ErrClosed = errors.New("websocket: connection closed")

	// ErrTimeout reports a liveness or I/O deadline failure.
	ErrTimeout = errors.New("websocket: timeout")

	// ErrProtocol reports a peer protocol violation.
	ErrProtocol = errors.New("websocket: protocol error")

	// ErrBadHandshake reports a rejected opening handshake. [Dial] and [Upgrade]
	// wrap it with details.
	ErrBadHandshake = errors.New("websocket: bad handshake")

	// ErrCloseSent reports a data write attempted after this endpoint sent a
	// close frame.
	ErrCloseSent = errors.New("websocket: cannot write after close")

	// ErrInvalidArgument reports a rejected argument or option. The connection
	// remains usable.
	ErrInvalidArgument = errors.New("websocket: invalid argument")

	errReaderScope        = fmt.Errorf("%w: reader used after close", ErrInvalidArgument)
	errWriterScope        = fmt.Errorf("%w: writer used after close", ErrInvalidArgument)
	errReaderAborted      = fmt.Errorf("%w: reader aborted", ErrClosed)
	errInvalidCloseCode   = fmt.Errorf("%w: close code not allowed on a close frame", ErrInvalidArgument)
	errInvalidCloseReason = fmt.Errorf("%w: close reason is not valid UTF-8", ErrInvalidArgument)
	errInvalidMessageType = fmt.Errorf("%w: message type must be MessageText or MessageBinary", ErrInvalidArgument)

	errWriteTimeout = fmt.Errorf("%w: write timeout", ErrTimeout)

	errReadTimeout = fmt.Errorf("%w: read timeout", ErrTimeout)

	errShutdownTimeout = fmt.Errorf("%w: close handshake not completed", ErrTimeout)

	errWriteUnstarted = errors.New("websocket: write not started")

	errConcurrentRead = fmt.Errorf("%w: concurrent read", ErrInvalidArgument)

	errCallbackPanic = fmt.Errorf("%w: callback panicked", ErrClosed)

	errSetReadDeadlineRate  = fmt.Errorf("%w: SetReadDeadline conflicts with MinReadRate", ErrInvalidArgument)
	errSetWriteDeadlineRate = fmt.Errorf("%w: SetWriteDeadline conflicts with MinWriteRate", ErrInvalidArgument)
)

// bufferPools shares buffers among connections with the same buffer size.
var bufferPools sync.Map

func bufferPoolForSize(n int) *sync.Pool {
	if v, ok := bufferPools.Load(n); ok {
		return v.(*sync.Pool)
	}
	p := &sync.Pool{New: func() any {
		b := make([]byte, n)
		return &b
	}}
	actual, _ := bufferPools.LoadOrStore(n, p)
	return actual.(*sync.Pool)
}

const (
	rsIdle = iota
	rsAwaitingData
	rsBody
)

// readMonitor owns the read liveness state and watchdog.
//
// Lifecycle methods lock mu. Helpers called through lockedMonitor require mu.
// Lock order is reader.mu before mu. fireAt rejects callbacks from prior arms.
type readMonitor struct {
	mu sync.Mutex

	readState int

	// Armed only during a read: with no read in progress, no goroutine can
	// consume a pong, and a running timer would kill a quiet, live connection.
	// fireAt is zero while disarmed.
	timer  *time.Timer
	fireAt time.Time

	// Copied from ReadOptions for the active read.
	keepaliveInterval time.Duration
	keepaliveTimeout  time.Duration
	idleDeadline      time.Time
	msgTimeout        time.Duration
	readRate          int
	readDeadline      time.Time
}

func (c *Conn) lockedMonitor() *readMonitor {
	c.monitor.mu.Lock()
	return &c.monitor
}

func (m *readMonitor) unlock() { m.mu.Unlock() }

func (m *readMonitor) init(cb func()) {
	m.timer = time.AfterFunc(time.Hour, cb)
	m.timer.Stop()
}

// arm sets the watchdog; a zero deadline disarms it.
func (m *readMonitor) arm(deadline time.Time) {
	m.fireAt = deadline
	if deadline.IsZero() {
		m.timer.Stop()
	} else {
		m.timer.Reset(time.Until(deadline))
	}
}

func (m *readMonitor) disarm() { m.arm(time.Time{}) }

// stale reports a callback from a prior arm.
func (m *readMonitor) stale() bool {
	return m.fireAt.IsZero() || time.Now().Before(m.fireAt)
}

// terminateConn publishes the first terminal error, then closes the transport.
// Read and write halves adopt the published error when they unblock.
func (c *Conn) terminateConn(err error) error {
	if c.terminalErr.CompareAndSwap(nil, &err) {
		c.shutdownTimer.Stop()
		return c.rwc.Close()
	}
	return nil
}

// terminateRead records the sticky read error. reader.mu is held.
func (c *Conn) terminateRead(err error) error {
	if c.reader.terminalErr != nil {
		return c.reader.terminalErr
	}
	m := c.lockedMonitor()
	m.disarm()
	m.unlock()
	var pe *protocolError
	var ce *CloseError
	switch {
	case errors.As(err, &pe):
	case errors.As(err, &ce):
	default:
		if e := c.loadTerminalErr(); e != nil {
			err = e
		}
	}
	if err == nil {
		err = ErrClosed
	}
	c.reader.terminalErr = err
	if pe != nil {
		// A failed protocol-close response does not replace the peer error.
		_ = c.writeControlFrame(opClose, buildClosePayload(pe.code, pe.text), c.frame.controlDeadline())
	}
	return err
}

// terminateWrite records the sticky write error. frame.mu is held.
func (c *Conn) terminateWrite(err error) error {
	if c.frame.terminalErr == nil {
		if e := c.loadTerminalErr(); e != nil {
			err = e
		}
		if err == nil {
			err = ErrClosed
		}
		c.frame.terminalErr = err
	}
	return c.frame.terminalErr
}

// armReadWait selects the earliest keepalive or idle deadline. mu is held.
func (m *readMonitor) armReadWait(now time.Time) {
	var fire time.Time
	switch m.readState {
	case rsIdle:
		if m.keepaliveInterval > 0 {
			fire = now.Add(m.keepaliveInterval)
		}
	case rsAwaitingData:
		if m.keepaliveTimeout > 0 {
			fire = now.Add(m.keepaliveTimeout)
		}
	}
	if !m.idleDeadline.IsZero() && (fire.IsZero() || m.idleDeadline.Before(fire)) {
		fire = m.idleDeadline
	}
	m.arm(fire)
}

// startRead installs options and arms the initial wait.
func (m *readMonitor) startRead(opts *ReadOptions) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	m.keepaliveInterval, m.keepaliveTimeout = 0, 0
	m.msgTimeout, m.readRate = 0, 0
	m.idleDeadline = time.Time{}
	if opts != nil {
		m.keepaliveInterval = opts.KeepaliveInterval
		m.keepaliveTimeout = opts.KeepaliveTimeout
		m.msgTimeout = opts.MessageTimeout
		m.readRate = opts.MinReadRate
		if opts.IdleTimeout > 0 {
			m.idleDeadline = now.Add(opts.IdleTimeout)
		}
	}
	m.readState = rsIdle
	m.readDeadline = time.Time{}
	m.armReadWait(now)
}

// beginBody replaces wait liveness with the message bound.
func (m *readMonitor) beginBody() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readState = rsBody
	now := time.Now()
	switch {
	case m.msgTimeout > 0:
		m.readDeadline = now.Add(m.msgTimeout)
	case m.readRate > 0:
		m.readDeadline = now.Add(rateGrace)
	default:
		m.readDeadline = time.Time{}
	}
	m.arm(m.readDeadline)
}

// noteControl refreshes keepalive without extending the idle deadline.
func (m *readMonitor) noteControl() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readState == rsBody {
		return
	}
	m.readState = rsIdle
	m.armReadWait(time.Now())
}

// beginFill grants n bytes of rate allowance without crediting them.
func (m *readMonitor) beginFill(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readRate <= 0 || m.readState != rsBody {
		return
	}
	m.arm(m.readDeadline.Add(rateAllowance(m.readRate, n)))
}

// endFill credits a completed fill and re-arms the rate deadline.
func (m *readMonitor) endFill(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readRate <= 0 || m.readState != rsBody {
		return
	}
	m.readDeadline = rollRateDeadline(m.readDeadline, m.readRate, n, time.Now())
	m.arm(m.readDeadline)
}

// endBody disarms body progress checks once the final payload is consumed.
// The scoped Read callback may continue doing application work afterward.
func (m *readMonitor) endBody() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readState != rsBody {
		return
	}
	m.readDeadline = time.Time{}
	m.disarm()
}

// bodyBuffered reports whether the final frame's complete payload has already
// arrived from the transport, even if the scoped Reader has not consumed it.
func (c *Conn) bodyBuffered() bool {
	return c.reader.final && c.br.Buffered() >= c.reader.remaining
}

// endRead clears all active read deadlines. It is the universal disarm:
// terminateConn does not touch the watchdog, so every read-exit path must
// pass through here.
func (m *readMonitor) endRead() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readState = rsIdle
	m.readDeadline = time.Time{}
	m.idleDeadline = time.Time{}
	m.disarm()
}

// onReadTimer advances keepalive or terminates an expired read.
func (c *Conn) onReadTimer() {
	m := c.lockedMonitor()
	if m.stale() {
		m.unlock()
		return
	}
	now := time.Now()
	switch {
	case m.readState == rsBody:
		c.readTimedOutLocked(m, "message body stalled")
	case !m.idleDeadline.IsZero() && !now.Before(m.idleDeadline):
		c.readTimedOutLocked(m, "no message within idle timeout")
	case m.readState == rsAwaitingData:
		c.readTimedOutLocked(m, "keepalive ping not answered")
	case m.readState == rsIdle:
		// Hard keepalive waits for a response; soft keepalive only sends pings.
		hardKeepalive := m.keepaliveTimeout > 0
		if hardKeepalive {
			m.readState = rsAwaitingData
		}
		m.armReadWait(now)
		// Sending and receiving share the keepalive budget. The watchdog
		// deadline, not controlDeadline, bounds the send: a large application
		// frame can hold frame.mu past a control write's budget, and a healthy
		// transfer must not trip a teardown.
		sendDeadline := m.fireAt
		m.unlock()
		if err := c.writeControlFrame(opPing, c.pingPayload[:], sendDeadline); err != nil {
			// A missed soft ping is retried on the next interval.
			if hardKeepalive || !errors.Is(err, ErrTimeout) {
				c.terminateConn(err)
			}
		}
	default:
		m.unlock()
	}
}

// readTimedOutLocked releases mu and terminates the connection.
func (c *Conn) readTimedOutLocked(m *readMonitor, reason string) {
	m.unlock()
	c.terminateConn(fmt.Errorf("%w: %s", errReadTimeout, reason))
}

// lock acquires the frame lock before deadline.
func (f *frame) lock(deadline time.Time) bool {
	select {
	case f.mu <- struct{}{}:
		return true
	default:
	}
	if deadline.IsZero() {
		f.mu <- struct{}{}
		return true
	}
	t := time.NewTimer(time.Until(deadline))
	defer t.Stop()
	select {
	case f.mu <- struct{}{}:
		return true
	case <-t.C:
		return false
	}
}

func (f *frame) unlock() { <-f.mu }

// writeFrame writes one serialized frame before deadline.
//
// Pre-write failures are non-terminal. A failure after writing begins is
// terminal because the peer may have received a partial frame.
func (c *Conn) writeFrame(op int, fin bool, buf, overflow []byte, deadline time.Time) error {
	if !c.frame.lock(deadline) {
		return errWriteTimeout
	}
	defer c.frame.unlock()
	if c.frame.terminalErr != nil {
		return c.frame.terminalErr
	}
	// Ping and Pong remain valid while a close handshake is pending.
	if c.frame.closeSent && op != opPing && op != opPong {
		return ErrCloseSent
	}
	armed := !deadline.IsZero()
	if armed {
		if !deadline.After(time.Now()) {
			return errWriteTimeout
		}
		c.frame.timer.Reset(time.Until(deadline))
	}
	err := c.writeFrameLocked(op, fin, buf, overflow)
	if armed {
		if !c.frame.timer.Stop() {
			// Prefer the timer error over the resulting transport error.
			err = errWriteTimeout
		}
	}
	if err != nil {
		c.terminateConn(err)
		return c.terminateWrite(err)
	}
	if op == opClose {
		c.frame.closeSent = true
	}
	return nil
}

// onFrameTimer terminates an overdue frame write. No stale guard: the timer
// is armed and stopped under frame.mu around one write, so a fire always
// means that write overran.
func (c *Conn) onFrameTimer() {
	c.terminateConn(errWriteTimeout)
}

// writeControlFrame writes one control frame before deadline.
func (c *Conn) writeControlFrame(op int, payload []byte, deadline time.Time) error {
	if len(payload) > maxControlPayload {
		return fmt.Errorf("websocket: control payload too large (%d)", len(payload))
	}
	return c.writeFrame(op, true, c.frame.control[:maxHeaderLen], payload, deadline)
}

// controlDeadline applies ControlReplyTimeout to an automatic response.
func (f *frame) controlDeadline() time.Time {
	if f.writeTimeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(f.writeTimeout)
}

// rateAllowance computes n/rate without duration overflow.
func rateAllowance(rate, n int) time.Duration {
	const max = uint64(math.MaxInt64)
	second := uint64(time.Second)
	whole := uint64(n / rate)
	if whole > max/second {
		return time.Duration(math.MaxInt64)
	}

	ns := whole * second
	hi, lo := bits.Mul64(uint64(n%rate), second)
	fraction, _ := bits.Div64(hi, lo, uint64(rate))
	if fraction > max-ns {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ns + fraction)
}

// rollRateDeadline credits completed bytes without accumulating extra slack.
func rollRateDeadline(deadline time.Time, rate, n int, now time.Time) time.Time {
	deadline = deadline.Add(rateAllowance(rate, n))
	if cap := now.Add(rateGrace); deadline.After(cap) {
		deadline = cap
	}
	return deadline
}

// rateDeadline grants allowance before transfer and caps carried slack.
func rateDeadline(deadline time.Time, rate, n int, now time.Time) time.Time {
	if cap := now.Add(rateGrace); deadline.After(cap) {
		deadline = cap
	}
	return deadline.Add(rateAllowance(rate, n))
}

// Conn is a WebSocket connection created by [Dial] or [Upgrade]. One goroutine
// may read while any number of goroutines write concurrently.
type Conn struct {
	rwc io.ReadWriteCloser
	br  *bufio.Reader

	// isServer controls frame masking.
	isServer bool

	maxMessageSize int

	manualCloseResponse bool

	shutdownTimeout time.Duration

	// shutdownTimer bounds the peer close response.
	shutdownTimer *time.Timer

	// Published before closing rwc so blocked operations report the cause.
	terminalErr atomic.Pointer[error]

	subprotocol string

	// Stable payload used to match keepalive pongs.
	pingPayload [8]byte

	bufferPool *sync.Pool

	// Lock order: reader.mu or writer.mu, then frame.mu, then monitor.mu.
	reader  reader
	writer  writer
	frame   frame
	monitor readMonitor
}

func (c *Conn) loadTerminalErr() error {
	if p := c.terminalErr.Load(); p != nil {
		return *p
	}
	return nil
}

// reader holds one in-flight inbound message.
type reader struct {
	mu        chan struct{}
	opcode    int
	remaining int
	declared  int
	final     bool
	err       error
	maskKey   [4]byte
	maskPos   int

	matchPong bool
	rateMode  bool

	validateUTF8 bool
	utf8State    uint32

	// Sticky across messages; guarded by mu.
	terminalErr error
}

// writer holds one in-flight outbound message.
type writer struct {
	mu      chan struct{}
	opcode  int
	bufp    *[]byte
	n       int
	started bool
	final   bool
	err     error

	deadline       time.Time
	rate           int
	messageTimeout time.Duration
}

// frame serializes outbound frames.
type frame struct {
	// A channel permits deadline-bound acquisition.
	mu chan struct{}

	// Bounds transport writes while mu is held.
	timer *time.Timer

	control [maxHeaderLen + maxControlPayload]byte

	writeTimeout time.Duration

	// Guarded by mu.
	closeSent bool

	// Sticky; guarded by mu.
	terminalErr error
}

type connConfig struct {
	isServer            bool
	controlReplyTimeout time.Duration
	shutdownTimeout     time.Duration
	maxMessageSize      int
	writeBufferSize     int
	manualCloseResponse bool
}

// newConn wraps an upgraded transport. br may contain post-handshake bytes.
func newConn(rwc io.ReadWriteCloser, br *bufio.Reader, cfg connConfig) *Conn {
	if br == nil {
		br = bufio.NewReaderSize(rwc, 256)
	}
	maxMsg := cfg.maxMessageSize
	if maxMsg <= 0 {
		maxMsg = math.MaxInt
	}
	bufSize := cfg.writeBufferSize
	if bufSize == 0 {
		bufSize = defaultWriteBufferSize
	} else if bufSize < minWriteBufferSize {
		bufSize = minWriteBufferSize
	}
	c := &Conn{
		rwc:                 rwc,
		br:                  br,
		isServer:            cfg.isServer,
		maxMessageSize:      maxMsg,
		manualCloseResponse: cfg.manualCloseResponse,
		shutdownTimeout:     cfg.shutdownTimeout,
		bufferPool:          bufferPoolForSize(bufSize),
	}
	c.reader.mu = make(chan struct{}, 1)
	c.writer.mu = make(chan struct{}, 1)
	c.frame.mu = make(chan struct{}, 1)
	c.frame.writeTimeout = cfg.controlReplyTimeout
	// Timers start stopped.
	c.frame.timer = time.AfterFunc(time.Hour, c.onFrameTimer)
	c.frame.timer.Stop()
	c.shutdownTimer = time.AfterFunc(time.Hour, c.onShutdownTimer)
	c.shutdownTimer.Stop()
	randFill(c.pingPayload[:])
	c.monitor.init(c.onReadTimer)
	return c
}

// Subprotocol returns the negotiated subprotocol, or "" if none.
func (c *Conn) Subprotocol() string { return c.subprotocol }

// NetConn returns the underlying network connection, or nil when unavailable.
//
// The [Conn] owns the result. Do not read, write, close, or set deadlines on it.
func (c *Conn) NetConn() net.Conn {
	if nc, ok := c.rwc.(net.Conn); ok {
		return nc
	}
	if u, ok := c.rwc.(interface{ NetConn() net.Conn }); ok {
		return u.NetConn()
	}
	return nil
}

// Read passes the next message to fn. The [Reader] is valid only during fn;
// unread payload is discarded afterward unless [Reader.Abort] is called.
//
// An fn error is returned unchanged, so applications can match their own
// errors with errors.Is or errors.As; unless fn failed a read or called
// [Reader.Abort], the connection remains usable. [ErrInvalidArgument]
// (invalid options or a concurrent read; reads are single-goroutine) also
// leaves the connection untouched. Any other error is terminal and sticky:
// every later Read returns it, so stop the read loop.
//
// A panic in fn tears down the connection and propagates.
func (c *Conn) Read(opts *ReadOptions, fn func(r *Reader) error) error {
	r, err := c.nextReader(opts)
	if err != nil {
		return err
	}
	panicked := true
	defer func() {
		if panicked {
			// The message cannot be drained safely after a panic.
			c.terminateConn(errCallbackPanic)
			r.close()
		}
	}()
	err = fn(r)
	panicked = false
	closeErr := r.close()
	if err == nil {
		err = closeErr
	}
	return err
}

// nextReader acquires reader.mu and starts the next message.
func (c *Conn) nextReader(opts *ReadOptions) (*Reader, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	select {
	case c.reader.mu <- struct{}{}:
	default:
		return nil, errConcurrentRead
	}
	if c.reader.terminalErr != nil {
		err := c.reader.terminalErr
		<-c.reader.mu
		return nil, err
	}
	c.reader.matchPong = opts != nil && opts.MatchPong
	c.reader.rateMode = opts != nil && opts.MinReadRate > 0
	c.reader.validateUTF8 = opts == nil || !opts.SkipUTF8Validation
	c.monitor.startRead(opts)
	if err := c.readFragment(true); err != nil {
		c.monitor.endRead()
		<-c.reader.mu
		return nil, err
	}
	c.monitor.beginBody()
	if c.bodyBuffered() {
		c.monitor.endBody()
	}
	return &Reader{c: c}, nil
}

// readFragment consumes controls and installs the next data fragment.
// reader.mu is held; first requires a new data message.
func (c *Conn) readFragment(first bool) error {
	m := &c.reader

	var h frameHeader
	for {
		var err error
		h, err = readHeader(c.br, c.isServer)
		if err != nil {
			if err == io.EOF {
				// EOF between frames is an abnormal close.
				err = &CloseError{Code: CloseAbnormalClosure}
			}
			return c.terminateRead(err)
		}
		if !isControlOp(h.opcode) {
			break
		}
		if err := c.handleControlFrame(h); err != nil {
			return err
		}
	}

	if first {
		if !isDataOp(h.opcode) {
			return c.terminateRead(peerError(CloseProtocolError, "expected data frame, got opcode %d", h.opcode))
		}
		m.declared = 0
		m.err = nil
		m.opcode = h.opcode
		m.utf8State = utf8Accept
	} else if h.opcode != opContinuation {
		return c.terminateRead(peerError(CloseProtocolError, "expected continuation, got opcode %d", h.opcode))
	}

	// Subtraction avoids overflow in the cumulative size.
	if h.length > c.maxMessageSize-m.declared {
		return c.terminateRead(peerError(CloseMessageTooBig, "message exceeds MaxMessageSize"))
	}
	m.declared += h.length
	m.remaining = h.length
	m.final = h.fin
	m.maskKey = h.maskKey
	m.maskPos = 0
	return nil
}

// Reader streams one inbound message. It is valid only during its [Conn.Read]
// callback.
type Reader struct{ c *Conn }

// Type returns [MessageBinary] or [MessageText].
func (r *Reader) Type() MessageType {
	if r.c != nil && r.c.reader.opcode == opText {
		return MessageText
	}
	return MessageBinary
}

// SetReadDeadline sets the absolute deadline for the current message body. A
// zero value removes the deadline. It overrides [ReadOptions.MessageTimeout].
//
// Expiry is terminal. SetReadDeadline conflicts with [ReadOptions.MinReadRate].
func (r *Reader) SetReadDeadline(t time.Time) error {
	c := r.c
	if c == nil {
		return errReaderScope
	}
	if c.reader.terminalErr != nil {
		return c.reader.terminalErr
	}
	m := c.lockedMonitor()
	defer m.unlock()
	if m.readRate > 0 {
		return errSetReadDeadlineRate
	}
	m.readDeadline = t
	m.arm(m.readDeadline)
	return nil
}

// Abort stops reading without draining the current message and sends a
// best-effort close with code and reason.
//
// An invalid code or invalid UTF-8 in reason returns [ErrInvalidArgument]
// without aborting. After a successful Abort, [Conn.Read] returns the callback
// error or [ErrClosed].
func (r *Reader) Abort(code CloseCode, reason string) error {
	c := r.c
	if c == nil {
		return errReaderScope
	}
	if !validWireCloseCode(code) {
		return errInvalidCloseCode
	}
	if !utf8.ValidString(reason) {
		return errInvalidCloseReason
	}
	if c.reader.terminalErr != nil {
		return c.reader.terminalErr
	}
	c.reader.err = errReaderAborted
	c.reader.terminalErr = errReaderAborted
	c.monitor.endRead()
	_ = c.writeControlFrame(opClose, buildClosePayload(code, reason), c.frame.controlDeadline())
	return nil
}

// Read reads message payload and returns [io.EOF] at the message boundary.
func (r *Reader) Read(p []byte) (int, error) {
	c := r.c
	if c == nil {
		return 0, errReaderScope
	}
	m := &c.reader
	if m.err != nil {
		return 0, m.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for m.remaining == 0 {
		if m.final {
			if c.bodyBuffered() {
				c.monitor.endBody()
			}
			if m.validateUTF8 && m.opcode == opText && m.utf8State != utf8Accept {
				m.err = c.terminateRead(peerError(CloseInvalidData, "incomplete UTF-8 sequence in text message"))
				return 0, m.err
			}
			m.err = io.EOF
			return 0, io.EOF
		}
		if err := c.readFragment(false); err != nil {
			m.err = err
			return 0, m.err
		}
	}

	want := min(len(p), m.remaining)
	if m.rateMode {
		c.monitor.beginFill(want)
	}
	n, err := readFull(c.br, p[:want])
	if err == nil && m.rateMode {
		c.monitor.endFill(n)
	}
	if c.isServer && m.maskKey != ([4]byte{}) {
		m.maskPos = maskCopy(p[:n], p[:n], m.maskKey, m.maskPos)
	}
	m.remaining -= n
	if err != nil {
		m.err = c.terminateRead(err)
		return n, m.err
	}
	if m.validateUTF8 && m.opcode == opText {
		m.utf8State = utf8Validate(m.utf8State, p[:n])
		if m.utf8State == utf8Reject {
			m.err = c.terminateRead(peerError(CloseInvalidData, "invalid UTF-8 in text message"))
			return n, m.err
		}
	}
	if c.bodyBuffered() {
		c.monitor.endBody()
	}
	return n, nil
}

// WriteTo copies the remaining message payload to dst.
func (r *Reader) WriteTo(dst io.Writer) (int64, error) {
	c := r.c
	if c == nil {
		return 0, errReaderScope
	}
	if c.reader.err != nil {
		if c.reader.err == io.EOF {
			return 0, nil
		}
		return 0, c.reader.err
	}
	bp := c.bufferPool.Get().(*[]byte)
	defer c.bufferPool.Put(bp)
	buf := *bp
	var total int64
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			wn, werr := dst.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
			if wn < n {
				return total, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return total, nil
			}
			return total, rerr
		}
	}
}

// close drains the message and releases reader.mu.
func (r *Reader) close() error {
	c := r.c
	if c == nil {
		return nil
	}
	defer func() {
		c.monitor.endRead()
		r.c = nil
		<-c.reader.mu
	}()
	_, err := r.WriteTo(io.Discard)
	return err
}

// ReadMessage reads and returns one complete message.
func (c *Conn) ReadMessage(opts *ReadOptions) ([]byte, MessageType, error) {
	r, err := c.nextReader(opts)
	if err != nil {
		return nil, 0, err
	}
	defer r.close()
	data, err := io.ReadAll(r)
	return data, r.Type(), err
}

// Write passes a streaming message writer to fn and finishes the message when
// fn returns.
//
// ctx bounds only writer acquisition; opts bounds the body.
//
// An fn error is returned unchanged, so applications can match their own
// errors with errors.Is or errors.As. [ErrInvalidArgument] and acquisition
// cancellation (errors.Is matches [context.Canceled] or
// [context.DeadlineExceeded]) leave the connection untouched; after any other
// error the connection is unusable. A writer timeout tears it down, and other
// fn errors start a graceful [CloseInternalError] handshake.
//
// Outbound text is not validated; check untrusted text (e.g. [utf8.Valid])
// before sending, or the peer may close with 1007. A panic in fn tears down
// the connection and propagates.
func (c *Conn) Write(ctx context.Context, mt MessageType, opts *WriteOptions, fn func(*Writer) error) error {
	w, err := c.nextWriter(ctx, mt, opts)
	if err != nil {
		return err
	}
	panicked := true
	defer func() {
		if panicked {
			// Prevent close from flushing a truncated message.
			c.writer.err = errCallbackPanic
			c.terminateConn(errCallbackPanic)
			w.close()
		}
	}()
	err = fn(w)
	panicked = false
	// Preserve a writer failure if fn returns a different error. The timeout
	// check below tests writeErr, not err: an application error that merely
	// wraps ErrTimeout still follows the callback-error contract.
	writeErr := c.writer.err
	if err != nil {
		c.writer.err = err
		w.close()
	} else {
		err = w.close()
		writeErr = c.writer.err
	}
	if err != nil {
		if errors.Is(writeErr, ErrTimeout) {
			// Pre-write timeouts have not yet closed the transport.
			c.terminateConn(writeErr)
		} else {
			c.Shutdown(CloseInternalError, "")
		}
	}
	return err
}

// nextWriter acquires writer.mu and initializes one message.
func (c *Conn) nextWriter(ctx context.Context, mt MessageType, opts *WriteOptions) (*Writer, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	var opcode int
	switch mt {
	case MessageText:
		opcode = opText
	case MessageBinary:
		opcode = opBinary
	default:
		return nil, errInvalidMessageType
	}
	select {
	case c.writer.mu <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", errWriteUnstarted, ctx.Err())
	}
	c.writer.opcode = opcode
	c.writer.bufp = c.bufferPool.Get().(*[]byte)
	c.writer.n = 0
	c.writer.started = false
	c.writer.final = false
	c.writer.err = nil
	c.writer.messageTimeout, c.writer.rate = 0, 0
	if opts != nil {
		c.writer.messageTimeout = opts.MessageTimeout
		c.writer.rate = opts.MinWriteRate
	}
	now := time.Now()
	switch {
	case c.writer.messageTimeout > 0:
		c.writer.deadline = now.Add(c.writer.messageTimeout)
	case c.writer.rate > 0:
		c.writer.deadline = now.Add(rateGrace)
	default:
		c.writer.deadline = time.Time{}
	}
	return &Writer{c: c}, nil
}

var (
	_ io.StringWriter = (*Writer)(nil)
	_ io.ReaderFrom   = (*Writer)(nil)
	_ io.WriterTo     = (*Reader)(nil)
)

// Writer streams one outbound message and coalesces small writes. It is valid
// only during its [Conn.Write] callback.
type Writer struct{ c *Conn }

// Final makes the next [Writer.Write] or [Writer.WriteString] finish the
// message. It is an optional optimization; [Conn.Write] otherwise finishes the
// message when the callback returns.
func (w *Writer) Final() {
	if m, err := w.message(); err == nil {
		m.final = true
	}
}

func (w *Writer) message() (*writer, error) {
	if w.c == nil {
		return nil, errWriterScope
	}
	m := &w.c.writer
	return m, m.err
}

// SetWriteDeadline sets the absolute deadline for the current message body. A
// zero value removes the deadline. It overrides [WriteOptions.MessageTimeout].
//
// Expiry is terminal. SetWriteDeadline conflicts with [WriteOptions.MinWriteRate].
func (w *Writer) SetWriteDeadline(t time.Time) error {
	c := w.c
	if c == nil {
		return errWriterScope
	}
	if c.writer.rate > 0 {
		return errSetWriteDeadlineRate
	}
	c.writer.deadline = t
	return nil
}

// Write adds p to the message. After [Writer.Final], it also finishes it.
func (w *Writer) Write(p []byte) (int, error) {
	m, err := w.message()
	if err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	buf := w.buffer()
	capRoom := cap(buf) - maxHeaderLen

	total := len(p)
	fill := min(capRoom-m.n, len(p))
	copy(buf[maxHeaderLen+m.n:maxHeaderLen+m.n+fill], p[:fill])
	m.n += fill
	p = p[fill:]

	if m.n < capRoom && !m.final {
		return total, nil
	}
	if m.final || len(p) >= capRoom {
		if err := w.flush(m.final, p); err != nil {
			return 0, err
		}
		return total, nil
	}
	if err := w.flush(false, nil); err != nil {
		return 0, err
	}
	copy(buf[maxHeaderLen:maxHeaderLen+len(p)], p)
	m.n = len(p)
	return total, nil
}

// WriteString is the [io.StringWriter] form of [Writer.Write].
func (w *Writer) WriteString(s string) (int, error) {
	m, err := w.message()
	if err != nil {
		return 0, err
	}
	if m.final {
		return w.Write([]byte(s))
	}
	buf := w.buffer()
	fill := min(cap(buf)-maxHeaderLen-m.n, len(s))
	copy(buf[maxHeaderLen+m.n:], s[:fill])
	m.n += fill
	n, err := w.Write([]byte(s[fill:]))
	return fill + n, err
}

func (w *Writer) buffer() []byte { return *w.c.writer.bufp }

// flush writes the staged payload and overflow as one frame.
func (w *Writer) flush(fin bool, overflow []byte) error {
	c := w.c
	m := &c.writer
	op := m.opcode
	if m.started {
		op = opContinuation
	}
	buf := (*m.bufp)[:maxHeaderLen+m.n]
	if m.rate > 0 {
		m.deadline = rateDeadline(m.deadline, m.rate, m.n+len(overflow), time.Now())
	}
	if err := c.writeFrame(op, fin, buf, overflow, m.deadline); err != nil {
		m.err = err
		return err
	}
	m.started = true
	m.n = 0
	if fin {
		m.err = errWriterScope
	}
	return nil
}

// ReadFrom copies r into the message without finishing it.
//
// A source error fails the message and causes [Conn.Write] to close gracefully.
func (w *Writer) ReadFrom(r io.Reader) (int64, error) {
	m, err := w.message()
	if err != nil {
		return 0, err
	}
	buf := w.buffer()
	capRoom := cap(buf) - maxHeaderLen
	var total int64
	for {
		rd, rerr := r.Read(buf[maxHeaderLen+m.n : cap(buf)])
		m.n += rd
		total += int64(rd)
		if m.n == capRoom {
			if err := w.flush(false, nil); err != nil {
				return total, err
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return total, nil
			}
			m.err = rerr
			return total, rerr
		}
	}
}

// close finishes the message and releases writer.mu.
func (w *Writer) close() error {
	c := w.c
	if c == nil {
		return nil
	}
	m := &c.writer
	defer func() {
		c.bufferPool.Put(m.bufp)
		m.bufp = nil
		w.c = nil
		<-c.writer.mu
	}()
	if m.err != nil {
		if m.err == errWriterScope {
			return nil
		}
		return m.err
	}
	return w.flush(true, nil)
}

// WriteMessage sends data as one message and one frame. ctx bounds only writer
// acquisition. Outbound text is not validated.
func (c *Conn) WriteMessage(ctx context.Context, mt MessageType, opts *WriteOptions, data []byte) error {
	return c.Write(ctx, mt, opts, func(w *Writer) error {
		w.Final()
		_, err := w.Write(data)
		return err
	})
}

// handleControlFrame consumes one validated control-frame header and payload.
func (c *Conn) handleControlFrame(h frameHeader) error {
	if h.length > maxControlPayload {
		return c.terminateRead(peerError(CloseProtocolError, "oversized control frame"))
	}
	if !h.fin {
		return c.terminateRead(peerError(CloseProtocolError, "fragmented control frame"))
	}
	payload := make([]byte, h.length)
	if _, err := readFull(c.br, payload); err != nil {
		return c.terminateRead(err)
	}
	if c.isServer && h.maskKey != ([4]byte{}) {
		maskCopy(payload, payload, h.maskKey, 0)
	}
	switch h.opcode {
	case opPing:
		// A missed bounded pong leaves the read usable.
		if err := c.writeControlFrame(opPong, payload, c.frame.controlDeadline()); err != nil && !errors.Is(err, ErrTimeout) {
			return err
		}
		if !c.reader.matchPong {
			c.monitor.noteControl()
		}
		return nil
	case opPong:
		if !c.reader.matchPong || bytes.Equal(payload, c.pingPayload[:]) {
			c.monitor.noteControl()
		}
		return nil
	case opClose:
		code, reason, err := parseClosePayload(payload)
		if err != nil {
			return c.terminateRead(err)
		}
		closeErr := &CloseError{Code: code, Text: reason}
		c.terminateRead(closeErr)
		// In manual mode the unsent response leaves closeSent clear, so the
		// application's remaining writes are not rejected with ErrCloseSent.
		if !c.manualCloseResponse {
			_ = c.writeControlFrame(opClose, payload, c.frame.controlDeadline())
		}
		return closeErr
	default:
		return c.terminateRead(peerError(CloseProtocolError, "unknown control opcode %d", h.opcode))
	}
}

// Shutdown sends a close frame and returns without waiting for the response.
// Continue reading, then call [Conn.Close]. A message the peer is still
// sending may be cut off; read any inbound message you need to completion
// first.
//
// An invalid code or invalid UTF-8 in reason returns [ErrInvalidArgument]. The
// reason is truncated to 123 bytes at a rune boundary.
//
// [DialOptions.ShutdownTimeout] or [UpgradeOptions.ShutdownTimeout] bounds both
// sending and waiting. Expiry tears down the connection with [ErrTimeout].
// Shutdown is safe for concurrent use and is a no-op when closing has begun.
func (c *Conn) Shutdown(code CloseCode, reason string) error {
	if !validWireCloseCode(code) {
		return errInvalidCloseCode
	}
	if !utf8.ValidString(reason) {
		return errInvalidCloseReason
	}
	if c.loadTerminalErr() != nil {
		return nil
	}
	// Sending and receiving share one shutdown budget.
	timeout := c.shutdownTimeout
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	err := c.writeControlFrame(opClose, buildClosePayload(code, reason), deadline)
	if errors.Is(err, ErrCloseSent) {
		return nil
	}
	if err != nil {
		if errors.Is(err, ErrTimeout) {
			c.terminateConn(errShutdownTimeout)
		}
		return err
	}
	// Recheck after Reset to cover a concurrent teardown: terminateConn's CAS
	// records the reason before its shutdownTimer.Stop, so a Stop racing this
	// Reset is covered in both orders — either it stops our arm, or the
	// recheck sees the reason and stops it.
	if timeout != 0 {
		if !deadline.After(time.Now()) {
			c.terminateConn(errShutdownTimeout)
			return nil
		}
		c.shutdownTimer.Reset(time.Until(deadline))
		if c.loadTerminalErr() != nil {
			c.shutdownTimer.Stop()
		}
	}
	return nil
}

// onShutdownTimer terminates an overdue close handshake. No stale guard: the
// timer is armed once and only ever fires to tear the connection down; a
// late fire reaches terminateConn as a no-op.
func (c *Conn) onShutdownTimer() {
	c.terminateConn(errShutdownTimeout)
}

// Close immediately closes the transport and unblocks active reads and writes.
// It is concurrent-safe and idempotent. Use [Conn.Shutdown] first for a graceful
// close.
func (c *Conn) Close() error {
	return c.terminateConn(ErrClosed)
}

// writeFrameLocked writes one frame. buf reserves maxHeaderLen bytes before its
// payload; overflow contains additional payload. frame.mu is held.
func (c *Conn) writeFrameLocked(op int, fin bool, buf, overflow []byte) error {
	// Coalesce the header with as much overflow as the buffer permits: the
	// rwc has no net.Buffers, so a lone header segment risks Nagle splitting
	// it from the body.
	dataLen := len(buf) - maxHeaderLen
	total := dataLen + len(overflow)
	k := min(cap(buf)-len(buf), len(overflow))
	dataEnd := maxHeaderLen + dataLen
	end := dataEnd + k

	var key [4]byte
	if !c.isServer {
		randFill(key[:])
	}

	var hdr [maxHeaderLen]byte
	hdrLen := buildHeader(hdr[:], c.isServer, op, fin, total, key)
	hdrStart := maxHeaderLen - hdrLen
	copy(buf[hdrStart:maxHeaderLen], hdr[:hdrLen])

	if c.isServer {
		copy(buf[dataEnd:end], overflow[:k])
		if _, err := c.rwc.Write(buf[hdrStart:end]); err != nil {
			return err
		}
		if rest := overflow[k:]; len(rest) > 0 {
			_, err := c.rwc.Write(rest)
			return err
		}
		return nil
	}

	// Stage overflow through buf so masking does not mutate the caller's slice.
	pos := maskCopy(buf[maxHeaderLen:dataEnd], buf[maxHeaderLen:dataEnd], key, 0)
	pos = maskCopy(buf[dataEnd:end], overflow[:k], key, pos)
	if _, err := c.rwc.Write(buf[hdrStart:end]); err != nil {
		return err
	}
	for rest := overflow[k:]; len(rest) > 0; {
		take := min(cap(buf), len(rest))
		pos = maskCopy(buf[:take], rest[:take], key, pos)
		if _, err := c.rwc.Write(buf[:take]); err != nil {
			return err
		}
		rest = rest[take:]
	}
	return nil
}
