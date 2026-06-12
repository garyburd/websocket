# Command Example

This application bridges a subprocess to a browser over a websocket: each line
you type is written to the command's standard input, and the command's standard
output and standard error are sent back and displayed. It also demonstrates a
`MessageScanner`, a small range-over-func helper for reading messages in a loop,
built entirely on the package's exported API.

This README is a roadmap for running the example and finding your way around the
source; the explanations live in comments in the code.

> **Security:** the server runs a subprocess driven by network input. It is a
> demonstration only. Do not expose it on an untrusted network.

## Running the example

The example requires a working Go installation
([Getting Started](https://go.dev/doc/install)). Run it directly:

```
go run github.com/garyburd/websocket/examples/command@latest
```

or from a clone of the repository:

```
git clone https://github.com/garyburd/websocket
cd websocket
go run ./examples/command
```

Then open http://localhost:8080/ in a browser. With no command given the server
runs the calculator `bc -i -l`, so you can type an expression and see the
result:

```
2 + 2          =>  4
sqrt(2)        =>  1.41421356237309504880
(1 + 5) * 7    =>  42
```

An invalid expression such as `sqrt(-1)` or `2 +` makes `bc` print an error on
its standard error stream, which the browser shows in a different color; `bc`
keeps running, so you can carry on. (`bc` is part of POSIX and is installed by
default on macOS and most Linux distributions.)

Pass a different command after the flags to run it instead — for example `cat`
to echo each line back, or `tr a-z A-Z` to upper-case it:

```
go run ./examples/command cat
go run ./examples/command tr a-z A-Z
```

The **Close input** button signals end-of-input: the server closes the
command's stdin, the command sees EOF, and the connection stays open so any
final output still appears. A command that produces output only after consuming
all its input, such as `sort`, shows this off — type a few lines, then click
Close input to see the sorted result:

```
go run ./examples/command sort
```

Use the `-addr` flag, before the command, to listen on a different address.

### A note on output buffering

The command's stdout is a pipe, not a terminal. The C standard library
line-buffers stdout when it is a terminal but fully buffers it when it is a
pipe, so some programs hold their output until a block fills or the program
exits — in this bridge, that means results never reach the browser. `bc` and
`cat` flush each line, so they work as shown, but a program that buffers will
appear silent.

Some programs have their own option to disable buffering — for example
`python3 -u` (or the `PYTHONUNBUFFERED` environment variable) makes Python flush
stdout and stderr immediately. Prefer that when the command offers it:

```
go run ./examples/command python3 -u script.py
```

Otherwise, force line buffering by running the command under a utility that
provides one. `stdbuf -oL <command>` (GNU coreutils, Linux) sets line-buffered
stdout, and `unbuffer <command>` (from the `expect` package) runs it under a
pseudo-terminal, which has the same effect and works on macOS too. For example:

```
go run ./examples/command stdbuf -oL <command>
```

## Reading the code

The pieces, and where each is explained in detail:

- **[main.go](main.go)** — the server, running one subprocess per connection.
  Start at `serveWs`, the HTTP handler `main` registers: it upgrades the
  request, starts the command, launches the pumps, and — see its comments —
  drives the connection lifecycle and graceful shutdown. `inputPump` owns the
  single read loop, configures keepalive, and dispatches each inbound message;
  `streamPump` forwards a stdout or stderr stream back. The wire format is a
  one-byte tag at the front of every message in each direction, defined by the
  `tag*` constants.

- **[scanner.go](scanner.go)** — `MessageScanner`, the range-over-func helper
  `inputPump` reads with. Its doc comment covers the pattern (the terminal error
  is reported by `Err` after the loop, the `Reader` is valid only inside the
  loop body, and closing the connection is left to the caller) and why such a
  helper belongs in the example rather than the package.

- **[home.html](home.html)** — the browser frontend, embedded into the server
  with `go:embed`. It opens the websocket, colors output by its stream tag,
  sends tagged input lines, and drives the Close input button.
