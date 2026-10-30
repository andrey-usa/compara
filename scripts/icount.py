#!/usr/bin/env python3
"""icount: run a command and print the user-space instructions it executed.

    icount.py [--json] [--counter auto|perf|cachegrind] -- COMMAND ARGS...

Time on a shared machine varies by two or three times between identical runs;
an instruction count of the same binary on the same input repeats to within a
few hundredths of a percent. That makes it a measure a pull request can be
held to.

Two counters:

  perf        the processor's own count, read through perf_event_open(2)
              (ctypes; no perf tool needed). Costs nothing, but needs a
              hardware counter the kernel lets this user open: many virtual
              machines have none.
  cachegrind  valgrind's count of the instructions it runs. Exact on any
              x86-64 or arm64 Linux, twenty to thirty times slower. It does
              not emulate AVX-512, so a program that picks its code at run
              time takes its AVX2 path.

`auto` (the default) takes perf when it opens, else cachegrind when valgrind
is installed. The two do not count alike, so compare counts from one counter
only. Prints `instructions: N (counter)`, or with --json
{"instructions": N, "counter": "..."}, and exits with the command's status;
exits 77 when neither counter is available.
"""
import ctypes
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile

PERF_EVENT_OPEN = {"x86_64": 298, "aarch64": 241}.get(os.uname().machine)


def open_perf():
    """A counter of user-space instructions, disabled until the next exec
    and inherited by children. Raises OSError when there is none."""
    if PERF_EVENT_OPEN is None:
        raise OSError(f"no perf_event_open number for {os.uname().machine}")
    libc = ctypes.CDLL(None, use_errno=True)
    attr = bytearray(128)
    # type PERF_TYPE_HARDWARE (0), size, config PERF_COUNT_HW_INSTRUCTIONS (1)
    struct.pack_into("IIQ", attr, 0, 0, len(attr), 1)
    # disabled (bit 0), inherit (1), exclude_kernel (5), exclude_hv (6),
    # enable_on_exec (12): only the command's own work is counted, from its
    # exec on, threads and children included.
    struct.pack_into("Q", attr, 40, (1 << 0) | (1 << 1) | (1 << 5) | (1 << 6) | (1 << 12))
    buf = ctypes.create_string_buffer(bytes(attr), len(attr))
    fd = libc.syscall(PERF_EVENT_OPEN, buf, 0, -1, -1, 0)
    if fd < 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))
    return fd


def perf_available():
    try:
        os.close(open_perf())
        return True
    except OSError:
        return False


def run_perf(cmd):
    fd = open_perf()
    try:
        # The counter starts at the child's exec (enable_on_exec); what the
        # parent and the child do before it is not counted. A child's count
        # is added to this one when it exits.
        status = subprocess.call(cmd)
        return status, struct.unpack("Q", os.read(fd, 8))[0]
    finally:
        os.close(fd)


def run_cachegrind(cmd):
    with tempfile.NamedTemporaryFile("r", suffix=".log") as log:
        status = subprocess.call(
            [
                "valgrind",
                "--tool=cachegrind",
                "--cache-sim=no",
                "--trace-children=yes",
                "--cachegrind-out-file=/dev/null",
                f"--log-file={log.name}",
                *cmd,
            ]
        )
        text = log.read()
    # One "I refs" line per process valgrind ran.
    counts = [int(n.replace(",", "")) for n in re.findall(r"I\s+refs:\s+([\d,]+)", text)]
    if not counts:
        sys.exit(f"icount: valgrind printed no count:\n{text[-2000:]}")
    return status, sum(counts)


def choose(counter):
    if counter == "auto":
        if perf_available():
            return "perf"
        return "cachegrind" if shutil.which("valgrind") else None
    if counter == "perf":
        return "perf" if perf_available() else None
    if counter == "cachegrind":
        return "cachegrind" if shutil.which("valgrind") else None
    sys.exit(f"icount: no counter called {counter!r} (auto, perf, cachegrind)")


def main(argv):
    if "--" not in argv or argv.index("--") == len(argv) - 1:
        print(__doc__, file=sys.stderr)
        return 2
    opts, cmd = argv[: argv.index("--")], argv[argv.index("--") + 1 :]
    as_json = "--json" in opts
    counter = "auto"
    if "--counter" in opts:
        counter = opts[opts.index("--counter") + 1]
    chosen = choose(counter)
    if chosen is None:
        print(f"icount: no instruction counter here ({counter})", file=sys.stderr)
        return 77
    status, n = (run_perf if chosen == "perf" else run_cachegrind)(cmd)
    if as_json:
        print(json.dumps({"instructions": n, "counter": chosen}))
    else:
        print(f"instructions: {n:,} ({chosen})")
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
