#!/usr/bin/env python3
"""perf_gate: hold a change to the work a comparison takes, not to a timer.

    perf_gate.py --head BIN [--base BIN] --small A B --large A B
                 [--engines sortmerge,turbo] [--counter auto|perf|cachegrind]
                 [--max-ratio 5.5] [--max-growth 0.05] [--require]
                 -- COMPARE OPTIONS (keys, ignores)

For each engine it counts the instructions of `compara compare` (icount.py)
on two fixed pairs of files, the large one four times the rows of the small:

  scaling  large / small must stay under --max-ratio. Four times the rows is
           about 4.0-4.2 times the instructions for a linear or n log n
           engine, 16 for a quadratic one. This catches a cost that grows
           with the file whatever the machine.
  growth   with --base, the head's count at each size must stay within
           --max-growth (5%) of the base binary's on the same files, counted
           the same way. This catches a constant slowdown, such as an extra
           pass, which the ratio does not see.

Counts repeat to about 0.03% between runs (threads and the per-process hash
keys included), so both bounds hold on a busy shared runner. The hash keys
are fixed (COMPARA_HASH_SEED) all the same, and sortmerge's batch is made
small (COMPARA_SORTMERGE_BATCH_BYTES, 4 MiB) so that both sizes take its
spill and merge path: at its default batch the small file is sorted in
memory and the large one spilled, a step in cost per row that is not a
regression.

Prints a Markdown table (for $GITHUB_STEP_SUMMARY). Exits 1 on a bound
broken; with no counter on the machine prints SKIPPED and exits 0, or 1
with --require.
"""
import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FIXED = {"COMPARA_HASH_SEED": "1", "COMPARA_SORTMERGE_BATCH_BYTES": str(4 << 20)}


def count(counter, binary, files, args, env):
    cmd = [sys.executable, os.path.join(HERE, "icount.py"), "--json", "--counter", counter, "--"]
    cmd += [binary, "compare", *files, *args]
    p = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, **FIXED, **env})
    if p.returncode == 77:
        return None
    # compare exits 1 when the files differ, which these do.
    if p.returncode not in (0, 1):
        sys.exit(f"perf_gate: {' '.join(cmd)} exited {p.returncode}\n{p.stderr[-1000:]}")
    return json.loads(p.stdout.strip().splitlines()[-1])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--head", required=True, help="the compara binary under test")
    ap.add_argument("--base", help="the binary it is compared with")
    ap.add_argument("--small", nargs=2, required=True, metavar=("A", "B"))
    ap.add_argument("--large", nargs=2, required=True, metavar=("A", "B"))
    ap.add_argument("--engines", default="sortmerge,turbo")
    ap.add_argument("--counter", default="auto", choices=["auto", "perf", "cachegrind"])
    ap.add_argument("--max-ratio", type=float, default=5.5)
    ap.add_argument("--max-growth", type=float, default=0.05)
    ap.add_argument("--require", action="store_true", help="fail when there is no counter")
    ap.add_argument("--env", action="append", default=[], help="NAME=value for the head only")
    ap.add_argument("compare_args", nargs="*")
    a = ap.parse_args()
    head_env = dict(e.split("=", 1) for e in a.env)

    counter = a.counter
    rows, bad = [], []
    for eng in a.engines.split(","):
        extra = [*a.compare_args, "--engine", eng]
        small = count(counter, a.head, a.small, extra, head_env)
        if small is None:
            print(f"SKIPPED: no instruction counter on this machine ({counter})")
            return 1 if a.require else 0
        # The first count settles which counter `auto` means for the rest.
        counter = small["counter"]
        large = count(counter, a.head, a.large, extra, head_env)
        h = (small["instructions"], large["instructions"])
        ratio = h[1] / h[0]
        line = [eng, f"{h[0]:,}", f"{h[1]:,}", f"{ratio:.2f}x"]
        if ratio > a.max_ratio:
            bad.append(f"{eng}: four times the rows cost {ratio:.2f}x the instructions (bound {a.max_ratio}x)")
        if a.base:
            b = [count(counter, a.base, files, extra, {})["instructions"] for files in (a.small, a.large)]
            growth = [h[0] / b[0] - 1, h[1] / b[1] - 1]
            line += [f"{b[0]:,}", f"{b[1]:,}", f"{growth[0]:+.2%}", f"{growth[1]:+.2%}"]
            for size, g in zip(("small", "large"), growth):
                if g > a.max_growth:
                    bad.append(f"{eng}, {size}: {g:+.2%} instructions against the base (bound +{a.max_growth:.0%})")
        rows.append(line)

    cols = ["engine", "small", "large", "ratio"]
    if a.base:
        cols += ["base small", "base large", "change small", "change large"]
    print(f"Instructions of `compare` ({counter}):\n")
    print("| " + " | ".join(cols) + " |")
    print("|" + "---|" * len(cols))
    for r in rows:
        print("| " + " | ".join(r) + " |")
    print()
    for b in bad:
        print(f"FAIL: {b}")
    print("perf gate: " + ("FAILED" if bad else "ok"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
