#!/usr/bin/env python3
"""buildinfo: the rustc that built a binary, read from the binary itself.

    buildinfo.py BIN...

rustc writes `rustc version 1.100.0 (0ef1bc386 2026-11-09)` into the ELF
`.comment` section of what it links, stripped or not, so a Linux binary
names its compiler whatever machine it was built on. The perf gate and the
benchmarks print it beside their counts: a new compiler moves instruction
counts with no change to the code (rustc 1.100 against 1.97: +0.94% on the
turbo CSV pair), so a number is only comparable with one from the same
compiler. Mach-O and PE binaries carry no such record; for them this prints
`unknown` and the workflow that built them records `rustc -V` instead.
"""
import struct
import sys

# A section header table past this, or more sections than this, is not a
# binary rustc wrote: the reader gives up rather than walk it.
MAX_SECTIONS = 4096
MAX_COMMENT = 1 << 16


def rustc_of(path):
    """`rustc 1.100.0 (0ef1bc386 2026-11-09)` for an ELF binary rustc linked, else None."""
    try:
        with open(path, "rb") as f:
            ident = f.read(64)
            if len(ident) < 52 or ident[:4] != b"\x7fELF" or ident[4] not in (1, 2) or ident[5] not in (1, 2):
                return None
            wide, end = ident[4] == 2, "<" if ident[5] == 1 else ">"
            if wide:
                shoff, = struct.unpack_from(end + "Q", ident, 0x28)
                shentsize, shnum, shstrndx = struct.unpack_from(end + "HHH", ident, 0x3A)
            else:
                shoff, = struct.unpack_from(end + "I", ident, 0x20)
                shentsize, shnum, shstrndx = struct.unpack_from(end + "HHH", ident, 0x2E)
            if not 0 < shnum <= MAX_SECTIONS or shstrndx >= shnum or shentsize < (64 if wide else 40):
                return None
            f.seek(shoff)
            table = f.read(shentsize * shnum)
            if len(table) < shentsize * shnum:
                return None

            def section(i):  # (name offset, file offset, size)
                at = i * shentsize
                if wide:
                    name, = struct.unpack_from(end + "I", table, at)
                    offset, size = struct.unpack_from(end + "QQ", table, at + 0x18)
                else:
                    name, = struct.unpack_from(end + "I", table, at)
                    offset, size = struct.unpack_from(end + "II", table, at + 0x10)
                return name, offset, size

            _, names_at, names_size = section(shstrndx)
            f.seek(names_at)
            names = f.read(min(names_size, MAX_COMMENT))
            for i in range(shnum):
                name, offset, size = section(i)
                if names[name:name + 9] != b".comment\x00":
                    continue
                f.seek(offset)
                for entry in f.read(min(size, MAX_COMMENT)).split(b"\x00"):
                    if entry.startswith(b"rustc version "):
                        return "rustc " + entry[len(b"rustc version "):].decode("ascii", "replace")
    except (OSError, struct.error):
        return None
    return None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    for p in sys.argv[1:]:
        print(f"{p}: {rustc_of(p) or 'unknown'}")
