#!/usr/bin/env python3
"""A local stand-in for object storage, for the remote-input tests.

Serves the files under --root at /<bucket>/<key> with GET and HEAD and HTTP
ranges (`bytes=a-b`, `bytes=a-`, `bytes=-n`), and the headers object_store
reads an object's metadata from: Content-Length, Content-Range, ETag and
Last-Modified. That is enough to be an S3 endpoint for path-style requests
(AWS_ENDPOINT_URL, signatures not checked) and a plain HTTP server.

--no-ranges answers every GET with the whole object, as servers that ignore
the Range header do. --private refuses requests that carry neither an
Authorization header nor a query string, as a private bucket refuses an
unsigned GET. Every request is appended to --log as one JSON line: method,
path, range asked, bytes sent. A request's line is written after its answer,
so a client can have it all before the line is there: GET /__settled answers
once every request taken is logged (503 after 10 s), and is not logged.

--overwrite-after N serves every object, after N GETs, as a newer version:
other bytes (digits in its second half moved on by one; --grow adds a line
too) under another ETag, or none with --no-etag. A GET whose If-Match names
the older ETag is refused with 412, unless --ignore-if-match, as some servers
do not check it.

--fault MODE misbehaves on the ranged GETs picked by --fault-at K (counted
from 0 across the server) and --fault-every N (then every N-th; 0 for that
one only), at most --fault-max M of them (0: no limit): `silent` sends
nothing at all; `stall` the headers and then nothing; `drop` half the body,
then closes the connection; `reset` closes it before answering; `overlong`
sends 1,000 bytes past the range asked, its Content-Length saying so;
`dribble` sends the body 1 KB a second. Three modes hit every request for
the end of an object (`bytes=-n`) instead, whatever --fault-at says, and
leave ranges with a first byte alone: `suffix-501` answers it 501
Unsupported client range, as Fastly does (GitHub release assets);
`suffix-416` answers 416 with no Content-Range, as a server that takes no
such form may; `suffix-200` sends the whole object, as a server that ignores
a Range it does not parse must. `ranges-501` answers every GET with a Range
501; `wide` answers it 206 with a span of 64 MiB from byte 0, whatever was
asked, and sends zeros to fill it. With
--fault-puts the fault hits PUTs instead, counted the same way: `silent`
reads nothing and never answers; `stall` reads the body and never answers;
`dribble` reads it 1 KB at a time, --rate bytes a second (1024), then
answers; `drop` reads half of it and closes the connection; `reset` closes
it before reading.

--redirect answers a GET or HEAD of /go/<bucket>/<key> with a 302 to
/<bucket>/<key>?signed=K, K new each time, as GitHub sends a release asset
to a signed URL on its CDN; --expire-after N then refuses each such URL
after N GETs of it with 403, as a signed URL does once it expires.

Prints `LISTENING <url>` once it is ready.
"""
import argparse
import email.utils
import hashlib
import json
import os
import re
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--log")
    ap.add_argument("--no-ranges", action="store_true")
    ap.add_argument("--private", action="store_true")
    ap.add_argument("--flaky", type=int, default=0,
                    help="answer every N-th ranged GET after the first with a 503, once per range")
    ap.add_argument("--overwrite-after", type=int, default=0,
                    help="after N GETs, serve every object as a new version (a new ETag)")
    ap.add_argument("--ignore-if-match", action="store_true")
    ap.add_argument("--no-etag", action="store_true")
    ap.add_argument("--grow", action="store_true")
    ap.add_argument("--fault", choices=["silent", "stall", "drop", "reset", "overlong", "dribble",
                                        "suffix-501", "suffix-416", "suffix-200", "ranges-501", "wide"])
    ap.add_argument("--fault-at", type=int, default=0)
    ap.add_argument("--fault-every", type=int, default=0)
    ap.add_argument("--fault-max", type=int, default=0)
    ap.add_argument("--fault-puts", action="store_true", help="the fault hits PUTs, not ranged GETs")
    ap.add_argument("--rate", type=int, default=1024, help="a dribbled PUT's bytes a second")
    ap.add_argument("--redirect", action="store_true",
                    help="answer /go/<bucket>/<key> with a 302 to /<bucket>/<key>?signed=K, K new each time")
    ap.add_argument("--expire-after", type=int, default=0,
                    help="refuse each signed=K URL with 403 after N GETs of it")
    args = ap.parse_args()
    lock = threading.Lock()
    # Requests read and not yet logged: GET /__settled waits for none.
    settled = threading.Condition(lock)
    busy = [0]
    gets = [0]
    ranged = [0]
    puts = [0]
    faults = [0]
    signed = {}

    def picked(i):
        """Whether request i (counted under the lock) is hit."""
        hit = i == args.fault_at or (
            args.fault_every and i > args.fault_at and (i - args.fault_at) % args.fault_every == 0)
        if hit and args.fault_max:
            hit = faults[0] < args.fault_max
        faults[0] += hit
        return hit
    newer = {}

    def newer_version(full):
        """The bytes of the object's newer version (--overwrite-after)."""
        with lock:
            if full not in newer:
                with open(full, "rb") as f:
                    b = bytearray(f.read())
                for i in range(len(b) // 2, len(b), 7):
                    if 0x30 <= b[i] <= 0x38:
                        b[i] += 1
                if args.grow:
                    b += b"x" * 99 + b"\n"
                newer[full] = bytes(b)
            return newer[full]

    root_real = os.path.realpath(args.root)

    def inside(*parts):
        """A path under --root, or None for one that would leave it."""
        full = os.path.realpath(os.path.join(root_real, *parts))
        return full if full == root_real or full.startswith(root_real + os.sep) else None
    failed = set()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def parse_request(self):
            ok = super().parse_request()
            if ok and self.path != "/__settled":
                with settled:
                    busy[0] += 1
                self.counted = True
            return ok

        def handle_one_request(self):
            self.counted = False
            try:
                super().handle_one_request()
            finally:
                if self.counted:
                    with settled:
                        busy[0] -= 1
                        settled.notify_all()

        def record(self, asked, sent):
            if args.log:
                with lock, open(args.log, "a") as f:
                    f.write(json.dumps({"method": self.command, "path": self.path,
                                        "range": asked, "sent": sent}) + "\n")

        def target(self):
            parts = urlsplit(self.path)
            rel = unquote(parts.path).lstrip("/")
            full = os.path.realpath(os.path.join(args.root, rel))
            if not full.startswith(os.path.realpath(args.root) + os.sep) or not os.path.isfile(full):
                return None, parts
            return full, parts

        def refuse(self, code, reason=None):
            body = b"<Error><Code>Refused</Code></Error>"
            self.send_response(code, reason)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/xml")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
            self.record(self.headers.get("Range"), 0)

        def etag_of(self, full, version=1):
            st = os.stat(full)
            return '"' + hashlib.md5(f"{full}:{st.st_mtime_ns}:{st.st_size}:{version}".encode()).hexdigest() + '"'

        def list_bucket(self, bucket, query):
            """S3 ListObjectsV2, every key at once."""
            from urllib.parse import parse_qs
            from xml.sax.saxutils import escape
            q = {k: v[0] for k, v in parse_qs(query, keep_blank_values=True).items()}
            prefix = q.get("prefix", "")
            root = inside(bucket)
            if root is None:
                return self.refuse(400)
            keys = []
            for d, dirs, files in os.walk(root):
                dirs[:] = [x for x in dirs if x != ".uploads"]
                for f in files:
                    full = os.path.join(d, f)
                    key = os.path.relpath(full, root).replace(os.sep, "/")
                    if key.startswith(prefix):
                        keys.append((key, full))
            keys.sort()
            items = "".join(
                f"<Contents><Key>{escape(k)}</Key><LastModified>"
                f"{__import__('datetime').datetime.fromtimestamp(os.stat(full).st_mtime, __import__('datetime').timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                f"</LastModified><ETag>{escape(self.etag_of(full))}</ETag><Size>{os.path.getsize(full)}</Size>"
                f"<StorageClass>STANDARD</StorageClass></Contents>"
                for k, full in keys)
            xml = (f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>"
                   f"<Name>{escape(bucket)}</Name><Prefix>{escape(prefix)}</Prefix><KeyCount>{len(keys)}</KeyCount>"
                   f"<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{items}</ListBucketResult>").encode()
            self.reply(200, xml)
            self.record("LIST", len(xml))

        def serve(self, head):
            if args.redirect and self.path.startswith("/go/"):
                with lock:
                    k = len(signed) + 1
                    signed[str(k)] = 0
                self.send_response(302)
                self.send_header("Location", f"{self.path[3:]}?signed={k}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return self.record(self.headers.get("Range"), 0)
            k = re.search(r"signed=(\d+)", urlsplit(self.path).query)
            if args.expire_after and not head and k and k.group(1) in signed:
                with lock:
                    signed[k.group(1)] += 1
                    expired = signed[k.group(1)] > args.expire_after
                if expired:
                    return self.refuse(403)
            full, parts = self.target()
            if args.private and not self.headers.get("Authorization") and not parts.query:
                return self.refuse(403)
            bucket_only = unquote(parts.path).strip("/")
            if "list-type=2" in parts.query and "/" not in bucket_only:
                return self.list_bucket(bucket_only, parts.query)
            if full is None:
                return self.refuse(404)
            st = os.stat(full)
            asked = self.headers.get("Range")
            with lock:
                gets[0] += 1
                n = gets[0]
                fault = None
                if asked and not head and not args.fault_puts:
                    i = ranged[0]
                    ranged[0] += 1
                    fault = args.fault if picked(i) else None
            version = 2 if args.overwrite_after and n > args.overwrite_after else 1
            data = newer_version(full) if version == 2 else None
            size = len(data) if data is not None else st.st_size
            etag = None if args.no_etag else self.etag_of(full, version)
            if args.flaky and asked and n > 1 and n % args.flaky == 0:
                # Once per range: a retry of it gets through.
                with lock:
                    first = (full, asked) not in failed
                    failed.add((full, asked))
                if first:
                    return self.refuse(503)
            wanted = self.headers.get("If-Match")
            if wanted and wanted != etag and not args.ignore_if_match:
                return self.refuse(412)
            if fault == "reset":
                return self.close_now()
            if fault == "silent":
                time.sleep(3600)
                return self.close_now()
            suffix = bool(asked) and not head and asked.strip().startswith("bytes=-")
            if (suffix and args.fault == "suffix-501") or (asked and not head and args.fault == "ranges-501"):
                return self.refuse(501, "Unsupported client range")
            if suffix and args.fault == "suffix-416":
                return self.refuse(416)
            if asked and not head and args.fault == "wide":
                wide = 64 << 20
                self.send_response(206)
                self.send_header("Content-Range", f"bytes 0-{wide - 1}/{wide}")
                self.send_header("Content-Length", str(wide))
                self.end_headers()
                sent = 0
                try:
                    for _ in range(wide >> 16):
                        self.wfile.write(bytes(1 << 16))
                        sent += 1 << 16
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return self.record(asked, sent)
            start, end, partial = 0, size, False
            if asked and not args.no_ranges and not head and not (suffix and args.fault == "suffix-200"):
                m = re.fullmatch(r"bytes=(\d*)-(\d*)", asked.strip())
                if not m or (m.group(1) == "" and m.group(2) == ""):
                    return self.refuse(416)
                a, b = m.group(1), m.group(2)
                if a == "":
                    n = int(b)
                    start, end = max(0, size - n), size
                else:
                    start = int(a)
                    end = size if b == "" else min(size, int(b) + 1)
                if size == 0 or start >= size:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return self.record(asked, 0)
                partial = True
            if head:
                body = b""
            elif data is not None:
                body = data[start:end]
            else:
                with open(full, "rb") as f:
                    f.seek(start)
                    body = f.read(end - start)
            if fault == "overlong":
                body += b"9" * 1000
            self.send_response(206 if partial else 200)
            self.send_header("Content-Length", str(end - start if head else len(body)))
            if partial:
                self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
            self.send_header("Accept-Ranges", "none" if args.no_ranges else "bytes")
            if etag:
                self.send_header("ETag", etag)
            self.send_header("Last-Modified", email.utils.formatdate(st.st_mtime, usegmt=True))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            sent = 0
            if head:
                pass
            elif fault == "stall":
                self.wfile.flush()
                time.sleep(3600)
                return self.close_now()
            elif fault == "drop":
                self.wfile.write(body[: len(body) // 2])
                self.wfile.flush()
                return self.close_now()
            elif fault == "dribble":
                for i in range(0, len(body), 1024):
                    self.wfile.write(body[i:i + 1024])
                    self.wfile.flush()
                    sent += len(body[i:i + 1024])
                    time.sleep(1)
            else:
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    # The client stopped reading: logged all the same, with
                    # how much reached it unknown.
                    self.record(asked, None)
                    raise
                sent = len(body)
            self.record(asked, sent)

        def close_now(self):
            """The connection closed at once, with nothing more sent."""
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.close_connection = True

        def body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(n) if n else b""

        def reply(self, code, payload=b"", ctype="application/xml", etag=None):
            self.send_response(code)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Content-Type", ctype)
            if etag:
                self.send_header("ETag", etag)
            self.end_headers()
            if payload:
                self.wfile.write(payload)

        def do_PUT(self):
            parts = urlsplit(self.path)
            q = dict(p.split("=", 1) if "=" in p else (p, "") for p in parts.query.split("&") if p)
            fault = None
            if args.fault_puts:
                with lock:
                    i = puts[0]
                    puts[0] += 1
                    fault = args.fault if picked(i) else None
            n = int(self.headers.get("Content-Length") or 0)
            if fault == "reset":
                return self.close_now()
            if fault == "silent":
                time.sleep(3600)
                return self.close_now()
            if fault == "drop":
                self.rfile.read(n // 2)
                return self.close_now()
            if fault == "dribble":
                # Paced by the clock, not by a sleep a piece: a sleep that
                # runs long (macOS runners' timers, a loaded box) is made up
                # on the next piece, so the body is read at --rate overall.
                # A 31 ms sleep a KB had read at under 16 KB/s on macOS.
                data = b""
                start = time.monotonic()
                while len(data) < n:
                    piece = self.rfile.read(min(1024, n - len(data)))
                    if not piece:
                        break
                    data += piece
                    ahead = start + len(data) / args.rate - time.monotonic()
                    if ahead > 0:
                        time.sleep(ahead)
            else:
                data = self.body()
            if fault == "stall":
                time.sleep(3600)
                return self.close_now()
            if args.private and not self.headers.get("Authorization") and not parts.query:
                return self.refuse(403)
            rel = unquote(parts.path).lstrip("/")
            etag = '"' + hashlib.md5(data).hexdigest() + '"'
            if "uploadId" in q:
                d = inside(".uploads", q["uploadId"])
                if d is None:
                    return self.refuse(400)
                os.makedirs(d, exist_ok=True)
                with open(os.path.join(d, "%08d" % int(q["partNumber"])), "wb") as f:
                    f.write(data)
            else:
                full = inside(rel)
                if full is None:
                    return self.refuse(400)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "wb") as f:
                    f.write(data)
            self.record("PUT", len(data))
            self.reply(200, etag=etag)

        def do_POST(self):
            parts = urlsplit(self.path)
            q = dict(p.split("=", 1) if "=" in p else (p, "") for p in parts.query.split("&") if p)
            self.body()
            rel = unquote(parts.path).lstrip("/")
            bucket, _, key = rel.partition("/")
            if "uploads" in q:
                upload = hashlib.md5(f"{rel}:{os.getpid()}:{threading.get_ident()}:{len(os.listdir(args.root))}".encode()).hexdigest()
                xml = (f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult>"
                       f"<Bucket>{bucket}</Bucket><Key>{key}</Key><UploadId>{upload}</UploadId>"
                       f"</InitiateMultipartUploadResult>").encode()
                return self.reply(200, xml)
            if "uploadId" in q:
                d, full = inside(".uploads", q["uploadId"]), inside(rel)
                if d is None or full is None:
                    return self.refuse(400)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "wb") as out:
                    for name in sorted(os.listdir(d)):
                        with open(os.path.join(d, name), "rb") as f:
                            out.write(f.read())
                etag = '"' + hashlib.md5(open(full, "rb").read()).hexdigest() + '-1"'
                xml = (f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult>"
                       f"<Bucket>{bucket}</Bucket><Key>{key}</Key><ETag>{etag}</ETag>"
                       f"</CompleteMultipartUploadResult>").encode()
                return self.reply(200, xml)
            return self.refuse(400)

        def do_DELETE(self):
            self.reply(204)

        def do_GET(self):
            if self.path == "/__settled":
                with settled:
                    done = settled.wait_for(lambda: busy[0] == 0, timeout=10)
                self.send_response(200 if done else 503)
                self.send_header("Content-Length", "0")
                return self.end_headers()
            self.serve(head=False)

        def do_HEAD(self):
            self.serve(head=True)

    class Server(ThreadingHTTPServer):
        # A client asking again for every request a stall cut off connects
        # dozens of times at once; the default backlog of 5 dropped some,
        # and their retries came a second or more later.
        request_queue_size = 128

        def handle_error(self, request, client_address):
            # A client that stops reading part-way (it had what it needed)
            # is not an error worth a traceback.
            if not isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError)):
                super().handle_error(request, client_address)

    server = Server(("127.0.0.1", args.port), Handler)
    print(f"LISTENING http://127.0.0.1:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    sys.exit(main())
