#!/usr/bin/env python3
"""Independent HTTP/1.1 and TLS peer for the public client test executable."""
import argparse
import json
import os
from pathlib import Path
import socket
import socketserver
import ssl
import subprocess
import threading
import time

class Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    def __init__(self, context=None):
        self.context = context
        self.events = []
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), Handler)
    def event(self, **value):
        with self.lock:
            self.events.append(value)
    def get_request(self):
        sock, address = super().get_request()
        self.event(connected=True, peer=list(address))
        if self.context:
            try:
                sock = self.context.wrap_socket(sock, server_side=True)
            except ssl.SSLError:
                sock.close()
                raise
        return sock, address

class UnixPeer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    event = Peer.event
    def __init__(self, path):
        self.events = []
        self.lock = threading.Lock()
        super().__init__(str(path), Handler)

class StallHandler(socketserver.BaseRequestHandler):
    def handle(self):
        time.sleep(1)

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(5)
        unclean = False
        stream = self.request.makefile("rb")
        try:
            while True:
                first = stream.readline(65537)
                if not first:
                    return
                method, target, version = first.decode("ascii").strip().split(" ")
                headers = []
                while True:
                    line = stream.readline(65537)
                    if line == b"\r\n":
                        break
                    if not line:
                        return
                    name, value = line.rstrip(b"\r\n").split(b":", 1)
                    headers.append((name.lower().decode("ascii"), value.strip().decode("latin1")))
                fields = dict(headers)
                self.server.event(method=method, target=target, version=version, headers=headers, connection=self.client_address[1] if isinstance(self.client_address, tuple) else str(self.client_address))
                if target in ("/early", "/expect-final"):
                    self.request.sendall(b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\n\r\n")
                    return
                if fields.get("expect") == "100-continue":
                    self.request.sendall(b"HTTP/1.1 100 Continue\r\n\r\n")
                body = bytearray()
                trailers = []
                if fields.get("transfer-encoding") == "chunked":
                    while True:
                        size = int(stream.readline().strip(), 16)
                        if not size:
                            while True:
                                line = stream.readline()
                                if line == b"\r\n":
                                    break
                                trailers.append(line.strip().decode("latin1"))
                            break
                        body.extend(stream.read(size))
                        assert stream.read(2) == b"\r\n"
                else:
                    body.extend(stream.read(int(fields.get("content-length", 0))))
                self.server.event(upload=target, size=len(body), trailers=trailers, body=list(body))
                if target == "/close-request":
                    return
                if target == "/invalid":
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx")
                    return
                if target == "/truncated":
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nab")
                    return
                if target == "/chunked":
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                    for part in (b"3\r\nabc", b"\r\n2\r\nde\r\n", b"0\r\nX-End: yes\r\n\r\n"):
                        self.request.sendall(part)
                        time.sleep(0.01)
                    continue
                if target == "/eof":
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\neof-body")
                    if isinstance(self.request, ssl.SSLSocket):
                        self.request.unwrap()
                    return
                if target == "/unclean":
                    unclean = True
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nunclean")
                    return
                if target == "/stall-head":
                    time.sleep(0.3)
                prefix = b""
                data = b"hello"
                extra = b"X-Dup: a\r\nX-Dup: b\r\nX-Bin: \xff\r\n"
                if target == "/echo" and "content-type" in fields:
                    extra += b"Content-Type: " + fields["content-type"].encode("latin1") + b"\r\n"
                status = b"200 OK"
                if target == "/informational":
                    prefix = b"HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\n"
                if target in ("/upload", "/echo"):
                    data = bytes(body)
                if target == "/policy-retry":
                    with self.server.lock:
                        count = sum(e.get("target") == target for e in self.server.events)
                    if count < 3:
                        status = b"503 Service Unavailable"
                        extra += b"Retry-After: 0\r\n"
                if target in ("/policy-307", "/policy-303"):
                    status = b"307 Temporary Redirect" if target.endswith("307") else b"303 See Other"
                    extra += b"Location: /policy-upload\r\n" if target.endswith("307") else b"Location: /policy-final\r\n"
                if target == "/policy-upload":
                    data = bytes(body)
                if target == "/policy-date":
                    if fields.get("if-modified-since") == "Wed, 21 Oct 2015 07:28:00 GMT":
                        status = b"304 Not Modified"
                        data = b""
                        extra += b"Cache-Control: max-age=60\r\n"
                    else:
                        extra += b"Cache-Control: max-age=0\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n"
                if target in ("/policy-vary", "/policy-oversize", "/policy-invalidate"):
                    extra += b"Cache-Control: max-age=60\r\n"
                if target == "/policy-vary":
                    extra += b"Vary: x-tone\r\n"
                if target == "/policy-oversize":
                    data = b"x" * 1024
                if target == "/large":
                    data = b"x" * (512 * 1024)
                if target == "/slow-body":
                    data = b"slow"
                if target in ("/empty", "/204", "/304") or method == "HEAD":
                    data = b""
                if target in ("/204", "/304"):
                    status = target[1:].encode() + b" Empty"
                if target in ("/redirect", "/loop", "/cross-origin"):
                    status = b"302 Found"
                    location = {"/redirect": b"/ok", "/loop": b"/loop", "/cross-origin": b"http://other.test/"}[target]
                    extra += b"Location: " + location + b"\r\n"
                    data = b""
                if target == "/cache":
                    extra += b"Cache-Control: max-age=60\r\nETag: \"v1\"\r\n"
                if target == "/revalidate":
                    if fields.get("if-none-match") == '\"v1\"':
                        status = b"304 Not Modified"
                        data = b""
                        extra += b"Cache-Control: max-age=60\r\n"
                    else:
                        extra += b"Cache-Control: max-age=0\r\nETag: \"v1\"\r\n"
                if target == "/nostore":
                    extra += b"Cache-Control: no-store, max-age=60\r\n"
                if target == "/vary":
                    data = fields.get("x-mode", "unset").encode()
                    extra += b"Cache-Control: max-age=60\r\nVary: X-Mode\r\n"
                length = b"" if target == "/204" else b"Content-Length: " + str(len(data)).encode() + b"\r\n"
                self.request.sendall(prefix + b"HTTP/1.1 " + status + b"\r\n" + length + extra + b"\r\n")
                if target == "/slow-body":
                    time.sleep(0.3)
                self.request.sendall(data)
                if target == "/idle-unclean":
                    unclean = True
                    return
                if target == "/idle-eof":
                    return
                if target == "/idle-extra":
                    time.sleep(0.03)
                    self.request.sendall(b"unsolicited")
                    return
        except (OSError, ValueError, AssertionError):
            return
        finally:
            stream.close()
            if isinstance(self.request, ssl.SSLSocket) and not unclean:
                try:
                    self.request.unwrap()
                except (OSError, ValueError):
                    pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--filter", default="Peer.*")
    parser.add_argument("--consumer", choices=("http", "tls", "ncrequest"))
    args = parser.parse_args()
    work = Path(args.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    key, cert = work / "key.pem", work / "cert.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key), "-out", str(cert), "-days", "1", "-subj", "/CN=origin.test", "-addext", "subjectAltName=DNS:origin.test,DNS:localhost,IP:127.0.0.1,IP:::1"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    plain = Peer()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.set_alpn_protocols(["http/1.1"])
    tls = Peer(context)
    context.sni_callback = lambda sock, name, ctx: tls.event(sni=name)
    (work / "http.sock").unlink(missing_ok=True)
    unix = UnixPeer(work / "http.sock")
    no_alpn_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    no_alpn_context.load_cert_chain(cert, key)
    no_alpn = Peer(no_alpn_context)
    expired_cert = work / "expired.pem"
    subprocess.run(["openssl", "x509", "-in", str(cert), "-signkey", str(key), "-not_before", "20200101000000Z", "-not_after", "20200102000000Z", "-out", str(expired_cert)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    expired_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    expired_context.load_cert_chain(expired_cert, key)
    expired = Peer(expired_context)
    stall = socketserver.ThreadingTCPServer(("127.0.0.1", 0), StallHandler)
    stall.daemon_threads = True
    servers = [plain, tls, unix, no_alpn, expired, stall]
    for server in servers:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    env = dict(os.environ, LIFETCHTO_PEER_PORT=str(plain.server_address[1]), LIFETCHTO_TLS_PORT=str(tls.server_address[1]), LIFETCHTO_CA=str(cert), LIFETCHTO_UNIX_PATH=str(work / "http.sock"), LIFETCHTO_NO_ALPN_PORT=str(no_alpn.server_address[1]), LIFETCHTO_EXPIRED_PORT=str(expired.server_address[1]), LIFETCHTO_STALL_PORT=str(stall.server_address[1]))
    try:
        argument = ("https://127.0.0.1:" + str(tls.server_address[1]) + "/ok") if args.consumer == "tls" else ("http://127.0.0.1:" + str(plain.server_address[1]) + "/ok") if args.consumer else "--gtest_filter=" + args.filter
        result = subprocess.run([str(Path(args.binary).resolve()), argument], env=env, timeout=60)
        events = {"http": plain.events, "tls": tls.events, "unix": unix.events, "no_alpn": no_alpn.events, "expired": expired.events}
        (work / "events.json").write_text(json.dumps(events, indent=2))
        if result.returncode:
            raise SystemExit(result.returncode)
        requests = [e for e in plain.events if "method" in e]
        assert all(e["version"] == "HTTP/1.1" and e["target"].startswith("/") for e in requests)
        assert all(len([h for h in e["headers"] if h[0] == "host"]) == 1 for e in requests)
        uploads = [e["size"] for e in plain.events if e.get("upload") == "/upload"]
        assert all(size in (6, 9) for size in uploads), uploads
        targets = [e["target"] for e in requests]
        if "/loop" in targets:
            assert targets.count("/loop") == 2
            assert targets.count("/cache") == 3
            assert targets.count("/revalidate") == 2
            assert targets.count("/vary") == 2
            assert targets.count("/nostore") == 2
            assert "/cache-miss" not in targets
        if "/policy-retry" in targets:
            assert targets.count("/policy-retry") == 3
            assert targets.count("/policy-307") == 1 and targets.count("/policy-upload") == 1
            replay = [e for e in requests if e["target"] in ("/policy-307", "/policy-upload")]
            for e in replay:
                assert e["method"] == "POST"
                assert [v for k, v in e["headers"] if k == "x-dup"] == ["a", "b"]
                assert dict(e["headers"])["x-bin"] == "ÿ"
                assert dict(e["headers"])["transfer-encoding"] == "chunked"
            assert [e["size"] for e in plain.events if e.get("upload") in ("/policy-307", "/policy-upload")] == [3, 3]
            assert all(e["trailers"] == ["X-End: yes"] for e in plain.events if e.get("upload") in ("/policy-307", "/policy-upload"))
            final = next(e for e in requests if e["target"] == "/policy-final")
            assert final["method"] == "GET"
        if "/policy-date" in targets:
            assert targets.count("/policy-date") == 2
            dates = [dict(e["headers"]) for e in requests if e["target"] == "/policy-date"]
            assert "if-modified-since" not in dates[0]
            assert dates[1]["if-modified-since"] == "Wed, 21 Oct 2015 07:28:00 GMT"
            assert targets.count("/policy-vary") == 2
            assert targets.count("/policy-oversize") == 2
            assert [e["method"] for e in requests if e["target"] == "/policy-invalidate"] == ["GET", "POST", "GET"]
        routed = [e["connection"] for e in requests if e["target"].startswith("/route-")]
        if routed:
            assert len(routed) == 3 and len(set(routed)) == 3, routed
        if unix.events:
            assert all(dict(e["headers"])["host"] == "unix.invalid" for e in unix.events if "headers" in e)
        if "/close-request" in targets:
            assert targets.count("/close-request") == 1
        for event in tls.events:
            if "headers" in event:
                host = dict(event["headers"])["host"]
                assert host in ("origin.test:" + str(tls.server_address[1]), "127.0.0.1:" + str(tls.server_address[1]), "[::1]:" + str(tls.server_address[1]))
        print(json.dumps({"requests": len(requests), "connections": sum("connected" in e for e in plain.events), "tls_sni": [e["sni"] for e in tls.events if "sni" in e]}))
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()

if __name__ == "__main__":
    main()
