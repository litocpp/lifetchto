#!/usr/bin/env python3
"""Independent TLS HTTP/2 peer. Requires test-only Python h2 and openssl."""
import argparse
import json
import os
import resource
from pathlib import Path
import socketserver
import ssl
import subprocess
import threading
import time

import h2.config
import h2.connection
import h2.events
import h2.settings
from hyperframe.frame import GoAwayFrame


class Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, context, bad_settings=False, stall_tls=False, stall_settings=False,
                 stream_window=None, mixed=False):
        self.context = context
        self.bad_settings = bad_settings
        self.stall_tls = stall_tls
        self.stall_settings = stall_settings
        self.stream_window = stream_window
        self.mixed = mixed
        self.events = []
        self.lock = threading.Lock()
        self.sequence = 0
        super().__init__(("127.0.0.1", 0), Handler)

    def event(self, **values):
        with self.lock:
            self.events.append(values)


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        with self.server.lock:
            self.server.sequence += 1
            identity = self.server.sequence
        try:
            if self.server.stall_tls:
                self.request.settimeout(4)
                while self.request.recv(65536):
                    pass
                return
            sock = self.server.context.wrap_socket(self.request, server_side=True)
            sock.settimeout(4)
            self.server.event(connection=identity, alpn=sock.selected_alpn_protocol())
            if sock.selected_alpn_protocol() != "h2":
                while True:
                    message = b""
                    while b"\r\n\r\n" not in message:
                        data = sock.recv(16384)
                        if not data:
                            sock.unwrap().close()
                            return
                        message += data
                    sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
                return
            if self.server.stall_settings:
                while sock.recv(65536):
                    pass
                return
            conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False, validate_inbound_headers=False))
            conn.initiate_connection()
            if self.server.stream_window is not None:
                conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE:
                                      self.server.stream_window})
            if self.server.mixed:
                conn.increment_flow_control_window(1024 * 1024 - 65535)
            initial = conn.data_to_send()
            if self.server.bad_settings:
                initial = bytes.fromhex("000006040000000000000200000001")
            sock.sendall(initial)
            if self.server.bad_settings:
                while sock.recv(65536):
                    pass
                sock.unwrap().close()
                return
            requests = {}
            pending = {}
            trailers = set()
            tunnels = set()
            tunnel_ended = set()
            tunnel_terminal = set()
            tunnel_started = set()
            split_reset = None
            window_target = None
            window_sent = 0
            window_credit = 0
            bulk_received = 0
            held_stream = None
            held_received = 0
            held_resets = 0
            goaway = False
            close_mode = "notify"
            while True:
                data = sock.recv(65536)
                if not data:
                    try:
                        sock.unwrap().close()
                    except (OSError, ssl.SSLError):
                        pass
                    return
                events = conn.receive_data(data)
                for event in events:
                    if isinstance(event, h2.events.RequestReceived):
                        fields = dict(event.headers)
                        if fields.get(b":method") == b"CONNECT":
                            sid = event.stream_id
                            authority = fields[b":authority"]
                            requests[sid] = ["CONNECT", bytearray(), fields]
                            self.server.event(connection=identity, stream=sid, target="CONNECT",
                                              authority=authority.decode())
                            if authority.startswith(b"stall"):
                                continue
                            if authority.startswith(b"deny"):
                                conn.send_headers(sid, [(b":status", b"407")], end_stream=True)
                            else:
                                tunnels.add(sid)
                                response_headers = [(b":status", b"200"),
                                    (b"x-connection", str(identity).encode()),
                                    (b"x-receive-window", str(conn.remote_settings.initial_window_size).encode()),
                                    (b"padding-type-reply", b"1")]
                                if self.server.mixed and authority.startswith(b"control-urgent"):
                                    response_headers.append((b"x-bulk-before-head", str(bulk_received).encode()))
                                    self.server.event(connection=identity, stream=sid,
                                                      bulk_before_head=bulk_received)
                                conn.send_headers(sid, response_headers)
                                if self.server.mixed and authority.startswith(b"hold-upload"):
                                    held_stream = sid
                                if authority.startswith(b"half"):
                                    conn.send_data(sid, b"early", end_stream=True)
                                elif authority.startswith(b"window"):
                                    window_target = sid
                                    pending[sid] = b"w" * (512 * 1024 + 32768)
                            continue
                        target = fields[b":path"].decode()
                        if target in ("/close-eof", "/close-stall"):
                            close_mode = target
                        requests[event.stream_id] = [target, bytearray(), fields]
                        self.server.event(connection=identity, stream=event.stream_id, target=target,
                                          headers=[(k.decode(), v.decode("latin1")) for k, v in event.headers])
                        if fields.get(b"expect") == b"100-continue":
                            conn.send_headers(event.stream_id, [(b":status", b"100")])
                        if target in ("/early", "/early-reset"):
                            conn.send_headers(event.stream_id, [(b":status", b"413"), (b"content-length", b"0")], end_stream=True)
                            if target == "/early-reset":
                                conn.reset_stream(event.stream_id, error_code=0)
                    elif isinstance(event, h2.events.DataReceived):
                        if event.stream_id in tunnels:
                            sid = event.stream_id
                            authority = requests[sid][2][b":authority"]
                            if self.server.mixed and authority.startswith(b"bulk"):
                                bulk_received += len(event.data)
                                conn.acknowledge_received_data(event.flow_controlled_length, sid)
                                continue
                            if self.server.mixed and authority.startswith(b"hold-upload"):
                                held_received += len(event.data)
                                if held_received > self.server.stream_window:
                                    raise AssertionError("upload exceeded the withheld stream credit")
                                continue
                            if authority.startswith(b"control"):
                                requests[sid][1].extend(event.data)
                                while b"\n" in requests[sid][1]:
                                    command, _, remaining = requests[sid][1].partition(b"\n")
                                    requests[sid][1] = bytearray(remaining)
                                    if command == b"reset-split":
                                        if split_reset is None:
                                            raise AssertionError("missing split reset target")
                                        conn.reset_stream(split_reset, error_code=0)
                                        tunnel_terminal.add(split_reset)
                                        self.server.event(connection=identity,
                                                          clean_reset=split_reset, batch=False)
                                    if command == b"window-status":
                                        if window_target is None:
                                            raise AssertionError("missing receive window target")
                                        response = (f"{window_sent}:"
                                                    f"{conn.local_flow_control_window(window_target)}:"
                                                    f"{window_credit}\n").encode()
                                    elif self.server.mixed and command == b"bulk-status":
                                        response = f"{bulk_received}\n".encode()
                                    elif self.server.mixed and command == b"mixed-status":
                                        response = (f"{window_sent}:{window_credit}:"
                                                    f"{held_received}:{held_resets}\n").encode()
                                    else:
                                        response = command + b"\n"
                                    pending[sid] = pending.get(sid, b"") + response
                                conn.acknowledge_received_data(event.flow_controlled_length, sid)
                                continue
                            if authority.startswith(b"clean-partial"):
                                requests[sid][1].extend(event.data)
                                if len(requests[sid][1]) == self.server.stream_window:
                                    conn.send_data(sid, b"done", end_stream=True)
                                    conn.reset_stream(sid, error_code=0)
                                    tunnel_terminal.add(sid)
                                    self.server.event(connection=identity, clean_reset=sid,
                                                      upload=len(requests[sid][1]), batch=True)
                                elif len(requests[sid][1]) > self.server.stream_window:
                                    raise AssertionError("upload exceeded withheld stream credit")
                                continue
                            if authority.startswith((b"clean-batch", b"clean-split",
                                                     b"reset-zero", b"end-reset-error")):
                                requests[sid][1].extend(event.data)
                                conn.acknowledge_received_data(event.flow_controlled_length, sid)
                                if sid in tunnel_started or len(requests[sid][1]) < 2:
                                    continue
                                tunnel_started.add(sid)
                                if authority.startswith(b"clean-batch"):
                                    conn.send_data(sid, b"first-")
                                    conn.send_data(sid, b"second")
                                    conn.send_data(sid, b"-last", end_stream=True)
                                    conn.reset_stream(sid, error_code=0)
                                    tunnel_terminal.add(sid)
                                    self.server.event(connection=identity, clean_reset=sid,
                                                      batch=True)
                                elif authority.startswith(b"clean-split"):
                                    conn.send_data(sid, b"split-body", end_stream=True)
                                    split_reset = sid
                                else:
                                    code = 0 if authority.startswith(b"reset-zero") else 7
                                    if code:
                                        conn.send_data(sid, b"", end_stream=True)
                                    conn.reset_stream(sid, error_code=code)
                                    tunnel_terminal.add(sid)
                                continue
                            if authority.startswith(b"reset-after"):
                                conn.acknowledge_received_data(event.flow_controlled_length, sid)
                                conn.reset_stream(sid, error_code=7)
                                continue
                            if not requests[sid][2][b":authority"].startswith(b"half"):
                                pending[sid] = pending.get(sid, b"") + event.data
                        else:
                            requests[event.stream_id][1].extend(event.data)
                        conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                    elif isinstance(event, h2.events.TrailersReceived):
                        self.server.event(connection=identity, upload_trailers=event.stream_id,
                                          fields=[(k.decode(), v.decode()) for k, v in event.headers])
                    elif isinstance(event, h2.events.StreamEnded):
                        sid = event.stream_id
                        if sid in tunnels:
                            tunnel_ended.add(sid)
                            if sid not in tunnel_terminal and not requests[sid][2][b":authority"].startswith(b"half"):
                                pending.setdefault(sid, b"")
                            continue
                        target, upload, fields = requests[sid]
                        self.server.event(connection=identity, upload=target, size=len(upload))
                        if target in ("/early", "/early-reset"):
                            continue
                        if target == "/reset":
                            conn.reset_stream(sid, error_code=7)
                            continue
                        if target == "/stall-head":
                            continue
                        body = bytes(upload) if target == "/echo" else b"x" * (512 * 1024) if target in ("/large", "/unread") else b"hello"
                        extra = [(b"x-connection", str(identity).encode()), (b"x-stream", str(sid).encode()),
                                 (b"x-dynamic", b"repeated dynamic huffman value"), (b"x-dup", b"a"), (b"x-dup", b"b")]
                        if target == "/informational":
                            conn.send_headers(sid, [(b":status", b"103"), (b"link", b"</a>; rel=preload")])
                        status = b"204" if target == "/204" else b"304" if target == "/304" else b"200"
                        if status in (b"204", b"304") or fields[b":method"] == b"HEAD":
                            conn.send_headers(sid, [(b":status", status)] + extra, end_stream=True)
                            continue
                        conn.send_headers(sid, [(b":status", status), (b"content-length", str(len(body)).encode())] + extra)
                        if target == "/truncated":
                            conn.send_data(sid, body[:3], end_stream=False)
                            sock.sendall(conn.data_to_send())
                            sock.close()
                            return
                        if target == "/slow":
                            continue
                        pending[sid] = body
                        if target == "/trailers":
                            trailers.add(sid)
                        if target == "/goaway":
                            goaway = sid
                    elif isinstance(event, h2.events.StreamReset):
                        self.server.event(connection=identity, reset=event.stream_id, code=int(event.error_code))
                        pending.pop(event.stream_id, None)
                        if event.stream_id == held_stream:
                            held_resets += 1
                    elif isinstance(event, h2.events.WindowUpdated):
                        self.server.event(connection=identity, window=event.stream_id, credit=event.delta)
                        if event.stream_id == window_target:
                            window_credit += event.delta
                    elif isinstance(event, h2.events.ConnectionTerminated):
                        self.server.event(connection=identity, goaway=int(event.error_code))
                        if close_mode == "/close-eof":
                            sock.close()
                            return
                        if close_mode == "/close-stall":
                            time.sleep(.5)
                            sock.close()
                            return
                        try:
                            sock.unwrap()
                        except (OSError, ssl.SSLError):
                            pass
                        return
                for sid in list(pending):
                    while sid in pending:
                        body = pending[sid]
                        count = min(len(body), conn.local_flow_control_window(sid), conn.max_outbound_frame_size)
                        if count == 0 and body:
                            break
                        last = count == len(body)
                        end = last and sid not in trailers and (sid not in tunnels or
                                                                sid in tunnel_ended or sid == window_target)
                        conn.send_data(sid, body[:count], end_stream=end)
                        if sid == window_target:
                            window_sent += count
                        if last:
                            pending.pop(sid)
                            if sid in trailers:
                                conn.send_headers(sid, [(b"x-end", b"yes")], end_stream=True)
                                trailers.remove(sid)
                        else:
                            pending[sid] = body[count:]
                wire = conn.data_to_send()
                if goaway:
                    frame = GoAwayFrame(0, last_stream_id=goaway, error_code=0)
                    wire = frame.serialize() + wire
                    goaway = False
                sock.sendall(wire)
        except (OSError, ssl.SSLError) as error:
            self.server.event(connection=identity, closed=type(error).__name__)
        except Exception as error:
            self.server.event(connection=identity, failure=repr(error))
            raise


def certificate(root):
    cert, key = root / "cert.pem", root / "key.pem"
    if not cert.exists():
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=localhost",
                        "-addext", "subjectAltName=DNS:localhost,DNS:origin.test,IP:127.0.0.1"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return cert, key


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--filter", default="H2Peer.*")
    args = parser.parse_args()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    cert, key = certificate(args.work_dir)
    peers = []
    for protocols, bad, stall_tls, stall_settings, window in (
            (["h2", "http/1.1"], False, False, False, None),
            (["http/1.1"], False, False, False, None), (["h2"], True, False, False, None),
            (["h2"], False, True, False, None), (["h2"], False, False, True, None),
            (["h2"], False, False, False, 1024)):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        context.set_alpn_protocols(protocols)
        peer = Peer(context, bad, stall_tls, stall_settings, window)
        peers.append(peer)
        threading.Thread(target=peer.serve_forever, daemon=True).start()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.set_alpn_protocols(["h2"])
    mixed_peer = Peer(context, stream_window=65536, mixed=True)
    peers.append(mixed_peer)
    threading.Thread(target=mixed_peer.serve_forever, daemon=True).start()
    env = dict(os.environ, LIFETCHTO_H2_PORT=str(peers[0].server_address[1]),
               LIFETCHTO_H1_PORT=str(peers[1].server_address[1]), LIFETCHTO_H2_BAD_PORT=str(peers[2].server_address[1]), LIFETCHTO_H2_CA=str(cert),
               LIFETCHTO_H2_TLS_STALL_PORT=str(peers[3].server_address[1]),
               LIFETCHTO_H2_SETTINGS_STALL_PORT=str(peers[4].server_address[1]))
    env["LIFETCHTO_H2_WINDOW_PORT"] = str(peers[5].server_address[1])
    env["LIFETCHTO_H2_MIXED_PORT"] = str(mixed_peer.server_address[1])
    try:
        result = subprocess.run([str(args.binary), "--gtest_filter=" + args.filter], env=env, timeout=90)
    finally:
        for peer in peers:
            peer.shutdown()
            peer.server_close()
        (args.work_dir / "events.json").write_text(json.dumps([p.events for p in peers], indent=2))
    print(json.dumps({"child_peak_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                      "requests": sum("target" in event for p in peers for event in p.events),
                      "mixed_connect_heads": [event for event in mixed_peer.events
                                              if "bulk_before_head" in event]}), flush=True)
    failures = [v for p in peers for v in p.events if "failure" in v]
    if failures:
        raise AssertionError(failures)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
