"""Independent loopback HTTP CONNECT and SOCKS peers."""
import http.client
import select
import socket
import socketserver
import struct
import time
from urllib.parse import urlsplit


def exact(stream, count):
    output = bytearray()
    while len(output) < count:
        part = stream.recv(count - len(output))
        if not part:
            raise EOFError()
        output.extend(part)
    return bytes(output)


def cstring(stream):
    output = bytearray()
    while len(output) < 1024:
        value = exact(stream, 1)
        if value == b"\0":
            return output.decode("ascii")
        output.extend(value)
    raise ValueError("unbounded SOCKS field")


def upstream(server, host, port):
    if port not in server.allowed_ports:
        raise ValueError("destination is outside the peer")
    if host not in ("origin.test", "localhost", "127.0.0.1", "::1"):
        raise ValueError("unexpected proxy hostname")
    return socket.create_connection(("127.0.0.1", port), timeout=5)


def relay(left, right):
    reading = {left: right, right: left}
    while reading:
        buffered = [stream for stream in reading if hasattr(stream, "pending") and stream.pending()]
        ready = buffered or select.select(list(reading), [], [], 5)[0]
        if not ready:
            return
        for stream in ready:
            data = stream.recv(65536)
            if not data:
                destination = reading.pop(stream)
                destination.shutdown(socket.SHUT_WR)
            else:
                reading[stream].sendall(data)


class ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.server.event(proxy_opened=True)
        self.request.settimeout(5)
        connection = None
        try:
            while True:
                head = bytearray()
                while not head.endswith(b"\r\n\r\n"):
                    data = self.request.recv(1)
                    if not data:
                        return
                    head.extend(data)
                    if len(head) > 65536:
                        raise ValueError("oversized proxy request")
                lines = bytes(head).split(b"\r\n")
                method, target, version = lines[0].decode("ascii").split(" ")
                fields = [line.split(b":", 1) for line in lines[1:] if line]
                headers = [(name.lower().decode("ascii"), value.strip().decode("latin1")) for name, value in fields]
                self.server.event(proxy_method=method, proxy_target=target, headers=headers, version=version)
                if method == "CONNECT":
                    parsed = urlsplit("http://" + target)
                    if parsed.hostname == "deny.test":
                        self.request.sendall(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 6\r\n\r\ndenied")
                        return
                    if parsed.hostname == "stall.test":
                        time.sleep(0.3)
                        return
                    with upstream(self.server, parsed.hostname, parsed.port) as stream:
                        prefix = exact(stream, 5) if parsed.port == self.server.coalesce_port else b""
                        self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n" + prefix)
                        relay(self.request, stream)
                    return
                parsed = urlsplit(target)
                if parsed.scheme != "http" or not parsed.hostname:
                    raise ValueError("HTTP proxy requires absolute-form")
                if parsed.port not in self.server.allowed_ports:
                    raise ValueError("unexpected proxy port")
                if connection is None:
                    connection = http.client.HTTPConnection("127.0.0.1", parsed.port, timeout=5)
                connection.putrequest(method, parsed.path + ("?" + parsed.query if parsed.query else ""), skip_host=True, skip_accept_encoding=True)
                for name, value in fields:
                    if name.lower() not in (b"proxy-connection", b"proxy-authorization"):
                        connection.putheader(name.decode("ascii"), value.strip().decode("latin1"))
                count = int(dict(headers).get("content-length", "0"))
                if count > 65536 or "transfer-encoding" in dict(headers):
                    raise ValueError("unexpected proxy test upload")
                connection.endheaders(exact(self.request, count) if count else None)
                response = connection.getresponse()
                body = response.read(1024 * 1024 + 1)
                if len(body) > 1024 * 1024:
                    raise ValueError("unbounded proxy response")
                output = f"HTTP/1.1 {response.status} {response.reason}\r\n".encode("ascii")
                for name, value in response.getheaders():
                    if name.lower() not in ("connection", "content-length", "transfer-encoding"):
                        output += f"{name}: {value}\r\n".encode("latin1")
                if response.status != 204 and method != "HEAD":
                    output += f"Content-Length: {len(body)}\r\n".encode()
                self.request.sendall(output + b"\r\n" + body)
        except (OSError, EOFError):
            pass
        except Exception as error:
            self.server.event(proxy_error=str(error))
        finally:
            if connection:
                connection.close()
            self.server.event(proxy_closed=True)


class SocksHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.server.event(proxy_opened=True)
        self.request.settimeout(5)
        try:
            version = exact(self.request, 1)[0]
            if version == 5:
                methods = exact(self.request, exact(self.request, 1)[0])
                if 0 not in methods:
                    raise ValueError("SOCKS auth is outside this peer")
                self.request.sendall(b"\x05\0")
                ver, command, reserved, kind = exact(self.request, 4)
                if (ver, command, reserved) != (5, 1, 0):
                    raise ValueError("invalid SOCKS request")
                if kind == 1:
                    host = socket.inet_ntop(socket.AF_INET, exact(self.request, 4))
                elif kind == 3:
                    host = exact(self.request, exact(self.request, 1)[0]).decode("ascii")
                elif kind == 4:
                    host = socket.inet_ntop(socket.AF_INET6, exact(self.request, 16))
                else:
                    raise ValueError("invalid SOCKS address")
                port = struct.unpack("!H", exact(self.request, 2))[0]
                answer = b"\x05\0\0\x01\x7f\0\0\x01\0\0"
            elif version == 4:
                command, port, address = struct.unpack("!BH4s", exact(self.request, 7))
                if command != 1:
                    raise ValueError("invalid SOCKS4 command")
                cstring(self.request)
                kind = 3 if address[:3] == b"\0\0\0" else 1
                host = cstring(self.request) if kind == 3 else socket.inet_ntoa(address)
                answer = b"\0\x5a" + struct.pack("!H", port) + address
            else:
                raise ValueError("unknown SOCKS version")
            self.server.event(socks_version=version, socks_host=host, socks_port=port, address_kind=kind)
            with upstream(self.server, host, port) as stream:
                self.request.sendall(answer)
                relay(self.request, stream)
        except (OSError, EOFError):
            pass
        except Exception as error:
            self.server.event(proxy_error=str(error))
        finally:
            self.server.event(proxy_closed=True)
