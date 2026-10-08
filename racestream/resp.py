"""A minimal Redis client: the RESP2 protocol in about 50 lines.

Every command is an array of bulk strings; every reply starts with a type byte:
  +simple string   -error   :integer   $bulk string   *array
redis-py does the same thing with connection pooling, retries and many conveniences.
"""
from __future__ import annotations

import socket
from urllib.parse import urlparse


class RedisError(Exception):
    pass


class Redis:
    def __init__(self, url: str = "redis://127.0.0.1:6379"):
        u = urlparse(url)
        self.sock = socket.create_connection((u.hostname or "127.0.0.1", u.port or 6379), timeout=10)
        self.f = self.sock.makefile("rb")

    def __call__(self, *args):
        """r("XADD", "race:timing", "*", "car", "24") -> parsed reply."""
        parts = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            parts.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(parts))
        return self._read()

    def _read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("connection closed")
        kind, rest = line[:1], line[1:-2]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RedisError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n == -1:
                return None
            data = self.f.read(n + 2)[:-2]
            return data.decode()
        if kind == b"*":
            n = int(rest)
            return None if n == -1 else [self._read() for _ in range(n)]
        raise RedisError(f"unexpected reply {line!r}")

    def close(self):
        self.f.close()
        self.sock.close()
