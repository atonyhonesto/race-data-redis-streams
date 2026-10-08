"""Use REDIS_URL if set; otherwise start a throwaway redis-server on a free port."""
from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import time

from .resp import Redis


@contextlib.contextmanager
def redis_url():
    if os.environ.get("REDIS_URL"):
        yield os.environ["REDIS_URL"]
        return
    exe = shutil.which("redis-server")
    if not exe:
        raise SystemExit("Set REDIS_URL, or install redis-server (or: docker run -p 6379:6379 redis:7)")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([exe, "--port", str(port), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"redis://127.0.0.1:{port}"
    try:
        for _ in range(50):
            try:
                Redis(url)("PING")
                break
            except OSError:
                time.sleep(0.1)
        yield url
    finally:
        proc.terminate()
        proc.wait()
