from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class RpcConnection:
    """
    Simple JSON-over-TCP connection with newline-delimited messages.

    One send() = one JSON object = one line on the wire.
    recv() blocks until a full JSON line is available.
    """

    sock: socket.socket
    _buf: bytes = b""

    def send(self, msg: Dict[str, Any]) -> None:
        data = json.dumps(msg, separators=(",", ":")).encode("utf-8") + b"\n"
        self.sock.sendall(data)

    def recv(self) -> Dict[str, Any]:
        while b"\n" not in self._buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise EOFError("Connection closed while waiting for RPC message")
            self._buf += chunk

        line, self._buf = self._buf.split(b"\n", 1)
        if not line:
            return {}
        return json.loads(line.decode("utf-8"))

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
