"""
Metadata Server TCP daemon (Level 1).

Starts a JSON-RPC TCP server that accepts client requests (put_meta/get_meta),
parses them via RpcConnection, and dispatches to MDSState. This exposes the
Level-0 journaled metadata engine over the network and forms the front-door
API for clients and, later, DataNode coordination.
"""

from __future__ import annotations

import socket
import threading
from typing import Any, Dict

from common.config import load_level0_config
from common.rpc import RpcConnection
from mds.state import MDSState


def handle_request(state: MDSState, req: Dict[str, Any]) -> Dict[str, Any]:
    op = req.get("op")
    args = req.get("args", {})

    if op == "ping":
        return {"ok": True, "msg": "mds_alive"}

    if op == "put_meta":
        path = args["path"]
        value = args["value"]
        state.put_metadata(path, value)
        return {"ok": True}

    if op == "get_meta":
        path = args["path"]
        value = state.store.get(path)
        return {"ok": True, "value": value}

    return {"ok": False, "error": f"unknown_op:{op}"}


def handle_client(conn_sock: socket.socket, state: MDSState) -> None:
    conn = RpcConnection(conn_sock)
    try:
        req = conn.recv()
        resp = handle_request(state, req)
        conn.send(resp)
    except EOFError:
        # client bailed, ignore
        pass
    finally:
        conn.close()


def serve_mds(host: str = "127.0.0.1", port: int = 9000) -> None:
    cfg = load_level0_config()
    state = MDSState.from_config(cfg)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        print(f"[MDS] Listening on {host}:{port}")
        while True:
            conn_sock, addr = s.accept()
            t = threading.Thread(
                target=handle_client,
                args=(conn_sock, state),
                daemon=True,
            )
            t.start()


if __name__ == "__main__":
    serve_mds()
