"""
DataNode RPC server for AegisFS.

This module exposes the local block storage (DataNodeStorage) over a simple
TCP-based JSON RPC protocol. The server accepts one RPC request per connection,
executes the corresponding storage operation, and returns a JSON response.

Supported RPC ops:
  - ping
  - store_block(block_id, data_b64)
  - read_block(block_id)
  - delete_block(block_id)
"""

from __future__ import annotations

import base64
import socket
import threading
from pathlib import Path
from typing import Any, Dict

from common.config import load_level0_config
from common.rpc import RpcConnection
from datanode.storage import DataNodeStorage


def handle_request(store: DataNodeStorage, req: Dict[str, Any]) -> Dict[str, Any]:
    op = req.get("op")
    args = req.get("args", {})

    if op == "ping":
        return {"ok": True, "msg": "datanode_alive"}

    if op == "store_block":
        block_id = args["block_id"]
        data_b64 = args["data_b64"]
        data = base64.b64decode(data_b64.encode("ascii"))
        store.write_block(block_id, data)
        return {"ok": True}

    if op == "read_block":
        block_id = args["block_id"]
        data = store.read_block(block_id)
        if data is None:
            return {"ok": False, "error": "not_found"}
        data_b64 = base64.b64encode(data).decode("ascii")
        return {"ok": True, "data_b64": data_b64}

    if op == "delete_block":
        block_id = args["block_id"]
        store.delete_block(block_id)
        return {"ok": True}

    return {"ok": False, "error": f"unknown_op:{op}"}


def handle_client(conn_sock: socket.socket, store: DataNodeStorage) -> None:
    conn = RpcConnection(conn_sock)
    try:
        req = conn.recv()
        resp = handle_request(store, req)
        conn.send(resp)
    except EOFError:
        pass
    finally:
        conn.close()


def serve_datanode(host: str = "127.0.0.1", port: int = 9101) -> None:
    cfg = load_level0_config()
    store = DataNodeStorage(cfg.data_dir)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        print(f"[DataNode] Listening on {host}:{port}")
        while True:
            conn_sock, _ = s.accept()
            t = threading.Thread(
                target=handle_client,
                args=(conn_sock, store),
                daemon=True,
            )
            t.start()


if __name__ == "__main__":
    serve_datanode()
