"""
Client Library for AegisFS (binary-safe, multi-block)
"""

from __future__ import annotations

import base64
import socket
from typing import Any, Dict, List
from uuid import uuid4

from common.rpc import RpcConnection


class AegisClient:
    """
    High-level client for AegisFS.
    Supports multi-block text and binary files via base64-encoded blocks.
    """

    BLOCK_SIZE = 4096  # bytes per block

    def __init__(self, mds_host: str = "127.0.0.1", mds_port: int = 9000,
                 dn_host: str = "127.0.0.1", dn_port: int = 9101) -> None:
        self.mds_host = mds_host
        self.mds_port = mds_port
        self.dn_host = dn_host
        self.dn_port = dn_port

    # ------------------------------------------------------------
    # Low-level RPC helpers
    # ------------------------------------------------------------
    def _mds_rpc(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        with socket.create_connection((self.mds_host, self.mds_port)) as s:
            conn = RpcConnection(s)
            conn.send(msg)
            return conn.recv()

    def _dn_rpc(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        with socket.create_connection((self.dn_host, self.dn_port)) as s:
            conn = RpcConnection(s)
            conn.send(msg)
            return conn.recv()

    # ------------------------------------------------------------
    # Metadata operations
    # ------------------------------------------------------------
    def get_meta(self, path: str) -> Dict[str, Any] | None:
        resp = self._mds_rpc({"op": "get_meta", "args": {"path": path}})
        if not resp.get("ok", True):
            return None
        return resp.get("value")

    def put_meta(self, path: str, value: Dict[str, Any]) -> None:
        resp = self._mds_rpc({"op": "put_meta", "args": {"path": path, "value": value}})
        if not resp.get("ok", False):
            raise RuntimeError(f"MDS put_meta failed: {resp}")

    # ------------------------------------------------------------
    # Block operations (binary-safe via base64)
    # ------------------------------------------------------------
    def store_block(self, block_id: str, data: bytes) -> None:
        data_b64 = base64.b64encode(data).decode("ascii")
        resp = self._dn_rpc({
            "op": "store_block",
            "args": {"block_id": block_id, "data_b64": data_b64},
        })
        if not resp.get("ok", False):
            raise RuntimeError(f"DataNode store_block failed: {resp}")

    def read_block(self, block_id: str) -> bytes | None:
        resp = self._dn_rpc({
            "op": "read_block",
            "args": {"block_id": block_id},
        })
        if not resp.get("ok", False):
            return None
        data_b64 = resp["data_b64"]
        return base64.b64decode(data_b64.encode("ascii"))

    # ------------------------------------------------------------
    # High-level file API (bytes-first)
    # ------------------------------------------------------------
    def write_bytes(self, path: str, data: bytes,
                    mime: str | None = None,
                    filename: str | None = None) -> None:
        blocks: List[str] = []
        for offset in range(0, len(data), self.BLOCK_SIZE):
            chunk = data[offset: offset + self.BLOCK_SIZE]
            block_id = f"b_{uuid4().hex[:8]}"
            self.store_block(block_id, chunk)
            blocks.append(block_id)

        meta: Dict[str, Any] = {
            "blocks": blocks,
            "size": len(data),
        }
        if mime:
            meta["mime"] = mime
        if filename:
            meta["filename"] = filename

        self.put_meta(path, meta)

    def read_bytes(self, path: str) -> bytes | None:
        meta = self.get_meta(path)
        if not meta:
            return None
        blocks: List[str] = meta.get("blocks", [])
        if not blocks:
            return b""

        pieces: List[bytes] = []
        for block_id in blocks:
            chunk = self.read_block(block_id)
            if chunk is None:
                return None
            pieces.append(chunk)
        return b"".join(pieces)

    # ------------------------------------------------------------
    # Text convenience API on top of bytes
    # ------------------------------------------------------------
    def write_file(self, path: str, text: str) -> None:
        self.write_bytes(path, text.encode("utf-8"))

    def read_file(self, path: str) -> str | None:
        data = self.read_bytes(path)
        if data is None:
            return None
        return data.decode("utf-8", errors="replace")

    # ------------------------------------------------------------
    # Extra helpers for CLI
    # ------------------------------------------------------------
    def delete_block(self, block_id: str) -> None:
        resp = self._dn_rpc({"op": "delete_block", "args": {"block_id": block_id}})
        if not resp.get("ok", False):
            raise RuntimeError(f"DataNode delete_block failed: {resp}")

    def list_paths(self) -> list[str]:
        resp = self._mds_rpc({"op": "list_meta", "args": {}})
        if not resp.get("ok", False):
            return []
        return resp.get("paths", [])

    def delete_file(self, path: str) -> None:
        meta = self.get_meta(path)
        if not meta:
            return
        for b in meta.get("blocks", []):
            self.delete_block(b)
        resp = self._mds_rpc({"op": "delete_meta", "args": {"path": path}})
        if not resp.get("ok", False):
            raise RuntimeError(f"MDS delete_meta failed: {resp}")
