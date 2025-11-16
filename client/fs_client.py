"""
Client Library for AegisFS — Multi-Block Version
"""

from __future__ import annotations

import socket
from typing import Any, Dict, List
from uuid import uuid4

from common.rpc import RpcConnection


class AegisClient:
    """
    High-level client for AegisFS.
    Now supports multi-block files using BLOCK_SIZE chunking.
    """

    BLOCK_SIZE = 4096  # bytes per block

    def __init__(self, mds_host="127.0.0.1", mds_port=9000,
                 dn_host="127.0.0.1", dn_port=9101) -> None:
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
    # Block operations (now binary-safe)
    # ------------------------------------------------------------
    def store_block(self, block_id: str, data: bytes) -> None:
        resp = self._dn_rpc({
            "op": "store_block",
            "args": {
                "block_id": block_id,
                "data": data.decode("latin1"),   # raw-byte safe
            },
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
        return resp["data"].encode("latin1")  # rehydrate raw bytes

    # ------------------------------------------------------------
    # Multi-block file API
    # ------------------------------------------------------------
    def write_file(self, path: str, text: str) -> None:
        """
        Write text to a file using chunked blocks.
        """

        payload = text.encode("utf-8")
        blocks: List[str] = []

        for offset in range(0, len(payload), self.BLOCK_SIZE):
            chunk = payload[offset: offset + self.BLOCK_SIZE]
            block_id = f"b_{uuid4().hex[:8]}"
            self.store_block(block_id, chunk)
            blocks.append(block_id)

        meta = {
            "blocks": blocks,
            "size": len(payload),
        }
        self.put_meta(path, meta)

    def read_file(self, path: str) -> str | None:
        """
        Reassemble a chunked multi-block file and return UTF-8 text.
        """
        meta = self.get_meta(path)
        if not meta:
            return None

        blocks: List[str] = meta.get("blocks", [])
        if not blocks:
            return ""

        pieces: List[bytes] = []
        for b in blocks:
            raw = self.read_block(b)
            if raw is None:
                return None
            pieces.append(raw)

        return b"".join(pieces).decode("utf-8")

    # ------------------------------------------------------------
    # Helper methods for CLI
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
