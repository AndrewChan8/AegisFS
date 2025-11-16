"""
Client Library for AegisFS

This module provides the high-level client used to interact with the AegisFS
distributed file system. It hides all RPC details and exposes a simple API for:

    • Writing a file  (write_file)
    • Reading a file  (read_file)
    • Fetching metadata (get_meta)
    • Storing and reading blocks on a DataNode (store_block, read_block)

Architecture:
    - The Metadata Server (MDS) is the control plane.
      It stores crash-safe metadata using a write-ahead journal.

    - The DataNode is the data plane.
      It stores raw blocks on disk using atomic writes.

    - The Client coordinates both:
        write_file(path, text):
            1. Pick a new block_id
            2. Write bytes directly to the DataNode
            3. Commit file metadata to the MDS

This is the minimal Level-2 client required to demonstrate real DFS behavior.
"""

from __future__ import annotations

import socket
from typing import Any, Dict, List

from common.rpc import RpcConnection


class AegisClient:
    """
    High-level client for AegisFS.

    Parameters
    ----------
    mds_host : str
        Hostname/IP of the Metadata Server.
    mds_port : int
        Port of the Metadata Server.
    dn_host : str
        Hostname/IP of the DataNode.
    dn_port : int
        Port of the DataNode.

    Notes
    -----
    This client assumes a single MDS and a single DataNode
    and supports only single-block files. This is enough to
    demonstrate the AegisFS write and read pipeline.
    """

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
        """Send a JSON-RPC message to the MDS and return its response."""
        with socket.create_connection((self.mds_host, self.mds_port)) as s:
            conn = RpcConnection(s)
            conn.send(msg)
            return conn.recv()

    def _dn_rpc(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Send a JSON-RPC message to the DataNode and return its response."""
        with socket.create_connection((self.dn_host, self.dn_port)) as s:
            conn = RpcConnection(s)
            conn.send(msg)
            return conn.recv()

    # ------------------------------------------------------------
    # Metadata operations
    # ------------------------------------------------------------
    def get_meta(self, path: str) -> Dict[str, Any] | None:
        """Return the metadata dict for `path`, or None if not found."""
        resp = self._mds_rpc({"op": "get_meta", "args": {"path": path}})
        if not resp.get("ok", True):
            return None
        return resp.get("value")

    def put_meta(self, path: str, value: Dict[str, Any]) -> None:
        """Write metadata for `path` to the MDS."""
        resp = self._mds_rpc({"op": "put_meta", "args": {"path": path, "value": value}})
        if not resp.get("ok", False):
            raise RuntimeError(f"MDS put_meta failed: {resp}")

    # ------------------------------------------------------------
    # Block operations
    # ------------------------------------------------------------
    def store_block(self, block_id: str, data: bytes) -> None:
        """
        Write a block to the DataNode.

        Parameters
        ----------
        block_id : str
            The unique block identifier (e.g. 'b1234').
        data : bytes
            Raw file data to write.
        """
        resp = self._dn_rpc({
            "op": "store_block",
            "args": {"block_id": block_id, "data": data.decode("utf-8")},
        })
        if not resp.get("ok", False):
            raise RuntimeError(f"DataNode store_block failed: {resp}")

    def read_block(self, block_id: str) -> bytes | None:
        """
        Read a block from the DataNode.

        Returns
        -------
        bytes | None
            Raw data, or None if the block does not exist.
        """
        resp = self._dn_rpc({
            "op": "read_block",
            "args": {"block_id": block_id},
        })
        if not resp.get("ok", False):
            return None
        return resp["data"].encode("utf-8")

    # ------------------------------------------------------------
    # High-level file API
    # ------------------------------------------------------------
    def write_file(self, path: str, text: str) -> None:
        """
        Write `text` into `path` as a single block file.

        Steps:
            1. Generate a fresh block_id.
            2. Write raw bytes to the DataNode.
            3. Commit metadata back to the MDS.

        This matches the Level-2 write pipeline.
        """
        from uuid import uuid4
        block_id = f"b_{uuid4().hex[:8]}"
        payload = text.encode("utf-8")

        self.store_block(block_id, payload)

        meta = {"blocks": [block_id], "size": len(payload)}
        self.put_meta(path, meta)

    def read_file(self, path: str) -> str | None:
        """
        Read a file stored at `path`.

        Steps:
            1. Fetch metadata from the MDS.
            2. Read the first block from the DataNode.
            3. Decode bytes to text.

        Returns None if the file or block is missing.
        """
        meta = self.get_meta(path)
        if not meta:
            return None
        blocks: List[str] = meta.get("blocks", [])
        if not blocks:
            return ""
        block_id = blocks[0]
        data = self.read_block(block_id)
        if data is None:
            return None
        return data.decode("utf-8")
