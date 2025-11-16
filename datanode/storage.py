"""
DataNode local storage layer for AegisFS.

This module provides a minimal, crash-safe block store used by a single DataNode.
Each block is stored as a file on the local filesystem under the configured
data directory. The block store offers:

  - Deterministic mapping: <block_id> → <data_dir>/<block_id>.blk
  - Atomic writes using a temp-file + fsync + atomic replace pattern
  - Simple read and delete operations
  - No networking, no metadata logic, no client semantics

This layer is intentionally "dumb": it only deals with bytes on disk and is
invoked by the DataNode RPC server. Higher layers (MDS, clients) decide where
blocks should live; this module only ensures they are safely stored.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


class DataNodeStorage:
    """
    Local block storage for a single DataNode.

    Blocks are stored as files under data_dir:
        <data_dir>/<block_id>.blk
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def block_path(self, block_id: str) -> Path:
        return self.data_dir / f"{block_id}.blk"

    def write_block(self, block_id: str, data: bytes) -> None:
        path = self.block_path(block_id)
        tmp_path = path.with_suffix(".blk.tmp")

        with tmp_path.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        tmp_path.replace(path)

    def read_block(self, block_id: str) -> Optional[bytes]:
        path = self.block_path(block_id)
        if not path.exists():
            return None
        return path.read_bytes()

    def delete_block(self, block_id: str) -> None:
        path = self.block_path(block_id)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
