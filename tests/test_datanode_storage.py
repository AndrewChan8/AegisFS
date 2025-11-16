from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datanode.storage import DataNodeStorage
import tempfile
from pathlib import Path


def run():
    print("=== DataNode Storage Test ===")
    with tempfile.TemporaryDirectory() as tmp:
        store = DataNodeStorage(Path(tmp))

        store.write_block("b1", b"hello")
        assert store.read_block("b1") == b"hello"

        store.write_block("b1", b"world")
        assert store.read_block("b1") == b"world"

        store.delete_block("b1")
        assert store.read_block("b1") is None

        print("PASS: DataNode storage correct.")


if __name__ == "__main__":
    run()
