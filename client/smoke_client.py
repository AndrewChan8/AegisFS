"""
AegisFS Client Smoke Test
-------------------------

This script exercises the minimal Level-2 file pipeline:

    write_file(path, text):
        - writes bytes to the DataNode
        - updates metadata on the MDS

    read_file(path):
        - fetches metadata from the MDS
        - reads the block from the DataNode

This confirms that:
    • MDS RPC is functional
    • DataNode RPC is functional
    • The client correctly coordinates both

Run this after starting:
    $ python3 -m mds.server
    $ python3 -m datanode.server
"""

from __future__ import annotations

from client.fs_client import AegisClient


def main() -> None:
    print("=== AegisFS Client Smoke Test ===")
    c = AegisClient()

    path = "/notes"
    text = "hello from AegisClient"

    print(f"[Client] Writing {len(text)} bytes to {path!r}...")
    c.write_file(path, text)

    print(f"[Client] Reading back {path!r}...")
    data = c.read_file(path)

    print("[Client] Read result:", repr(data))


if __name__ == "__main__":
    main()
