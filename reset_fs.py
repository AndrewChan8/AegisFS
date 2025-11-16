"""
Demo reset for AegisFS.

Uses the public client API so the MDS in-memory state and on-disk state stay consistent.
"""

from __future__ import annotations

from client.fs_client import AegisClient


def main() -> None:
    client = AegisClient()
    paths = sorted(client.list_paths())
    for p in paths:
        client.delete_file(p)
    print("AegisFS reset: all files and blocks deleted via MDS/DataNode.")


if __name__ == "__main__":
    main()
