from __future__ import annotations
"""
AegisFS CLI
-----------

Simple command-line interface on top of AegisClient.

Commands:
    aegisfs write <path> <text>
    aegisfs read <path>
    aegisfs stat <path>
"""

import argparse
from client.fs_client import AegisClient


def cmd_write(c: AegisClient, path: str, text: str) -> None:
    print(f"[Client] Writing {len(text)} bytes to {path!r}...")
    c.write_file(path, text)
    print("[Client] Write complete.")


def cmd_read(c: AegisClient, path: str) -> None:
    print(f"[Client] Reading {path!r}...")
    data = c.read_file(path)
    if data is None:
        print("[Client] ERROR: file not found")
    else:
        print(data)


def cmd_stat(c: AegisClient, path: str) -> None:
    print(f"[Client] Fetching metadata for {path!r}...")
    meta = c.get_meta(path)
    if not meta:
        print("[Client] ERROR: file not found")
        return
    print(f"[Client] Metadata: {meta}")

def cmd_ls(c: AegisClient) -> None:
    print("[Client] Listing all paths...")
    paths = c.list_paths()
    if not paths:
        print("(empty)")
        return
    for p in sorted(paths):
        print(p)


def cmd_rm(c: AegisClient, path: str) -> None:
    print(f"[Client] Deleting {path!r}...")
    c.delete_file(path)
    print("[Client] Delete complete.")


def main() -> None:
    parser = argparse.ArgumentParser(prog="aegisfs", description="AegisFS command-line client")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_write = sub.add_parser("write", help="write a file")
    p_write.add_argument("path")
    p_write.add_argument("text")

    p_read = sub.add_parser("read", help="read a file")
    p_read.add_argument("path")

    p_stat = sub.add_parser("stat", help="show file metadata")
    p_stat.add_argument("path")

    # NEW: ls
    p_ls = sub.add_parser("ls", help="list all paths")

    # NEW: rm
    p_rm = sub.add_parser("rm", help="delete a file")
    p_rm.add_argument("path")

    args = parser.parse_args()
    c = AegisClient()

    if args.cmd == "write":
        cmd_write(c, args.path, args.text)
    elif args.cmd == "read":
        cmd_read(c, args.path)
    elif args.cmd == "stat":
        cmd_stat(c, args.path)
    elif args.cmd == "ls":
        cmd_ls(c)
    elif args.cmd == "rm":
        cmd_rm(c, args.path)


if __name__ == "__main__":
    main()
