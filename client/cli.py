"""
AegisFS CLI
-----------

Polished, presentation-ready CLI for AegisFS.

Commands:
    write <path> <text>           - write inline text into AegisFS
    read <path>                  - read file contents from AegisFS
    stat <path>                  - pretty-print file metadata
    ls                           - list all files in AegisFS
    rm <path>                    - delete a file (and its blocks)
    put <local> <dfs_path>       - upload local UTF-8 text file into AegisFS
    get <dfs_path> <local>       - download AegisFS file to local UTF-8 text file
"""
from __future__ import annotations

import argparse
import sys
import re

from client.fs_client import AegisClient

# ─────────────────────────────────────────────────────────────
# Styling
# ─────────────────────────────────────────────────────────────

USE_COLOR = sys.stdout.isatty()

BOLD  = "\033[1m" if USE_COLOR else ""
DIM   = "\033[2m" if USE_COLOR else ""
GREEN = "\033[32m" if USE_COLOR else ""
RED   = "\033[31m" if USE_COLOR else ""
CYAN  = "\033[36m" if USE_COLOR else ""
RESET = "\033[0m" if USE_COLOR else ""


def banner(op: str, detail: str = "") -> None:
    title = f"AegisFS ▸ {op}"
    if detail:
        title += f" {detail}"
    width = max(len(title) + 4, 40)
    line = "─" * width
    if USE_COLOR:
        print(f"{CYAN}{line}{RESET}")
        print(f"{CYAN}│ {BOLD}{title}{RESET}{CYAN} │{RESET}")
        print(f"{CYAN}{line}{RESET}")
    else:
        print(line)
        print(f"| {title} |")
        print(line)


def info(msg: str) -> None:
    if USE_COLOR:
        print(f"{DIM}… {msg}{RESET}")
    else:
        print(f"... {msg}")


def ok(msg: str) -> None:
    if USE_COLOR:
        print(f"{GREEN}✔ {msg}{RESET}")
    else:
        print(f"[OK] {msg}")


def err(msg: str) -> None:
    if USE_COLOR:
        print(f"{RED}✖ {msg}{RESET}")
    else:
        print(f"[ERR] {msg}")


# ─────────────────────────────────────────────────────────────
# Helpers for fixed-width stat box
# ─────────────────────────────────────────────────────────────

ANSI_RE = re.compile(r"\x1b\[.*?m")


def visible_length(s: str) -> int:
    # Length without ANSI escape codes
    return len(ANSI_RE.sub("", s))


def pad_line(content: str, width: int) -> str:
    vis = visible_length(content)
    pad = max(0, width - vis)
    return content + (" " * pad)


# ─────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────

def cmd_write(c: AegisClient, path: str, text: str) -> None:
    banner("write", path)
    info(f"{len(text)} bytes")
    c.write_file(path, text)
    ok("write complete")


def cmd_read(c: AegisClient, path: str) -> None:
    banner("read", path)
    data = c.read_file(path)
    if data is None:
        err(f"file not found: {path}")
    else:
        print(data)


def cmd_stat(c: AegisClient, path: str) -> None:
    banner("stat", path)
    meta = c.get_meta(path)
    if not meta:
        err(f"file not found: {path}")
        return

    size = meta.get("size", 0)
    blocks = meta.get("blocks", []) or []

    BOX_WIDTH = 60  # total width including borders

    def inside(line: str) -> str:
        inner_width = BOX_WIDTH - 2  # between the two border chars
        # we print "│ " + content + "│" -> content width = inner_width - 1
        padded = pad_line(line, inner_width - 1)
        return "│ " + padded + "│"

    print("┌" + "─" * (BOX_WIDTH - 2) + "┘".replace("┘", "┐"))
    print(inside("File Metadata"))
    print("│" + "─" * (BOX_WIDTH - 2) + "│")

    print(inside(f"Path   : {path}"))
    print(inside(f"Size   : {size}"))
    print(inside(f"Blocks : {len(blocks)}"))

    print(inside("Block IDs:"))
    for b in blocks:
        print(inside(f"  - {b}"))

    print("└" + "─" * (BOX_WIDTH - 2) + "┘")


def cmd_ls(c: AegisClient) -> None:
    banner("ls")
    paths = sorted(c.list_paths())
    if not paths:
        if USE_COLOR:
            print(DIM + "(empty filesystem)" + RESET)
        else:
            print("(empty filesystem)")
        return

    header = f"{'PATH':<24} {'SIZE':>8} {'BLOCKS':>8}"
    sep = "-" * len(header)
    if USE_COLOR:
        print(BOLD + header + RESET)
    else:
        print(header)
    print(sep)

    for p in paths:
        meta = c.get_meta(p) or {}
        size = meta.get("size", 0)
        blocks = len(meta.get("blocks", []) or [])
        print(f"{p:<24} {size:>8} {blocks:>8}")


def cmd_rm(c: AegisClient, path: str) -> None:
    banner("rm", path)
    meta = c.get_meta(path)
    if not meta:
        err(f"file not found: {path}")
        return
    c.delete_file(path)
    ok("delete complete")


def cmd_put(c: AegisClient, local_path: str, dfs_path: str) -> None:
    """Upload a local UTF-8 text file into AegisFS."""
    banner("put", f"{local_path} → {dfs_path}")
    try:
        with open(local_path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        err(f"could not read local file: {e}")
        return

    info(f"uploading {len(text)} bytes")
    c.write_file(dfs_path, text)
    ok("upload complete")


def cmd_get(c: AegisClient, dfs_path: str, local_path: str) -> None:
    """Download a file from AegisFS to a local UTF-8 text file."""
    banner("get", f"{dfs_path} → {local_path}")
    data = c.read_file(dfs_path)
    if data is None:
        err(f"file not found: {dfs_path}")
        return

    try:
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(data)
    except OSError as e:
        err(f"could not write local file: {e}")
        return

    ok("download complete")


# ─────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(prog="aegisfs", description="AegisFS command-line client")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_write = sub.add_parser("write", help="write a file (inline text)")
    p_write.add_argument("path")
    p_write.add_argument("text")

    p_read = sub.add_parser("read", help="read a file")
    p_read.add_argument("path")

    p_stat = sub.add_parser("stat", help="show file metadata")
    p_stat.add_argument("path")

    sub.add_parser("ls", help="list all paths")

    p_rm = sub.add_parser("rm", help="delete a file")
    p_rm.add_argument("path")

    p_put = sub.add_parser("put", help="upload local UTF-8 text file into AegisFS")
    p_put.add_argument("local_path")
    p_put.add_argument("dfs_path")

    p_get = sub.add_parser("get", help="download AegisFS file to local UTF-8 text file")
    p_get.add_argument("dfs_path")
    p_get.add_argument("local_path")

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
    elif args.cmd == "put":
        cmd_put(c, args.local_path, args.dfs_path)
    elif args.cmd == "get":
        cmd_get(c, args.dfs_path, args.local_path)


if __name__ == "__main__":
    main()
