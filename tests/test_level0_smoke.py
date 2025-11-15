from __future__ import annotations

import os
from pathlib import Path

from common.config import load_level0_config
from common.metadata_store import MetadataStore
from common.journal import Journal
from mds.state import MDSState


def run_level0_smoke() -> None:
    cfg = load_level0_config()

    # Start from a clean slate.
    if cfg.metadata_file.exists():
        cfg.metadata_file.unlink()
    if cfg.journal_file.exists():
        cfg.journal_file.unlink()

    # 1) MetadataStore basic persistence
    store = MetadataStore(cfg.metadata_file)
    store.load()
    assert store._meta == {}
    store.put("/foo.txt", {"blocks": [0], "size": 10})
    store.save()

    store2 = MetadataStore(cfg.metadata_file)
    store2.load()
    assert store2.get("/foo.txt") == {"blocks": [0], "size": 10}

    # 2) Journal + recovery semantics
    j = Journal(cfg.journal_file)

    # Committed tx for /a.txt
    tx1 = j.begin("create", path="/a.txt")
    j.apply(tx1, {
        "action": "put",
        "key": "/a.txt",
        "value": {"blocks": [1], "size": 111},
    })
    j.commit(tx1)

    # Uncommitted tx for /b.txt (should NOT appear after recovery)
    tx2 = j.begin("create", path="/b.txt")
    j.apply(tx2, {
        "action": "put",
        "key": "/b.txt",
        "value": {"blocks": [2], "size": 222},
    })
    # no commit

    # 3) Recover MDS state purely from journal
    state = MDSState.from_config(cfg)
    meta = state.store._meta

    assert "/a.txt" in meta, "Committed file missing after recovery"
    assert "/b.txt" not in meta, "Uncommitted file appeared after recovery"

    print("Level 0 smoke test PASSED.")
    print("Final metadata:", meta)


if __name__ == "__main__":
    run_level0_smoke()
