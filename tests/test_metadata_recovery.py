from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import tempfile
from pathlib import Path
from common.config import Level0Config
from mds.state import MDSState


def cfg(root: str) -> Level0Config:
    root = Path(root)
    return Level0Config(
        root_dir=root,
        metadata_file=root / "meta.json",
        journal_file=root / "journal.log",
        data_dir=root / "data",
        log_dir=root / "logs",
    )


def run():
    print("=== Metadata Recovery Test ===")
    with tempfile.TemporaryDirectory() as tmp:
        c = cfg(tmp)

        state = MDSState.from_config(c)
        state.put_metadata("/x", {"size": 5})
        state.put_metadata("/y", {"size": 10})

        # simulate crash: delete snapshot metadata file
        if c.metadata_file.exists():
            c.metadata_file.unlink()

        # rebuild from journal
        state2 = MDSState.from_config(c)
        meta = state2.store._meta

        assert "/x" in meta
        assert "/y" in meta

        print("PASS: Metadata rebuild from journal correct.")


if __name__ == "__main__":
    run()
