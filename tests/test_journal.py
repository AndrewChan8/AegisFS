from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.journal import Journal, JournalOp
from pathlib import Path
import tempfile


def run():
    print("=== Journal Test ===")
    with tempfile.TemporaryDirectory() as tmp:
        jpath = Path(tmp) / "journal.log"
        j = Journal(jpath)

        tx = j.begin("put", path="/a")
        j.apply(tx, {"action": "put", "key": "/a", "value": {"x": 1}})
        j.commit(tx)

        recs = list(j.iter_records())
        assert len(recs) == 3
        assert recs[0].op == JournalOp.BEGIN
        assert recs[1].op == JournalOp.APPLY
        assert recs[2].op == JournalOp.COMMIT

        print("PASS: Journal append + replay is correct.")


if __name__ == "__main__":
    run()
