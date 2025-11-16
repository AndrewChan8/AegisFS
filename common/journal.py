"""
Write-ahead logging for the Metadata Server.

Implements an append-only JSONL journal with BEGIN / APPLY / COMMIT / ABORT
records. The journal is the durable source of truth for metadata mutations.
Recovery scans the log and replays only committed transactions to rebuild
the MetadataStore after a crash.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Any, Iterator


class JournalOp(str, Enum):
    BEGIN = "BEGIN"
    APPLY = "APPLY"
    COMMIT = "COMMIT"
    ABORT = "ABORT"


@dataclass
class JournalRecord:
    txid: int
    op: JournalOp
    data: Dict[str, Any]


class Journal:
    """
    Append-only JSONL journal.
    Provides BEGIN/APPLY/COMMIT/ABORT records.
    Replay is implemented later.
    """

    def __init__(self, path: Path):
        self.path = path
        self._next_txid = 1
        self._init_txid_from_disk()

    def _init_txid_from_disk(self) -> None:
        # Find highest txid so we continue numbering safely.
        if not self.path.exists():
            return
        max_txid = 0
        for rec in self.iter_records():
            max_txid = max(max_txid, rec.txid)
        self._next_txid = max_txid + 1

    def new_txid(self) -> int:
        txid = self._next_txid
        self._next_txid += 1
        return txid

    def append(self, rec: JournalRecord) -> None:
        line = json.dumps(
            {"txid": rec.txid, "op": rec.op.value, "data": rec.data}
        )
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def iter_records(self) -> Iterator[JournalRecord]:
        if not self.path.exists():
            return iter(())
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                if not (line := line.strip()):
                    continue
                raw = json.loads(line)
                yield JournalRecord(
                    txid=raw["txid"],
                    op=JournalOp(raw["op"]),
                    data=raw.get("data", {}),
                )

    def begin(self, op: str, **extra: Any) -> int:
        """
        Start a new transaction for a high-level metadata op.
        Returns the txid.
        """
        txid = self.new_txid()
        rec = JournalRecord(
            txid=txid,
            op=JournalOp.BEGIN,
            data={"op": op, **extra},
        )
        self.append(rec)
        return txid

    def apply(self, txid: int, data: Dict[str, Any]) -> None:
        """
        Log a state change associated with an existing transaction.
        Does not touch metadata itself; caller is responsible for applying it.
        Shows what will be mutated
        """
        rec = JournalRecord(txid=txid, op=JournalOp.APPLY, data=data)
        self.append(rec)

    def commit(self, txid: int) -> None:
        """
        Mark a transaction as committed (durable).
        Concrete proof that a transaction was fully mutated
        """
        rec = JournalRecord(txid=txid, op=JournalOp.COMMIT, data={})
        self.append(rec)

    def abort(self, txid: int) -> None:
        """Explicitly abort a transaction."""
        rec = JournalRecord(txid=txid, op=JournalOp.ABORT, data={})
        self.append(rec)
