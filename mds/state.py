from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Set
from collections import defaultdict

from common.config import Level0Config
from common.metadata_store import MetadataStore
from common.journal import Journal, JournalOp


@dataclass
class MDSState:
    """
    Level 0 MDS state: metadata + journal + recovery.

    On startup we ignore any existing metadata file and rebuild it
    purely from committed journal transactions.
    """

    cfg: Level0Config
    store: MetadataStore
    journal: Journal

    @classmethod
    def from_config(cls, cfg: Level0Config) -> "MDSState":
        journal = Journal(cfg.journal_file)
        store = MetadataStore(cfg.metadata_file)
        state = cls(cfg=cfg, store=store, journal=journal)
        state.recover_from_journal()
        return state

    def recover_from_journal(self) -> None:
        """
        Rebuild metadata from committed APPLY records. Only rebuilds from COMMIT and disregards else.

        APPLY records must have:
          - action: "put" or "delete"
          - key: path string
          - value: JSON-serializable dict (for put only)
        """
        tx_applies = defaultdict(list)   # txid -> list[dict]
        committed: Set[int] = set()
        aborted: Set[int] = set()

        for rec in self.journal.iter_records():
            if rec.op is JournalOp.APPLY:
                tx_applies[rec.txid].append(rec.data)
            elif rec.op is JournalOp.COMMIT:
                committed.add(rec.txid)
            elif rec.op is JournalOp.ABORT:
                aborted.add(rec.txid)

        # Start from a clean in-memory metadata state.
        self.store._meta.clear()

        # Apply only committed, non-aborted transactions in txid order.
        for txid in sorted(committed - aborted):
            for act in tx_applies.get(txid, []):
                action = act.get("action")
                key = act.get("key")
                if action == "put":
                    self.store.put(key, act["value"])
                elif action == "delete":
                    self.store.delete(key)

        # Persist rebuilt state to disk.
        self.store.save()

    # ---------- Public Level 0 operations ----------

    def put_metadata(self, path: str, value: dict) -> None:
        """
        Create or update metadata for a path with journaling.
        """
        txid = self.journal.begin("put", path=path)
        self.journal.apply(txid, {
            "action": "put",
            "key": path,
            "value": value,
        })

        self.store.put(path, value)
        self.store.save()

        self.journal.commit(txid)

    def delete_metadata(self, path: str) -> None:
        """
        Delete metadata for a path with journaling.
        """
        txid = self.journal.begin("delete", path=path)
        self.journal.apply(txid, {
            "action": "delete",
            "key": path,
        })

        self.store.delete(path)
        self.store.save()

        self.journal.commit(txid)
