from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any


class MetadataStore:
    """
    Level 0: single-node metadata KV.
    Keys = file paths (strings).
    Values = arbitrary JSON-serializable dicts (e.g., {"blocks": [...], "size": int}).
    """

    def __init__(self, path: Path):
        self.path = path
        self._meta: Dict[str, Any] = {}

    def load(self) -> None:
        if self.path.exists():
            self._meta = json.loads(self.path.read_text())
        else:
            self._meta = {}

    def save(self) -> None:
        self.path.write_text(json.dumps(self._meta, indent=2))

    def get(self, key: str) -> Any:
        return self._meta.get(key)

    def put(self, key: str, value: Any) -> None:
        self._meta[key] = value

    def delete(self, key: str) -> None:
        if key in self._meta:
            del self._meta[key]
