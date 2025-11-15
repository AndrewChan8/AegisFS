from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Level0Config:
    root_dir: Path
    metadata_file: Path
    journal_file: Path
    data_dir: Path
    log_dir: Path

    @classmethod
    def from_file(cls, path: str | Path) -> "Level0Config":
        path = Path(path)
        cfg = json.loads(path.read_text())

        root = Path(cfg.get("root_dir", ".")).resolve()

        return cls(
            root_dir=root,
            metadata_file=root / cfg.get("metadata_file", "mds_metadata.json"),
            journal_file=root / cfg.get("journal_file", "mds_journal.log"),
            data_dir=root / cfg.get("data_dir", "data"),
            log_dir=root / cfg.get("log_dir", "logs"),
        )


def load_level0_config() -> Level0Config:
    config_path = os.environ.get("AEGISFS_CONFIG", "config.json")
    return Level0Config.from_file(config_path)

