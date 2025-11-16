from __future__ import annotations

import shutil
from pathlib import Path

from common.config import load_level0_config


def main() -> None:
    cfg = load_level0_config()
    print("Using config:", cfg)

    # metadata + journal
    for p in [cfg.metadata_file, cfg.journal_file]:
        if p.exists():
            print("Removing", p)
            p.unlink()

    # data dir (remove all blocks)
    if cfg.data_dir.exists():
        print("Clearing data dir", cfg.data_dir)
        for child in cfg.data_dir.iterdir():
            if child.is_file():
                print("  Removing block", child)
                child.unlink()
    else:
        print("Creating data dir", cfg.data_dir)
        cfg.data_dir.mkdir(parents=True, exist_ok=True)

    print("Reset complete.")


if __name__ == "__main__":
    main()
