from __future__ import annotations

from pathlib import Path

from common.config import load_level0_config


def nuke(path: Path) -> None:
    if path.exists():
        print(f"Removing {path}")
        path.unlink()
    else:
        print(f"(missing) {path}")


def main() -> None:
    cfg = load_level0_config()
    print("Using config:")
    print("  root_dir     =", cfg.root_dir)
    print("  metadata_file=", cfg.metadata_file)
    print("  journal_file =", cfg.journal_file)
    print("  data_dir     =", cfg.data_dir)
    print("  log_dir      =", cfg.log_dir)

    # 1) kill metadata + journal
    nuke(cfg.metadata_file)
    nuke(cfg.journal_file)

    # 2) clear all blocks in data_dir
    if cfg.data_dir.exists():
        print(f"Clearing data dir {cfg.data_dir}")
        for child in cfg.data_dir.iterdir():
            if child.is_file():
                print("  Removing block", child)
                child.unlink()
    else:
        print(f"Creating data dir {cfg.data_dir}")
        cfg.data_dir.mkdir(parents=True, exist_ok=True)

    print("Reset complete.")


if __name__ == "__main__":
    main()
