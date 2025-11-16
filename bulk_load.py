# bulk_load.py — quickly populate AegisFS with many files
from __future__ import annotations
from client.fs_client import AegisClient

client = AegisClient()

def main() -> None:
    # small text files
    for i in range(1, 51):
        path = f"/demo/small_{i}.txt"
        client.write_file(path, f"Small demo file #{i} for AegisFS.\n")

    # larger multi-block files
    big_payload = ("AegisFS big demo file line\n" * 2000)
    for i in range(1, 11):
        path = f"/demo/big_{i}.txt"
        client.write_file(path, big_payload)

    print("Loaded 50 small + 10 big demo files into AegisFS.")

if __name__ == "__main__":
    main()
