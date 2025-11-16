#!/usr/bin/env python3
import subprocess
import sys

TESTS = [
    "tests.test_journal",
    "tests.test_metadata_recovery",
    "tests.test_mds_rpc",
    "tests.test_datanode_storage",
    "tests.test_datanode_rpc",
    "tests.test_end_to_end",
]

def main():
    print("=== Running All AegisFS Tests ===\n")

    for t in TESTS:
        print(f"--- Running {t} ---")
        try:
            subprocess.check_call([sys.executable, "-m", t])
            print(f"PASS: {t}\n")
        except subprocess.CalledProcessError:
            print(f"FAIL: {t}\n")
            sys.exit(1)

    print("=== ALL TESTS PASSED ===")

if __name__ == "__main__":
    main()

