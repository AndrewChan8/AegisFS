from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import socket
import threading
import tempfile
import time
from pathlib import Path

from common.rpc import RpcConnection
from datanode.server import serve_datanode


def run():
    print("=== DataNode RPC Test ===")
    with tempfile.TemporaryDirectory() as tmp:
        cfgfile = Path(tmp) / "config.json"
        cfgfile.write_text(f'{{"root_dir": "{tmp}"}}')
        os.environ["AEGISFS_CONFIG"] = str(cfgfile)

        t = threading.Thread(
            target=serve_datanode,
            kwargs={"host": "127.0.0.1", "port": 9201},
            daemon=True,
        )
        t.start()

        # wait for server
        for _ in range(30):
            try:
                s = socket.create_connection(("127.0.0.1", 9201))
                break
            except ConnectionRefusedError:
                time.sleep(0.1)
        else:
            raise RuntimeError("DataNode server did not start in time")

        conn = RpcConnection(s)
        conn.send({
            "op": "store_block",
            "args": {"block_id": "b5", "data": "XYZ"}
        })
        assert conn.recv()["ok"]
        conn.close()

        # read back
        for _ in range(30):
            try:
                s2 = socket.create_connection(("127.0.0.1", 9201))
                break
            except ConnectionRefusedError:
                time.sleep(0.1)

        conn2 = RpcConnection(s2)
        conn2.send({
            "op": "read_block",
            "args": {"block_id": "b5"}
        })
        resp = conn2.recv()
        assert resp["data"] == "XYZ"

        print("PASS: DataNode RPC correct.")


if __name__ == "__main__":
    run()
