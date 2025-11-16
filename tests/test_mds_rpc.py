from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import socket
import threading
import tempfile
import time
from pathlib import Path
from common.rpc import RpcConnection
from mds.server import serve_mds


def run():
    print("=== MDS RPC Test ===")
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "config.json"
        cfg_path.write_text(f'{{"root_dir": "{tmp}"}}')
        os.environ["AEGISFS_CONFIG"] = str(cfg_path)

        t = threading.Thread(
            target=serve_mds,
            kwargs={"host": "127.0.0.1", "port": 9100},
            daemon=True,
        )
        t.start()

        # wait for server
        for _ in range(30):
            try:
                s = socket.create_connection(("127.0.0.1", 9100))
                break
            except ConnectionRefusedError:
                time.sleep(0.1)
        else:
            raise RuntimeError("MDS server did not start in time")

        # put_meta
        conn = RpcConnection(s)
        conn.send({
            "op": "put_meta",
            "args": {"path": "/abc", "value": {"v": 1}}
        })
        assert conn.recv()["ok"]
        conn.close()

        # get_meta
        for _ in range(30):
            try:
                s2 = socket.create_connection(("127.0.0.1", 9100))
                break
            except ConnectionRefusedError:
                time.sleep(0.1)

        conn2 = RpcConnection(s2)
        conn2.send({
            "op": "get_meta",
            "args": {"path": "/abc"}
        })
        resp2 = conn2.recv()
        assert resp2["value"] == {"v": 1}

        print("PASS: MDS RPC works.")


if __name__ == "__main__":
    run()
