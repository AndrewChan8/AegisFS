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
from datanode.server import serve_datanode


def run():
    print("=== End-to-End MDS ↔ DataNode Pipeline Test ===")
    with tempfile.TemporaryDirectory() as tmp:
        cfg = Path(tmp) / "config.json"
        cfg.write_text(f'{{"root_dir": "{tmp}"}}')
        os.environ["AEGISFS_CONFIG"] = str(cfg)

        # start both servers
        threading.Thread(
            target=serve_mds,
            kwargs={"host": "127.0.0.1", "port": 9300},
            daemon=True,
        ).start()

        threading.Thread(
            target=serve_datanode,
            kwargs={"host": "127.0.0.1", "port": 9301},
            daemon=True,
        ).start()

        # wait for MDS
        for _ in range(30):
            try:
                sm = socket.create_connection(("127.0.0.1", 9300))
                break
            except ConnectionRefusedError:
                time.sleep(0.1)

        # wait for DataNode
        for _ in range(30):
            try:
                sd = socket.create_connection(("127.0.0.1", 9301))
                break
            except ConnectionRefusedError:
                time.sleep(0.1)

        # write block to datanode
        conn_dn = RpcConnection(sd)
        conn_dn.send({
            "op": "store_block",
            "args": {"block_id": "b100", "data": "HELLO"}
        })
        assert conn_dn.recv()["ok"]
        conn_dn.close()

        # tell MDS the file references the block
        conn_mds = RpcConnection(sm)
        conn_mds.send({
            "op": "put_meta",
            "args": {"path": "/f", "value": {"blocks": ["b100"]}}
        })
        assert conn_mds.recv()["ok"]
        conn_mds.close()

        # read back from datanode
        for _ in range(30):
            try:
                sr = socket.create_connection(("127.0.0.1", 9301))
                break
            except ConnectionRefusedError:
                time.sleep(0.1)

        conn_read = RpcConnection(sr)
        conn_read.send({
            "op": "read_block",
            "args": {"block_id": "b100"}
        })
        resp = conn_read.recv()
        assert resp["data"] == "HELLO"

        print("PASS: End-to-end pipeline is correct.")


if __name__ == "__main__":
    run()
