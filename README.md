# AegisFS — Distributed File System (Level 0–2)

AegisFS is a minimal distributed filesystem featuring:

- **Crash-safe metadata** using a write-ahead log (WAL)
- **Metadata Server (MDS)** — the control plane
- **DataNode block storage** — the data plane
- **AegisClient** — the coordinating client logic
- **A command-line interface (CLI)** for real file operations
- **Real disk persistence** for metadata, journal, and blocks

This version implements full **Level-2 functionality**:
client → datanode write pipeline, metadata commit, and file readback.

---

## Quick Start

### 1. Reset the filesystem (optional but recommended)

```bash
python3 reset_fs.py
```

Clears:
- metadata snapshot  
- journal  
- all DataNode block files  

---

### 2. Start the Metadata Server (Terminal 1)

```bash
python3 -m mds.server
```

Expected:

```
[MDS] Listening on 127.0.0.1:9000
```

---

### 3. Start the DataNode (Terminal 2)

```bash
python3 -m datanode.server
```

Expected:

```
[DataNode] Listening on 127.0.0.1:9101
```

---

### 4. Use the CLI (Terminal 3)

Run commands via:

```bash
python3 -m client.cli <command> ...
```

---

## CLI Commands

### **Write a file**
```bash
python3 -m client.cli write /demo "hello world"
```

### **Read a file**
```bash
python3 -m client.cli read /demo
```

### **List all files**
```bash
python3 -m client.cli ls
```

### **Inspect metadata**
```bash
python3 -m client.cli stat /demo
```

### **Delete a file**
```bash
python3 -m client.cli rm /demo
```

---

## Smoke Test

To verify the client + pipeline:

```bash
python3 -m client.smoke_client
```

This tests:
- client → datanode block write  
- client → MDS metadata commit  
- client → MDS/datanode readback  

---

## Reset Everything

Restore AegisFS to an initial clean state:

```bash
python3 reset_fs.py
```

Removes:
- `mds_metadata.json`
- `mds_journal.log`
- all block files in `data/`

---

## Architecture Summary

**MDS (Control Plane)**  
- Stores metadata in `mds_metadata.json`  
- Logs every update in `mds_journal.log`  
- Rebuilds state using WAL replay on startup  

**DataNode (Data Plane)**  
- Stores raw blocks in `data/<block_id>.blk`  
- Crash-safe atomic writes via temporary swap files  

**Client (Coordinator)**  
- Performs Level-2 write pipeline:  
  1. Generate a new `block_id`  
  2. Write bytes to the DataNode  
  3. Commit metadata to the MDS  
- Performs Level-2 read pipeline:  
  1. Get metadata  
  2. Read block(s) from the DataNode  

---

## Example Demo

```bash
python3 reset_fs.py
python3 -m client.cli write /hello "Hello AegisFS!"
python3 -m client.cli ls
python3 -m client.cli read /hello
python3 -m client.cli stat /hello
python3 -m client.cli rm /hello
python3 -m client.cli ls
```

---

## Status

This version implements:

| Level | Feature | Status |
|-------|---------|--------|
| 0 | Metadata + Journal (WAL + Crash Recovery) | ✅ Done |
| 1 | MDS RPC + DataNode RPC | ✅ Done |
| 2 | Real client read/write pipeline | ✅ Done |
| 2 | CLI (write/read/stat/ls/rm) | ✅ Done |

AegisFS is fully end-to-end operational.

---
