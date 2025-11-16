# AegisFS — Distributed File System
AegisFS is a minimal but fully functional **distributed file system** featuring:
- **Crash-safe metadata** with a write-ahead log (WAL)
- **Metadata Server (MDS)** — namespace + journaling
- **DataNode** — atomic block storage using tmp→rename
- **Binary-safe, multi-block client** (text + images + audio)
- **Polished CLI** for real file operations
- **Live Streamlit visualizer** with block graphs and previews
AegisFS implements the complete **Level-2 DFS pipeline**: block chunking, RPC-based block commits, metadata journaling, crash recovery, and end-to-end readback for both text and binary files.

## Quick Start

### 1. Reset the filesystem (optional)
```bash
python3 reset_fs.py
```

### 2. Start the Metadata Server
```bash
python3 -m mds.server
```

### 3. Start the DataNode
```bash
python3 -m datanode.server
```

### 4. Use the CLI
```bash
python3 -m client.cli <command> ...
```

## CLI Commands

### Text Operations
```bash
python3 -m client.cli write /demo "hello"
python3 -m client.cli read /demo
python3 -m client.cli stat /demo
python3 -m client.cli ls
python3 -m client.cli rm /demo
```

### Binary-Safe File Transfer
```bash
python3 -m client.cli put local.bin /uploads/local.bin
python3 -m client.cli get /uploads/local.bin local.bin
```

## Streamlit Visualizer
```bash
streamlit run app.py
```

## Architecture Overview

### Metadata Server
- WAL journaling
- Crash recovery
- JSON-RPC API

### DataNode
- Atomic block writes
- Base64 RPC for binary safety

### Client
- Multi-block write/read pipeline
- Text + binary support

## Example Demo
```bash
python3 reset_fs.py
python3 -m client.cli write /hello "Hello AegisFS!"
python3 -m client.cli put image.png /uploads/image.png
python3 -m client.cli stat /uploads/image.png
python3 -m client.cli get /uploads/image.png recovered.png
```

## Status
| Component | Feature | Status |
|----------|---------|--------|
| Level 0 | WAL + Crash Recovery | ✅ |
| Level 1 | RPC Servers | ✅ |
| Level 2 | Chunked Pipeline | ✅ |
| Binary Support | Images/Audio/Video | ✅ |
| UI | Streamlit Visualizer | ✅ |
