# app.py — AegisFS Visualizer
from __future__ import annotations

import textwrap
import streamlit as st
from client.fs_client import AegisClient

client = AegisClient()

st.set_page_config(
    page_title="AegisFS Visualizer",
    layout="wide",
)

st.title("AegisFS 🔐 Distributed Filesystem Viewer")
st.caption("Metadata server + block storage visualized in real time.")

# ─────────────────────────────────────────────────────────────
# Demo controls
# ─────────────────────────────────────────────────────────────

col_demo_left, col_demo_right = st.columns(2)

with col_demo_left:
    st.subheader("Quick demo files")
    if st.button("📄 Create small demo file (/notes.txt)"):
        client.write_file("/notes.txt", "Hello from AegisFS visualizer!")
        st.success("Created /notes.txt")

with col_demo_right:
    st.subheader("Multi-block demo")
    if st.button("📦 Create large demo file (/big)"):
        big_text = "\n".join(["Aegis block test line"] * 4000)
        client.write_file("/big", big_text)
        st.success("Created /big with many blocks")

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Browser upload → AegisFS
# ─────────────────────────────────────────────────────────────

st.subheader("Upload a file into AegisFS")

upload_cols = st.columns([2, 2, 1])

with upload_cols[0]:
    uploaded = st.file_uploader("Choose a file", type=None)

with upload_cols[1]:
    default_path = ""
    if uploaded is not None:
        default_path = f"/uploads/{uploaded.name}"
    target_path = st.text_input("Target path in AegisFS", value=default_path)

with upload_cols[2]:
    st.write("")  # spacing
    st.write("")
    do_upload = st.button("⬆️ Upload")

if do_upload:
    if uploaded is None:
        st.error("No file selected.")
    elif not target_path.startswith("/"):
        st.error("Path must start with '/'.")
    else:
        try:
            # Treat upload as UTF-8 text payload, like CLI put/write.
            data_bytes = uploaded.getvalue()
            text = data_bytes.decode("utf-8")
            client.write_file(target_path, text)
            st.success(f"Uploaded {uploaded.name} → {target_path} ({len(data_bytes)} bytes)")
        except UnicodeDecodeError:
            st.error("File is not UTF-8 text; current demo client stores text files only.")

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Files + metadata view
# ─────────────────────────────────────────────────────────────

col_left, col_right = st.columns([1, 2], gap="large")

with col_left:
    st.subheader("Filesystem")
    if st.button("🔄 Refresh file list"):
        st.experimental_rerun()

    paths = sorted(client.list_paths())
    if not paths:
        st.info("Filesystem is empty. Use the demo buttons or upload a file.")
        selected_path = None
    else:
        selected_path = st.radio(
            "Select a file",
            options=paths,
            index=0,
        )

with col_right:
    st.subheader("Metadata & Blocks")
    if not selected_path:
        st.write("Select a file on the left to inspect its metadata and block layout.")
    else:
        meta = client.get_meta(selected_path) or {}
        size = meta.get("size", 0)
        blocks = meta.get("blocks", []) or []
        num_blocks = len(blocks)

        m1, m2, m3 = st.columns(3)
        m1.metric("Size (bytes)", f"{size}")
        m2.metric("# of Blocks", f"{num_blocks}")
        m3.metric("Block Size", f"{client.BLOCK_SIZE} bytes")

        st.markdown("#### Raw Metadata")
        st.code(
            textwrap.dedent(f"""\
            {{
                "path": "{selected_path}",
                "size": {size},
                "blocks": [
                    {", ".join(f'"{b}"' for b in blocks)}
                ]
            }}"""),
            language="json",
        )

        st.markdown("#### Block Layout")
        if not blocks:
            st.warning("This file has no blocks recorded.")
        else:
            dot_lines = [
                'digraph G {',
                '  rankdir=LR;',
                '  node [shape=box, style=filled, color="#0f766e", fontname="monospace"];',
                f'  file [label="{selected_path}", shape=folder, color="#1d4ed8"];',
            ]
            for i, b in enumerate(blocks):
                dot_lines.append(f'  b{i} [label="{b}"];')
                dot_lines.append(f'  file -> b{i};')
            dot_lines.append('}')
            dot_src = "\n".join(dot_lines)
            st.graphviz_chart(dot_src)

        st.markdown("#### File Preview")
        try:
            text = client.read_file(selected_path)
        except Exception as e:
            st.error(f"Error reading file: {e}")
            text = None

        if text is None:
            st.warning("File data missing or unreadable.")
        else:
            preview = text[:2000]
            if len(text) > len(preview):
                preview += "\n\n… (truncated for preview) …"
            st.text(preview)
