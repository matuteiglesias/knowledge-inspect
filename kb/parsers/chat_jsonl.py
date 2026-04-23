"""
chat_jsonl.py

Chat export JSONL -> Markdown Document + node parsing.

Core idea (from dev5):
- Build a daily markdown document containing assistant messages, titled sections, timestamps fileciteturn2file10L18-L28
- Parse markdown into nodes using MarkdownNodeParser(include_metadata=True) fileciteturn2file10L70-L71

This module is intentionally conservative:
- It does NOT assume a single JSONL schema beyond a few fields.
- It keeps provenance in metadata so IDs are stable.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import datetime as dt
import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

from llama_index.core import Document
from llama_index.core.node_parser import MarkdownNodeParser


def _ms_to_iso(ts_ms: int) -> str:
    return dt.datetime.fromtimestamp(ts_ms / 1000).isoformat(timespec="seconds")


def chat_jsonl_to_markdown(
    path: Path,
    *,
    role_filter: str = "assistant",
    title_key: str = "title",
    content_key: str = "content",
    timestamp_key: str = "timestamp",
) -> Tuple[str, Dict[str, Any]]:
    """
    Convert a single chat JSONL file to markdown text.

    Returns (markdown_text, metadata).
    """
    date = path.stem
    lines: List[str] = [f"# {date}"]
    kept = 0
    total = 0

    with path.open(encoding="utf-8") as f:
        for row in f:
            row = row.strip()
            if not row:
                continue
            total += 1
            try:
                j = json.loads(row)
            except Exception:
                continue

            if role_filter and j.get("role") != role_filter:
                continue

            ts_raw = j.get(timestamp_key)
            ts = _ms_to_iso(int(ts_raw)) if isinstance(ts_raw, (int, float, str)) and str(ts_raw).isdigit() else "unknown-ts"
            title = j.get(title_key) or "untitled"
            content = j.get(content_key) or ""
            # Use heading structure so MarkdownNodeParser produces section nodes
            lines.append(f"\n## {title}\n### {ts}\n{content}")
            kept += 1

    meta = {"file": path.name, "date": date, "total_rows": total, "kept_rows": kept}
    return "\n".join(lines), meta


def jsonl_to_document(path: Path, **kwargs: Any) -> Document:
    md, meta = chat_jsonl_to_markdown(path, **kwargs)
    return Document(text=md, metadata=meta)


def node_id_from_parts(*parts: str) -> str:
    """
    Stable, content-addressed id helper.
    Use for both embedding cache IDs and Chroma IDs.
    """
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8", errors="ignore"))
        h.update(b"\n")
    return h.hexdigest()


def node_id_from_node_text(node_text: str, *, source_file: str, header_path: str | None = None) -> str:
    """
    One reasonable default for node IDs:
    - file + header_path (if any) + text hash.
    """
    return node_id_from_parts(source_file, header_path or "", node_text)


def parse_markdown_nodes(doc: Document, *, include_metadata: bool = True) -> list:
    """
    Parse markdown into nodes (llama_index nodes).
    """
    parser = MarkdownNodeParser(include_metadata=include_metadata)
    nodes = parser.get_nodes_from_documents([doc])
    return nodes


def filter_substantive_nodes(nodes: Iterable, *, min_newlines: int = 1) -> list:
    """
    Your drafts often skip single-line nodes. fileciteturn2file10L71-L72
    """
    out = []
    for n in nodes:
        txt = getattr(n, "text", "") or ""
        if txt.count("\n") < min_newlines:
            continue
        if not txt.strip():
            continue
        out.append(n)
    return out
