"""
pipelines/chat_analyze.py

Purpose
- Load all embedded nodes from the Chroma collection.
- Perform a simple clustering/ordering pass (hierarchical dendrogram leaf order).
- Export a single combined markdown file (combined_notes.md) into artifacts/exports.
- Emit run_record.json.

This is intentionally minimal: it creates an analysis *artifact* you can inspect and iterate on.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import json
import traceback
import datetime as dt

import numpy as np

from kb.config.kb_config import KBConfig, load_config
from kb.vectorstore.chroma_client import ChromaConfig, get_collection
from kb.vectorstore.chroma_io import load_vectors_and_min_nodes


def _utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _make_run_id(prefix: str = "kb_chat_analyze") -> str:
    return f"{prefix}_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


@dataclass(frozen=True)
class AnalyzeResult:
    run_record_path: Path
    export_path: Path
    run_record: Dict[str, Any]


def analyze(
    *,
    cfg: Optional[KBConfig] = None,
    export_name: str = "combined_notes.md",
    batch_size: int = 500,
    max_nodes: int | None = None,
) -> AnalyzeResult:
    cfg = cfg or load_config()
    cfg.ensure_dirs()

    run_id = _make_run_id()
    rr_path = cfg.run_records_dir / f"{run_id}.run_record.json"
    export_path = cfg.exports_dir / export_name

    run_record: Dict[str, Any] = {
        "run_id": run_id,
        "operator": "kb.chat_analyze",
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "status": "running",
        "config": {
            "kb_root": str(cfg.kb_root),
            "chroma_dir": str(cfg.chroma_dir),
            "collection": cfg.collection_name,
            "batch_size": int(batch_size),
            "max_nodes": max_nodes,
            "export_path": str(export_path),
        },
        "inputs": {},
        "outputs": {},
        "stats": {"nodes_loaded": 0, "export_bytes": 0},
        "errors": [],
    }

    try:
        chroma_cfg = ChromaConfig(chroma_dir=cfg.chroma_dir, collection_name=cfg.collection_name, allow_reset=...)
        client, coll = get_collection(chroma_cfg, reset=False)

        vecs, nodes = load_vectors_and_min_nodes(coll, batch_size=int(batch_size))
        if max_nodes is not None and vecs.shape[0] > int(max_nodes):
            vecs = vecs[: int(max_nodes)]
            nodes = nodes[: int(max_nodes)]

        run_record["stats"]["nodes_loaded"] = int(vecs.shape[0])

        if vecs.shape[0] == 0:
            combined = "# combined_notes\n\n(no nodes in collection)\n"
            _write_text_atomic(export_path, combined)
        else:
            # Hierarchical clustering leaf ordering (cosine distance, average linkage)
            from scipy.spatial.distance import pdist
            from scipy.cluster.hierarchy import linkage, leaves_list

            Z = linkage(pdist(vecs, metric="cosine"), method="average")
            order = leaves_list(Z)

            parts = []
            parts.append("# combined_notes")
            parts.append(f"\nGenerated at {_utc_now_iso()} from collection '{cfg.collection_name}'.\n")

            for idx in order:
                n = nodes[int(idx)]
                hdr = (n.metadata or {}).get("header_path")
                hdr_str = "/".join(hdr) if isinstance(hdr, list) else (str(hdr) if hdr else "")
                parts.append("\n---\n")
                if hdr_str:
                    parts.append(f"## {hdr_str}\n")
                parts.append(n.text.rstrip() + "\n")

            combined = "\n".join(parts)
            _write_text_atomic(export_path, combined)

        run_record["outputs"] = {
            "export_path": str(export_path),
            "run_record_path": str(rr_path),
        }
        run_record["finished_at"] = _utc_now_iso()
        run_record["status"] = "ok"
        run_record["stats"]["export_bytes"] = int(export_path.stat().st_size) if export_path.exists() else 0

    except Exception as e:
        run_record["status"] = "error"
        run_record["finished_at"] = _utc_now_iso()
        run_record["errors"].append({
            "type": "exception",
            "message": str(e),
            "traceback": traceback.format_exc(),
        })

    finally:
        try:
            _write_json_atomic(rr_path, run_record)
        except Exception:
            pass

    return AnalyzeResult(run_record_path=rr_path, export_path=export_path, run_record=run_record)
