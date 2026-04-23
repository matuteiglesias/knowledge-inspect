"""Analyze seam for KB."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import traceback

from kb.config.kb_config import KBConfig, load_config
from kb.pipelines.run_record_contract import (
    add_output_artifact,
    attach_exception,
    complete_stage,
    finalize_and_write_contract_artifacts,
    make_run_id,
    make_run_record,
    utc_now_iso,
)
from kb.vectorstore.chroma_client import ChromaConfig, get_collection
from kb.vectorstore.chroma_io import load_vectors_and_min_nodes


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

    run_id = make_run_id("kb_chat_analyze")
    rr_path = cfg.run_records_dir / f"{run_id}.run_record.json"
    export_path = cfg.exports_dir / export_name

    run_record = make_run_record(
        cfg=cfg,
        run_id=run_id,
        entrypoint="kb_chat_analyze",
        operator="kb.chat_analyze",
        config={
            "chroma_dir": str(cfg.chroma_dir),
            "collection": cfg.collection_name,
            "batch_size": int(batch_size),
            "max_nodes": max_nodes,
            "export_path": str(export_path),
        },
        inputs={"items": [{"input_kind": "collection", "collection": cfg.collection_name}]},
        stage_defs=[
            {"name": "config_load"},
            {"name": "input_resolution"},
            {"name": "parse"},
            {"name": "export"},
            {"name": "contract_artifact_emission"},
        ],
        counters={"nodes_loaded": 0, "export_bytes": 0},
    )

    try:
        complete_stage(run_record, "config_load", success=True)
        complete_stage(run_record, "input_resolution", success=True)
        complete_stage(run_record, "parse", success=True, details={"state": "started"})

        chroma_cfg = ChromaConfig(chroma_dir=cfg.chroma_dir, collection_name=cfg.collection_name, allow_reset=...)
        _, coll = get_collection(chroma_cfg, reset=False)

        vecs, nodes = load_vectors_and_min_nodes(coll, batch_size=int(batch_size))
        if max_nodes is not None and vecs.shape[0] > int(max_nodes):
            vecs = vecs[: int(max_nodes)]
            nodes = nodes[: int(max_nodes)]

        run_record["counters"]["nodes_loaded"] = int(vecs.shape[0])
        complete_stage(run_record, "parse", success=True, details={"nodes_loaded": int(vecs.shape[0])})
        complete_stage(run_record, "export", success=True, details={"state": "started"})

        if vecs.shape[0] == 0:
            combined = "# combined_notes\n\n(no nodes in collection)\n"
            _write_text_atomic(export_path, combined)
        else:
            from scipy.cluster.hierarchy import leaves_list, linkage
            from scipy.spatial.distance import pdist

            Z = linkage(pdist(vecs, metric="cosine"), method="average")
            order = leaves_list(Z)

            parts = ["# combined_notes", f"\nGenerated at {utc_now_iso()} from collection '{cfg.collection_name}'.\n"]
            for idx in order:
                n = nodes[int(idx)]
                hdr = (n.metadata or {}).get("header_path")
                hdr_str = "/".join(hdr) if isinstance(hdr, list) else (str(hdr) if hdr else "")
                parts.append("\n---\n")
                if hdr_str:
                    parts.append(f"## {hdr_str}\n")
                parts.append(n.text.rstrip() + "\n")
            _write_text_atomic(export_path, "\n".join(parts))

        run_record["counters"]["export_bytes"] = int(export_path.stat().st_size) if export_path.exists() else 0
        complete_stage(run_record, "export", success=True, details={"export_bytes": run_record["counters"]["export_bytes"]})

        run_record["outputs"].update({"export_path": str(export_path), "run_record_path": str(rr_path)})
        add_output_artifact(
            run_record,
            path=export_path,
            artifact_kind="analysis_export",
            artifact_family="export",
            schema_version=1,
        )
        run_record["stats"] = dict(run_record["counters"])

    except Exception as e:
        complete_stage(run_record, "parse", success=False)
        complete_stage(run_record, "export", success=False)
        attach_exception(run_record, e)

    finally:
        run_record["outputs"]["run_record_path"] = str(rr_path)
        requested_status = "error" if run_record.get("errors") else ("empty_success" if run_record["counters"].get("nodes_loaded", 0) == 0 else "success")
        finalize_and_write_contract_artifacts(
            cfg=cfg,
            run_record=run_record,
            rr_path=rr_path,
            requested_status=requested_status,
        )

    return AnalyzeResult(run_record_path=rr_path, export_path=export_path, run_record=run_record)
