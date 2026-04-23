"""Analyze seam for KB."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import traceback
import json

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


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _latest_chunk_set_path(cfg: KBConfig) -> Optional[Path]:
    candidates = sorted(cfg.chunk_sets_dir.glob("*.chunk_set.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


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
    summary_path = cfg.summaries_dir / f"{run_id}.summary.json"
    input_artifacts = []

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
        inputs={"items": []},
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

        chunk_set_path = _latest_chunk_set_path(cfg)
        if chunk_set_path is not None:
            chunk_set = json.loads(chunk_set_path.read_text(encoding="utf-8"))
            chunks = list(chunk_set.get("chunks", []))
            if max_nodes is not None:
                chunks = chunks[: int(max_nodes)]
            nodes = chunks
            run_record["inputs"]["items"].append(
                {
                    "input_kind": "chunk_set",
                    "path": str(chunk_set_path),
                    "artifact_family": chunk_set.get("artifact_family"),
                    "artifact_kind": chunk_set.get("artifact_kind"),
                    "run_id": chunk_set.get("run_id"),
                }
            )
            input_artifacts.append({"path": str(chunk_set_path), "artifact_kind": "chunk_set", "artifact_family": "chunk_bus"})
        else:
            from kb.vectorstore.chroma_client import ChromaConfig, get_collection
            from kb.vectorstore.chroma_io import load_vectors_and_min_nodes

            chroma_cfg = ChromaConfig(chroma_dir=cfg.chroma_dir, collection_name=cfg.collection_name, allow_reset=...)
            _, coll = get_collection(chroma_cfg, reset=False)
            vecs, nodes = load_vectors_and_min_nodes(coll, batch_size=int(batch_size))
            if max_nodes is not None and vecs.shape[0] > int(max_nodes):
                vecs = vecs[: int(max_nodes)]
                nodes = nodes[: int(max_nodes)]
            run_record["inputs"]["items"].append({"input_kind": "collection", "collection": cfg.collection_name})
            input_artifacts.append({"collection": cfg.collection_name, "artifact_kind": "collection_snapshot", "artifact_family": "chunk_bus"})

        run_record["counters"]["nodes_loaded"] = int(len(nodes))
        complete_stage(run_record, "parse", success=True, details={"nodes_loaded": int(len(nodes))})
        complete_stage(run_record, "export", success=True, details={"state": "started"})

        if len(nodes) == 0:
            combined = "# combined_notes\n\n(no nodes in collection)\n"
            _write_text_atomic(export_path, combined)
        else:
            if chunk_set_path is not None:
                order = list(range(len(nodes)))
            else:
                from scipy.cluster.hierarchy import leaves_list, linkage
                from scipy.spatial.distance import pdist

                Z = linkage(pdist(vecs, metric="cosine"), method="average")
                order = leaves_list(Z)

            parts = ["# combined_notes", f"\nGenerated at {utc_now_iso()} from collection '{cfg.collection_name}'.\n"]
            for idx in order:
                n = nodes[int(idx)]
                if isinstance(n, dict):
                    hdr = (n.get("header_path") or [])
                    text = str(n.get("text", ""))
                else:
                    hdr = (n.metadata or {}).get("header_path")
                    text = n.text
                hdr_str = "/".join(hdr) if isinstance(hdr, list) else (str(hdr) if hdr else "")
                parts.append("\n---\n")
                if hdr_str:
                    parts.append(f"## {hdr_str}\n")
                parts.append(text.rstrip() + "\n")
            _write_text_atomic(export_path, "\n".join(parts))

        run_record["counters"]["export_bytes"] = int(export_path.stat().st_size) if export_path.exists() else 0
        complete_stage(run_record, "export", success=True, details={"export_bytes": run_record["counters"]["export_bytes"]})

        summary_artifact = {
            "artifact_family": "summary_bus",
            "artifact_kind": "chunk_set_summary",
            "schema_version": 1,
            "run_id": run_id,
            "producer": "kb",
            "entrypoint": "kb_chat_analyze",
            "input_artifacts": input_artifacts,
            "summary_text": export_path.read_text(encoding="utf-8"),
            "export_path": str(export_path),
        }
        _write_text_atomic(summary_path, json.dumps(summary_artifact, indent=2, ensure_ascii=False))

        run_record["outputs"].update(
            {
                "summary_artifact_path": str(summary_path),
                "export_path": str(export_path),
                "run_record_path": str(rr_path),
            }
        )
        run_record["outputs"]["internal_side_effects"] = {
            "clustering_ordering": "scipy hierarchical clustering over loaded vectors",
        }
        add_output_artifact(
            run_record,
            path=summary_path,
            artifact_kind="chunk_set_summary",
            artifact_family="summary_bus",
            schema_version=1,
            promotion_status="active",
            extra={"is_primary": True, "companion_exports": [str(export_path)]},
        )
        add_output_artifact(
            run_record,
            path=export_path,
            artifact_kind="analysis_export",
            artifact_family="export",
            schema_version=1,
            extra={"is_companion": True},
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
