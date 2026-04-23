"""
pipelines/papers_grobid.py

Purpose
- Wrap the legacy grobid_ingest.py runner as a pipeline with a run_record.json.
- Keep this as an integration seam. We are not refactoring GROBID parsing here.

Expected behavior
- Takes one PDF path at a time (like the legacy script).
- Optionally posts to a running GROBID service (legacy flag).
- Optionally emits TEI XML and/or upserts into a Chroma dir.

This pipeline does NOT force using your KB buses yet; it just standardizes run recording.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import json
import traceback
import datetime as dt

from kb.config.kb_config import KBConfig, load_config


def _utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _make_run_id(prefix: str = "kb_papers_grobid") -> str:
    return f"{prefix}_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


@dataclass(frozen=True)
class GrobidResult:
    run_record_path: Path
    run_record: Dict[str, Any]


def run_pdf(
    pdf_path: Path,
    *,
    cfg: Optional[KBConfig] = None,
    do_post_grobid: bool = True,
    save_tei: Optional[Path] = None,
    chroma_dir: Optional[Path] = None,
    emit_langchain: bool = False,
) -> GrobidResult:
    cfg = cfg or load_config()
    cfg.ensure_dirs()

    run_id = _make_run_id()
    rr_path = cfg.run_records_dir / f"{run_id}.run_record.json"

    run_record: Dict[str, Any] = {
        "run_id": run_id,
        "operator": "kb.papers_grobid",
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "status": "running",
        "config": {
            "kb_root": str(cfg.kb_root),
        },
        "inputs": {
            "pdf_path": str(Path(pdf_path)),
            "do_post_grobid": bool(do_post_grobid),
            "save_tei": str(save_tei) if save_tei else None,
            "chroma_dir": str(chroma_dir) if chroma_dir else None,
            "emit_langchain": bool(emit_langchain),
        },
        "outputs": {},
        "stats": {},
        "errors": [],
    }

    try:
        # Import the legacy script (user-provided) at runtime.
        # Put it on PYTHONPATH or keep it vendored alongside this package.
        import grobid_ingest  # expects grobid_ingest.py to be importable

        grobid_ingest.run(
            str(pdf_path),
            do_post_grobid=bool(do_post_grobid),
            save_tei=str(save_tei) if save_tei else None,
            chroma_dir=str(chroma_dir) if chroma_dir else None,
            emit_langchain=bool(emit_langchain),
        )

        run_record["status"] = "ok"
        run_record["outputs"] = {
            "run_record_path": str(rr_path),
            "save_tei": str(save_tei) if save_tei else None,
            "chroma_dir": str(chroma_dir) if chroma_dir else None,
        }
        run_record["finished_at"] = _utc_now_iso()

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

    return GrobidResult(run_record_path=rr_path, run_record=run_record)
