"""papers grobid seam."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from kb.config.kb_config import KBConfig, load_config
from kb.pipelines.run_record_contract import (
    add_output_artifact,
    attach_exception,
    complete_stage,
    finalize_and_write_contract_artifacts,
    make_run_id,
    make_run_record,
)


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

    run_id = make_run_id("kb_papers_grobid")
    rr_path = cfg.run_records_dir / f"{run_id}.run_record.json"

    run_record = make_run_record(
        cfg=cfg,
        run_id=run_id,
        entrypoint="kb_papers_grobid",
        operator="kb.papers_grobid",
        config={"kb_root": str(cfg.kb_root)},
        inputs={
            "items": [
                {
                    "input_kind": "pdf",
                    "pdf_path": str(Path(pdf_path)),
                    "do_post_grobid": bool(do_post_grobid),
                    "save_tei": str(save_tei) if save_tei else None,
                    "chroma_dir": str(chroma_dir) if chroma_dir else None,
                    "emit_langchain": bool(emit_langchain),
                }
            ]
        },
        stage_defs=[
            {"name": "config_load"},
            {"name": "input_resolution"},
            {"name": "parse"},
            {"name": "embed_persist"},
            {"name": "contract_artifact_emission"},
        ],
        counters={},
    )

    try:
        complete_stage(run_record, "config_load", success=True)
        complete_stage(run_record, "input_resolution", success=True)
        complete_stage(run_record, "parse", success=True, details={"state": "started"})

        import grobid_ingest

        grobid_ingest.run(
            str(pdf_path),
            do_post_grobid=bool(do_post_grobid),
            save_tei=str(save_tei) if save_tei else None,
            chroma_dir=str(chroma_dir) if chroma_dir else None,
            emit_langchain=bool(emit_langchain),
        )

        complete_stage(run_record, "parse", success=True)
        complete_stage(run_record, "embed_persist", success=True)

        run_record["outputs"].update(
            {
                "run_record_path": str(rr_path),
                "save_tei": str(save_tei) if save_tei else None,
                "chroma_dir": str(chroma_dir) if chroma_dir else None,
            }
        )
        if save_tei:
            add_output_artifact(
                run_record,
                path=Path(save_tei),
                artifact_kind="tei_xml",
                artifact_family="grobid",
                schema_version=1,
            )

    except Exception as e:
        complete_stage(run_record, "parse", success=False)
        complete_stage(run_record, "embed_persist", success=False)
        attach_exception(run_record, e)

    finally:
        run_record["outputs"]["run_record_path"] = str(rr_path)
        requested_status = "error" if run_record.get("errors") else "success"
        finalize_and_write_contract_artifacts(
            cfg=cfg,
            run_record=run_record,
            rr_path=rr_path,
            requested_status=requested_status,
        )

    return GrobidResult(run_record_path=rr_path, run_record=run_record)
