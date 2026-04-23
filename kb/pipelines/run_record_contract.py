from __future__ import annotations

import datetime as dt
import json
import platform
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from kb.config.kb_config import KBConfig

RUN_RECORD_VERSION = 2
PROJECT = "kb"
SCHEMA_VERSIONS = {
    "run_record": 2,
    "manifest": 1,
    "observability": 1,
}
CONTRACTUAL_STATUSES = {"success", "empty_success", "partial_success", "error"}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_run_id(entrypoint: str) -> str:
    return f"{entrypoint}_{dt.datetime.now(dt.UTC).strftime('%Y%m%dT%H%M%SZ')}"


def write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _normalize_stage_defs(stage_defs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    stages: List[Dict[str, Any]] = []
    for stage in stage_defs:
        stages.append({
            "name": stage["name"],
            "status": "pending",
            "started_at": None,
            "completed_at": None,
            "details": stage.get("details", {}),
        })
    return stages


def _safe_environment(cfg: KBConfig) -> Dict[str, Any]:
    return {
        "python_version": sys.version.split(" ")[0],
        "platform": platform.platform(),
        "kb_root": str(cfg.kb_root),
        "artifacts_dir": str(cfg.artifacts_dir),
    }


def make_run_record(
    *,
    cfg: KBConfig,
    run_id: str,
    entrypoint: str,
    operator: str,
    config: Dict[str, Any],
    inputs: Dict[str, Any],
    stage_defs: Iterable[Dict[str, Any]],
    counters: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    created_at = utc_now_iso()
    return {
        "run_record_version": RUN_RECORD_VERSION,
        "schema_versions": dict(SCHEMA_VERSIONS),
        "project": PROJECT,
        "run_id": run_id,
        "entrypoint": entrypoint,
        "operator": operator,
        "created_at": created_at,
        "completed_at": None,
        "status": "error",
        "config": config,
        "environment": _safe_environment(cfg),
        "stages": _normalize_stage_defs(stage_defs),
        "warnings": [],
        "inputs": inputs,
        "outputs": {"artifacts": []},
        "counters": counters or {},
        # Compatibility field; callers may still read this while moving to counters.
        "stats": counters or {},
        "errors": [],
    }


def start_stage(run_record: Dict[str, Any], stage_name: str) -> None:
    for stage in run_record.get("stages", []):
        if stage.get("name") == stage_name:
            stage["status"] = "running"
            stage["started_at"] = utc_now_iso()
            return


def complete_stage(run_record: Dict[str, Any], stage_name: str, *, success: bool, details: Optional[Dict[str, Any]] = None) -> None:
    for stage in run_record.get("stages", []):
        if stage.get("name") == stage_name:
            stage["status"] = "success" if success else "error"
            if stage.get("started_at") is None:
                stage["started_at"] = utc_now_iso()
            stage["completed_at"] = utc_now_iso()
            if details:
                stage["details"] = {**(stage.get("details") or {}), **details}
            return


def add_output_artifact(
    run_record: Dict[str, Any],
    *,
    path: Path,
    artifact_kind: str,
    artifact_family: Optional[str] = None,
    schema_version: Optional[int] = None,
    promotion_status: str = "working",
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    record: Dict[str, Any] = {
        "path": str(path),
        "artifact_kind": artifact_kind,
        "artifact_family": artifact_family,
        "schema_version": schema_version,
        "promotion_status": promotion_status,
    }
    if extra:
        record.update(extra)
    run_record.setdefault("outputs", {}).setdefault("artifacts", []).append(record)
    return record


def attach_exception(run_record: Dict[str, Any], exc: Exception) -> None:
    run_record.setdefault("errors", []).append(
        {
            "type": "exception",
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
    )


def _derive_final_status(run_record: Dict[str, Any], requested_status: str) -> str:
    if requested_status == "error":
        return "error"
    counters = run_record.get("counters", {}) or {}
    has_errors = bool(run_record.get("errors"))
    if requested_status == "empty_success":
        return "partial_success" if has_errors else "empty_success"
    if has_errors:
        return "partial_success"
    nodes = int(counters.get("nodes_loaded", counters.get("nodes_kept", 0)) or 0)
    if nodes == 0:
        return "empty_success"
    return "success"


def finalize_and_write_contract_artifacts(
    *,
    cfg: KBConfig,
    run_record: Dict[str, Any],
    rr_path: Path,
    requested_status: str,
) -> Dict[str, str]:
    final_status = _derive_final_status(run_record, requested_status=requested_status)
    if final_status not in CONTRACTUAL_STATUSES:
        final_status = "error"

    run_record["status"] = final_status
    run_record["completed_at"] = utc_now_iso()
    run_record.setdefault("outputs", {})["run_record_path"] = str(rr_path)
    add_output_artifact(
        run_record,
        path=rr_path,
        artifact_kind="run_record",
        artifact_family="contract",
        schema_version=SCHEMA_VERSIONS["run_record"],
    )

    manifest_dir = cfg.artifacts_dir / "manifests"
    observability_dir = cfg.artifacts_dir / "observability"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    observability_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = manifest_dir / f"{run_record['run_id']}.manifest.json"
    manifest = {
        "manifest_version": SCHEMA_VERSIONS["manifest"],
        "run_id": run_record["run_id"],
        "project": run_record["project"],
        "entrypoint": run_record["entrypoint"],
        "status": run_record["status"],
        "created_at": run_record.get("created_at"),
        "completed_at": run_record.get("completed_at"),
        "artifacts": {
            "run_record": str(rr_path),
            "public_outputs": run_record.get("outputs", {}),
        },
    }
    write_json_atomic(manifest_path, manifest)

    latest_path = observability_dir / f"{run_record['operator']}.latest.json"
    latest = {
        "observability_version": SCHEMA_VERSIONS["observability"],
        "run_id": run_record["run_id"],
        "project": run_record["project"],
        "entrypoint": run_record["entrypoint"],
        "operator": run_record["operator"],
        "status": run_record["status"],
        "created_at": run_record.get("created_at"),
        "completed_at": run_record.get("completed_at"),
        "run_record_path": str(rr_path),
        "manifest_path": str(manifest_path),
    }
    write_json_atomic(latest_path, latest)

    run_record["outputs"].update(
        {
            "manifest_path": str(manifest_path),
            "observability_latest_path": str(latest_path),
        }
    )
    add_output_artifact(
        run_record,
        path=manifest_path,
        artifact_kind="manifest",
        artifact_family="contract",
        schema_version=SCHEMA_VERSIONS["manifest"],
    )
    add_output_artifact(
        run_record,
        path=latest_path,
        artifact_kind="observability_latest",
        artifact_family="contract",
        schema_version=SCHEMA_VERSIONS["observability"],
    )

    complete_stage(run_record, "contract_artifact_emission", success=True)
    try:
        write_json_atomic(rr_path, run_record)
    except Exception:
        pass

    return {"manifest_path": str(manifest_path), "observability_latest_path": str(latest_path)}
