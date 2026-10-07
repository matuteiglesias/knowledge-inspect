"""Producer-owned deterministic export from summary_bus artifacts to generic JSONL evidence."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable

ADAPTER_CONTRACT = "producer-local:knowledge-inspect.evidence-jsonl@1"
EXPECTED_FAMILY = "summary_bus"
EXPECTED_KIND = "chunk_set_summary"
EXPECTED_SCHEMA_VERSION = 1
EXPECTED_PRODUCER = "kb"


class EvidenceExportError(ValueError):
    """Raised when a producer summary cannot be safely projected."""


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_summary(path: Path) -> tuple[dict[str, Any], str]:
    if path.is_symlink() or not path.is_file():
        raise EvidenceExportError(f"summary input must be a regular file: {path.name}")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceExportError(f"summary input is not valid UTF-8 JSON: {path.name}") from exc
    if not isinstance(payload, dict):
        raise EvidenceExportError(f"summary input must be a JSON object: {path.name}")
    return payload, _sha256_bytes(raw)


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise EvidenceExportError(f"summary input requires non-empty {key}")
    return value.strip()


def _safe_upstream(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("input_artifacts")
    if not isinstance(raw, list):
        raise EvidenceExportError("summary input requires input_artifacts list")

    result: list[dict[str, Any]] = []
    allowed = ("artifact_kind", "artifact_family", "run_id", "sha256")
    for item in raw:
        if not isinstance(item, dict):
            raise EvidenceExportError("input_artifacts entries must be objects")
        projected = {
            key: item[key]
            for key in allowed
            if isinstance(item.get(key), str) and item[key]
        }
        if projected:
            result.append(projected)
    return result


def summary_to_evidence_record(
    payload: dict[str, Any],
    *,
    source_artifact_sha256: str,
) -> dict[str, Any]:
    if payload.get("artifact_family") != EXPECTED_FAMILY:
        raise EvidenceExportError("unsupported artifact_family")
    if payload.get("artifact_kind") != EXPECTED_KIND:
        raise EvidenceExportError("unsupported artifact_kind")
    if payload.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        raise EvidenceExportError("unsupported summary schema_version")
    if payload.get("producer") != EXPECTED_PRODUCER:
        raise EvidenceExportError("unsupported producer")

    run_id = _required_text(payload, "run_id")
    entrypoint = _required_text(payload, "entrypoint")
    summary_text = _required_text(payload, "summary_text")
    source_ref = f"knowledge-inspect:summary:{run_id}"
    text_sha256 = _sha256_bytes(summary_text.encode("utf-8"))

    return {
        "source_ref": source_ref,
        "summary": summary_text,
        "title": f"Knowledge Inspect summary {run_id}",
        "tags": ["knowledge-inspect", "summary-bus"],
        "text_sha256": text_sha256,
        "meta": {
            "adapter_contract": ADAPTER_CONTRACT,
            "producer": EXPECTED_PRODUCER,
            "entrypoint": entrypoint,
            "run_id": run_id,
            "artifact_family": EXPECTED_FAMILY,
            "artifact_kind": EXPECTED_KIND,
            "source_artifact_sha256": source_artifact_sha256,
            "input_artifacts": _safe_upstream(payload),
            "provenance": {
                "source_ref": source_ref,
            },
        },
    }


def export_summary_bus(
    paths: Iterable[str | os.PathLike[str]],
    *,
    output: str | os.PathLike[str],
) -> dict[str, Any]:
    source_paths = sorted(
        (Path(path).expanduser() for path in paths),
        key=lambda path: path.as_posix(),
    )
    if not source_paths:
        raise EvidenceExportError("at least one summary input is required")

    target = Path(output).expanduser()
    if target.exists():
        raise EvidenceExportError("output already exists; refusing to overwrite")
    if target.is_symlink() or any(parent.is_symlink() for parent in target.absolute().parents):
        raise EvidenceExportError("output path must not contain a symlink")

    records: list[dict[str, Any]] = []
    seen_refs: set[str] = set()
    source_sha256s: list[str] = []
    for path in source_paths:
        payload, source_sha256 = _load_summary(path)
        record = summary_to_evidence_record(
            payload,
            source_artifact_sha256=source_sha256,
        )
        source_ref = str(record["source_ref"])
        if source_ref in seen_refs:
            raise EvidenceExportError(f"duplicate source_ref: {source_ref}")
        seen_refs.add(source_ref)
        source_sha256s.append(source_sha256)
        records.append(record)

    rendered = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        for record in records
    ).encode("utf-8")

    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise

    return {
        "contract": ADAPTER_CONTRACT,
        "record_count": len(records),
        "source_artifact_sha256s": source_sha256s,
        "output_sha256": _sha256_bytes(rendered),
        "output_bytes": len(rendered),
    }
