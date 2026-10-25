"""Source-owned, metadata-only run evidence for *actual* speech consumer calls.

The producer (politics-wiki) keeps source authority. We only record the
consumer action, its input/derived output hashes and true execution times.
No speech text, search query, or private local path enters the receipt manifest.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def dump(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def record_speech_run(
    *, root: Path, mode: str, started_at: str, finished_at: str,
    source_file: Path, output_file: Path,
) -> Path:
    if mode not in {"index", "query"}:
        raise ValueError("unsupported speech consumer operation")
    if not (source_file.is_file() and output_file.is_file()):
        raise ValueError("source and output evidence files must exist")
    # Each invocation receives a unique run identity, even when byte-identical
    # derived output is reproduced from the same immutable speech projection.
    timestamp = started_at.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")
    digest_seed = hashlib.sha256((
        str(time.time_ns()) + mode + sha256(source_file) + sha256(output_file)
    ).encode()).hexdigest()[:12]
    rid = f"kb_speech_{mode}_{timestamp}_{digest_seed}"
    directory = root / rid
    directory.mkdir(parents=True, exist_ok=False)
    artifact_kind = "speech_lexical_index" if mode == "index" else "speech_query_results"
    manifest = {
        "manifest_version": 2,
        "run_id": rid,
        "producer": "matuteiglesias/knowledge-inspect",
        "operator": f"kb.speech_{mode}",
        "status": "success",
        "created_at": started_at,
        "completed_at": finished_at,
        "artifacts": [{
            "artifact_kind": artifact_kind,
            "sha256": sha256(output_file),
            "input_sha256": sha256(source_file),
        }],
    }
    record = {
        "run_id": rid,
        "operator": f"kb.speech_{mode}",
        "status": "success",
        "created_at": started_at,
        "completed_at": finished_at,
        "evidence": {
            "input_sha256": sha256(source_file),
            "derived_output_sha256": sha256(output_file),
            "output_path": str(output_file.resolve()),
            "artifact_kind": artifact_kind,
        },
    }
    dump(directory / "run_record.json", record)
    dump(directory / "manifest.json", manifest)
    dump(directory / "source_proof.json", {
        "contract": "observability:source-execution@1",
        "producer_id": "producer.manual.knowledge-inspect-speech",
        "run_id": rid,
        "source_kind": "producer_run_record",
        "started_at": started_at,
        "finished_at": finished_at,
        "manifest_sha256": sha256(directory / "manifest.json"),
    })
    return directory
