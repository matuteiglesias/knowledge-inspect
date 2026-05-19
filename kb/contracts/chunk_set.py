from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts" / "chunk_set.v1.schema.json"


class ChunkSetValidationError(ValueError):
    """Raised when a chunk_set payload fails contract validation."""


def _load_schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def load_chunk_set(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _validate_without_jsonschema(payload: dict[str, Any]) -> None:
    required_top = [
        "artifact_family",
        "artifact_kind",
        "schema_version",
        "run_id",
        "producer",
        "entrypoint",
        "source_items",
        "chunk_count",
        "chunks",
    ]
    for field in required_top:
        if field not in payload:
            raise ChunkSetValidationError(f"missing required top-level field: {field}")

    chunks = payload.get("chunks")
    if not isinstance(chunks, list):
        raise ChunkSetValidationError("chunks must be a list")

    for idx, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            raise ChunkSetValidationError(f"chunk at index {idx} must be an object")
        for field in ["chunk_id", "text", "chunk_index"]:
            if field not in chunk:
                raise ChunkSetValidationError(f"chunk {idx} missing required field: {field}")
        if not chunk.get("paper_id") and not chunk.get("document_id"):
            raise ChunkSetValidationError(
                f"chunk {idx} must include either paper_id or document_id"
            )


def validate_chunk_set_dict(payload: dict[str, Any]) -> None:
    schema = _load_schema()

    try:
        import jsonschema  # type: ignore

        jsonschema.validate(instance=payload, schema=schema)
    except ImportError:
        _validate_without_jsonschema(payload)
    except Exception as exc:  # jsonschema.ValidationError and similar
        raise ChunkSetValidationError(str(exc)) from exc

    for idx, chunk in enumerate(payload.get("chunks", [])):
        if not chunk.get("paper_id") and not chunk.get("document_id"):
            raise ChunkSetValidationError(
                f"chunk {idx} must include either paper_id or document_id"
            )


def validate_chunk_set_file(path: str | Path) -> dict[str, Any]:
    payload = load_chunk_set(path)
    validate_chunk_set_dict(payload)
    return payload
