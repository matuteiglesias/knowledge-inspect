"""Read-only verification of the producer-owned evidence for one run."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from kb.config.kb_config import KBConfig


RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$")
OPERATOR_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$")

EXIT_CODES = {
    "verified": 0,
    "complete_legacy_unverified": 2,
    "partial": 3,
    "checksum_mismatch": 4,
    "invalid_structure": 5,
    "unsafe_reference": 5,
    "unknown_run": 6,
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("top-level JSON value is not an object")
    return value


def _reference(raw: object, artifacts_root: Path) -> Path:
    if not isinstance(raw, str) or not raw:
        raise ValueError("reference is not a non-empty string")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = artifacts_root / candidate
    resolved_root = artifacts_root.resolve()
    resolved = candidate.resolve()
    if not resolved.is_relative_to(resolved_root):
        raise PermissionError(f"reference escapes artifact root: {raw}")
    return resolved


def verify_run(cfg: KBConfig, run_id: str, *, operator: str | None = None) -> dict[str, Any]:
    """Assess one run without creating, changing, or discovering arbitrary files."""
    if not RUN_ID_RE.fullmatch(run_id):
        return _report(run_id, operator, ["unsafe_reference"], ["run_id is not a safe artifact identifier"])
    if operator is not None and not OPERATOR_RE.fullmatch(operator):
        return _report(run_id, operator, ["unsafe_reference"], ["operator is not a safe artifact identifier"])

    root = cfg.artifacts_dir.resolve()
    canonical_rr_path = root / "run_records" / f"{run_id}.run_record.json"
    characterized_rr_path = root / "run_records" / f"{run_id}.json"
    # Production seams use the first name; Task 2A's producer-generated fixtures
    # use the second. Checking these two exact names is bounded (not discovery).
    rr_path = canonical_rr_path if canonical_rr_path.is_file() else characterized_rr_path
    manifest_path = root / "manifests" / f"{run_id}.manifest.json"
    findings: set[str] = set()
    details: list[str] = []

    if not rr_path.is_file() and not manifest_path.is_file():
        return _report(run_id, operator, ["unknown_run"], ["no run record or bundle manifest exists"])

    documents: dict[str, dict[str, Any]] = {}
    for name, path in (("run record", rr_path), ("bundle manifest", manifest_path)):
        if not path.is_file():
            findings.update(("partial", "missing_member"))
            details.append(f"missing {name}: {path.relative_to(root)}")
            continue
        try:
            documents[name] = _load_json(path)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            findings.add("invalid_structure")
            details.append(f"invalid {name}: {exc}")

    record = documents.get("run record")
    manifest = documents.get("bundle manifest")
    if record is not None:
        if record.get("run_id") != run_id or not isinstance(record.get("operator"), str):
            findings.add("invalid_structure")
            details.append("run record has invalid run_id or operator")
        elif operator is not None and record["operator"] != operator:
            findings.add("invalid_structure")
            details.append("operator does not match the run record")
        else:
            operator = record["operator"]

    if manifest is not None:
        artifacts = manifest.get("artifacts")
        if manifest.get("run_id") != run_id or not isinstance(artifacts, list):
            findings.add("invalid_structure")
            details.append("bundle manifest has invalid run_id or artifacts")
        else:
            for index, member in enumerate(artifacts):
                if not isinstance(member, dict):
                    findings.add("invalid_structure")
                    details.append(f"manifest member {index} is not an object")
                    continue
                try:
                    path = _reference(member.get("path"), root)
                except PermissionError as exc:
                    findings.add("unsafe_reference")
                    details.append(str(exc))
                    continue
                except ValueError as exc:
                    findings.add("invalid_structure")
                    details.append(f"invalid manifest member {index}: {exc}")
                    continue
                if not path.is_file():
                    findings.update(("partial", "missing_member"))
                    details.append(f"missing member: {path.relative_to(root)}")
                    # Task 2A pinned this exact v2 finalizer signature: a missing
                    # run record with no hash is a known legacy write-order loss.
                    if member.get("artifact_kind") == "run_record" and "sha256" not in member:
                        findings.add("legacy_unverified")
                    continue
                expected = member.get("sha256")
                # The v2 bundle intentionally does not hash itself.
                if member.get("artifact_kind") == "manifest":
                    continue
                if expected is None:
                    findings.add("legacy_unverified")
                    details.append(f"member has no recorded checksum: {path.relative_to(root)}")
                elif not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
                    findings.add("invalid_structure")
                    details.append(f"member has invalid checksum: {path.relative_to(root)}")
                elif _sha256(path) != expected:
                    findings.add("checksum_mismatch")
                    details.append(f"checksum mismatch: {path.relative_to(root)}")

    if operator is not None and OPERATOR_RE.fullmatch(operator):
        latest_path = root / "observability" / f"{operator}.latest.json"
        if latest_path.is_file():
            try:
                latest = _load_json(latest_path)
                if latest.get("run_id") == run_id:
                    for key, expected_path in (("run_record_path", rr_path), ("manifest_path", manifest_path)):
                        try:
                            target = _reference(latest.get(key), root)
                        except PermissionError as exc:
                            findings.add("unsafe_reference")
                            details.append(str(exc))
                            continue
                        except ValueError as exc:
                            findings.add("invalid_structure")
                            details.append(f"invalid latest {key}: {exc}")
                            continue
                        if target != expected_path.resolve() or not target.is_file():
                            findings.update(("partial", "missing_member", "stale_latest"))
                            details.append(f"latest {key} is stale")
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
                findings.add("invalid_structure")
                details.append(f"invalid latest pointer: {exc}")

    if not findings:
        findings.add("verified")
    return _report(run_id, operator, sorted(findings), details)


def _report(run_id: str, operator: str | None, findings: list[str], details: list[str]) -> dict[str, Any]:
    finding_set = set(findings)
    if "unsafe_reference" in finding_set:
        status = "unsafe_reference"
    elif "invalid_structure" in finding_set:
        status = "invalid_structure"
    elif "checksum_mismatch" in finding_set:
        status = "checksum_mismatch"
    elif "partial" in finding_set:
        status = "partial"
    elif "legacy_unverified" in finding_set:
        status = "complete_legacy_unverified"
    elif "unknown_run" in finding_set:
        status = "unknown_run"
    else:
        status = "verified"
    return {
        "verifier_version": 1,
        "run_id": run_id,
        "operator": operator,
        "status": status,
        "findings": findings,
        "details": details,
        "exit_code": EXIT_CODES[status],
    }
