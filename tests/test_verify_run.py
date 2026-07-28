from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kb.config.kb_config import load_config
from kb.contracts.run_evidence import verify_run
from kb.pipelines import run_record_contract as contract


class VerifyRunTests(unittest.TestCase):
    def _evidence(self, root: Path, *, payload: bool = True) -> tuple[object, Path, Path, Path, Path]:
        with patch.dict("os.environ", {"KB_ROOT": str(root)}, clear=False):
            cfg = load_config()
        cfg.ensure_dirs()
        run_id = "verify_fixture"
        payload_path = cfg.chunk_sets_dir / f"{run_id}.chunk_set.json"
        if payload:
            contract.write_json_atomic(payload_path, {"schema_version": 1, "chunks": []})
        record = contract.make_run_record(
            cfg=cfg,
            run_id=run_id,
            entrypoint="fixture",
            operator="kb.fixture",
            config={},
            inputs={},
            stage_defs=[{"name": "contract_artifact_emission"}],
            counters={"nodes_loaded": 0},
        )
        contract.add_output_artifact(record, path=payload_path, artifact_kind="chunk_set")
        paths = contract.finalize_and_write_contract_artifacts(
            cfg=cfg, run_record=record, rr_path=cfg.run_records_dir / f"{run_id}.json", requested_status="success"
        )
        return cfg, cfg.run_records_dir / f"{run_id}.json", Path(paths["manifest_path"]), Path(paths["observability_latest_path"]), payload_path

    def test_verified_current_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg, *_ = self._evidence(Path(td))
            report = verify_run(cfg, "verify_fixture", operator="kb.fixture")
            self.assertEqual(report["status"], "verified")
            self.assertEqual(report["findings"], ["verified"])
            self.assertEqual(report["exit_code"], 0)

    def test_missing_member_is_partial(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg, _, _, _, payload = self._evidence(Path(td), payload=False)
            report = verify_run(cfg, "verify_fixture")
            self.assertEqual(report["status"], "partial")
            self.assertTrue({"partial", "missing_member"}.issubset(report["findings"]))
            self.assertNotIn("legacy_unverified", report["findings"])
            self.assertEqual(report["exit_code"], 3)
            self.assertFalse(payload.exists())

    def test_checksum_mismatch_is_proven_not_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg, _, _, _, payload = self._evidence(Path(td))
            payload.write_text("changed", encoding="utf-8")
            report = verify_run(cfg, "verify_fixture")
            self.assertEqual(report["status"], "checksum_mismatch")
            self.assertIn("checksum_mismatch", report["findings"])
            self.assertNotIn("legacy_unverified", report["findings"])

    def test_malformed_manifest_and_unknown_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg, _, manifest, _, _ = self._evidence(Path(td))
            manifest.write_text("{broken", encoding="utf-8")
            self.assertEqual(verify_run(cfg, "verify_fixture")["status"], "invalid_structure")
            self.assertEqual(verify_run(cfg, "absent")["status"], "unknown_run")

    def test_stale_latest_is_reported_when_it_claims_this_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg, rr, _, _, _ = self._evidence(Path(td))
            rr.unlink()
            report = verify_run(cfg, "verify_fixture", operator="kb.fixture")
            self.assertEqual(report["status"], "partial")
            self.assertIn("stale_latest", report["findings"])

    def test_rejects_traversal_and_unsafe_manifest_reference(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg, _, manifest, _, _ = self._evidence(Path(td))
            self.assertEqual(verify_run(cfg, "../outside")["status"], "unsafe_reference")
            bundle = json.loads(manifest.read_text(encoding="utf-8"))
            bundle["artifacts"][0]["path"] = "/etc/passwd"
            manifest.write_text(json.dumps(bundle), encoding="utf-8")
            self.assertEqual(verify_run(cfg, "verify_fixture")["status"], "unsafe_reference")

    def test_verification_does_not_mutate_artifact_tree(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg, *_ = self._evidence(Path(td))
            before = {p.relative_to(cfg.artifacts_dir): p.read_bytes() for p in cfg.artifacts_dir.rglob("*") if p.is_file()}
            verify_run(cfg, "verify_fixture", operator="kb.fixture")
            after = {p.relative_to(cfg.artifacts_dir): p.read_bytes() for p in cfg.artifacts_dir.rglob("*") if p.is_file()}
            self.assertEqual(before, after)

    def test_cli_json_and_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self._evidence(Path(td))
            result = subprocess.run(
                [sys.executable, "-m", "kb.cli.kb_verify_run", "verify_fixture", "--operator", "kb.fixture"],
                text=True,
                capture_output=True,
                env={**__import__("os").environ, "KB_ROOT": td},
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["status"], "verified")


if __name__ == "__main__":
    unittest.main()
