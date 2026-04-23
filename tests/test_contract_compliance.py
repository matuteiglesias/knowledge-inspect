from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from kb.config.kb_config import load_config
from kb.pipelines.papers_grobid import run_pdf


ROOT = Path(__file__).resolve().parents[1]


class ContractComplianceTests(unittest.TestCase):
    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def test_canonical_entrypoints_note_contains_one_command_per_seam(self) -> None:
        note = (ROOT / "kb_entrypoints.md").read_text(encoding="utf-8")
        self.assertIn("python -m kb.cli.kb_chat_ingest", note)
        self.assertIn("python -m kb.cli.kb_chat_analyze", note)
        self.assertIn("python -m kb.cli.kb_papers_grobid", note)

    def test_health_contract_documents_smoke_vs_real_ingest(self) -> None:
        note = (ROOT / "kb_health_contract.md").read_text(encoding="utf-8")
        self.assertIn("Cheap smoke", note)
        self.assertIn("Real ingest", note)
        self.assertIn("--smoke", note)
        self.assertIn("--dry-run", note)

    def test_bus_role_decision_note_exists_and_declares_seam_roles(self) -> None:
        note = (ROOT / "kb_bus_role_decision.md").read_text(encoding="utf-8")
        self.assertIn("kb_chat_ingest", note)
        self.assertIn("consumer + producer", note)
        self.assertIn("chunk_set", note)
        self.assertIn("kb_chat_analyze", note)
        self.assertIn("chunk_set_summary", note)
        self.assertIn("kb_papers_grobid", note)
        self.assertIn("transitional", note)

    def test_pipeline_sources_use_shared_contract_helper(self) -> None:
        helper_src = (ROOT / "kb/pipelines/run_record_contract.py").read_text(encoding="utf-8")
        self.assertIn("finalize_and_write_contract_artifacts", helper_src)

        pipeline_files = [
            ROOT / "kb/pipelines/chat_ingest.py",
            ROOT / "kb/pipelines/chat_analyze.py",
            ROOT / "kb/pipelines/papers_grobid.py",
        ]
        for path in pipeline_files:
            src = path.read_text(encoding="utf-8")
            self.assertIn("make_run_record", src, msg=f"missing shared run-record builder in {path}")
            self.assertIn("finalize_and_write_contract_artifacts", src, msg=f"missing shared contract finalizer in {path}")

    def test_grobid_seam_emits_contract_schema_and_linkage_even_on_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            os.environ["KB_ROOT"] = td
            cfg = load_config()
            res = run_pdf(Path(td) / "missing.pdf", cfg=cfg, do_post_grobid=False)

            rr_path = res.run_record_path
            self.assertTrue(rr_path.exists(), "run record should exist")

            rr = json.loads(rr_path.read_text(encoding="utf-8"))
            self.assertIn(rr["status"], {"success", "empty_success", "partial_success", "error"})
            for field in [
                "run_record_version",
                "project",
                "entrypoint",
                "created_at",
                "completed_at",
                "stages",
                "schema_versions",
                "warnings",
                "environment",
                "counters",
                "inputs",
                "outputs",
                "errors",
            ]:
                self.assertIn(field, rr)

            self.assertIn("manifest_path", rr.get("outputs", {}))
            self.assertIn("observability_latest_path", rr.get("outputs", {}))

            manifest_path = Path(rr["outputs"]["manifest_path"])
            latest_path = Path(rr["outputs"]["observability_latest_path"])
            self.assertTrue(manifest_path.exists(), "manifest should exist")
            self.assertTrue(latest_path.exists(), "observability latest should exist")

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            latest = json.loads(latest_path.read_text(encoding="utf-8"))

            self.assertEqual(manifest["manifest_version"], rr["schema_versions"]["manifest"])
            self.assertEqual(manifest["run_id"], rr["run_id"])
            self.assertEqual(manifest["artifact_family"], "contract")
            self.assertEqual(manifest["artifact_kind"], "manifest")
            self.assertEqual(manifest["schema_version_emitted"], rr["schema_versions"]["manifest"])
            self.assertEqual(manifest["producer"], "kb")
            self.assertTrue(manifest["producer_version"])
            self.assertEqual(manifest["status"], rr["status"])
            self.assertIsInstance(manifest["artifacts"], list)
            self.assertGreaterEqual(len(manifest["artifacts"]), 3)

            artifact_by_kind = {a["artifact_kind"]: a for a in manifest["artifacts"]}
            for required_kind in ["run_record", "manifest", "observability_latest"]:
                self.assertIn(required_kind, artifact_by_kind)
                self.assertIn("path", artifact_by_kind[required_kind])
                self.assertIn("schema_version_emitted", artifact_by_kind[required_kind])
                self.assertIn("artifact_family", artifact_by_kind[required_kind])
                self.assertIn("promotion_status", artifact_by_kind[required_kind])

            self.assertEqual(Path(artifact_by_kind["run_record"]["path"]), rr_path)
            self.assertEqual(Path(artifact_by_kind["manifest"]["path"]), manifest_path)
            self.assertEqual(Path(artifact_by_kind["observability_latest"]["path"]), latest_path)
            self.assertIn("sha256", artifact_by_kind["run_record"])
            self.assertIn("sha256", artifact_by_kind["observability_latest"])
            self.assertEqual(artifact_by_kind["run_record"]["sha256"], self._sha256(rr_path))
            self.assertEqual(artifact_by_kind["observability_latest"]["sha256"], self._sha256(latest_path))

            self.assertEqual(latest["observability_version"], rr["schema_versions"]["observability"])
            self.assertEqual(latest["artifact_family"], "module_observability")
            self.assertEqual(latest["artifact_kind"], "module_latest")
            self.assertEqual(latest["scope"], "module_local")
            self.assertEqual(latest["run_id"], rr["run_id"])
            self.assertEqual(latest["entrypoint"], rr["entrypoint"])
            self.assertEqual(latest["status"], rr["status"])
            self.assertEqual(Path(latest["run_record_path"]), rr_path)
            self.assertEqual(Path(latest["manifest_path"]), manifest_path)
            self.assertEqual(latest["completed_at"], rr["completed_at"])


if __name__ == "__main__":
    unittest.main()
