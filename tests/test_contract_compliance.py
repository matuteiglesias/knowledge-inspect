from __future__ import annotations

import os
import json
import tempfile
import unittest
from pathlib import Path

from kb.config.kb_config import load_config
from kb.pipelines.papers_grobid import run_pdf


ROOT = Path(__file__).resolve().parents[1]


class ContractComplianceTests(unittest.TestCase):
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

    def test_pipeline_sources_include_manifest_and_observability_writes(self) -> None:
        pipeline_files = [
            ROOT / "kb/pipelines/chat_ingest.py",
            ROOT / "kb/pipelines/chat_analyze.py",
            ROOT / "kb/pipelines/papers_grobid.py",
        ]
        for path in pipeline_files:
            src = path.read_text(encoding="utf-8")
            self.assertIn("manifest_path", src, msg=f"missing manifest handling in {path}")
            self.assertIn("observability", src, msg=f"missing observability handling in {path}")
            self.assertIn("_write_contract_artifacts", src, msg=f"missing contract helper in {path}")

    def test_grobid_seam_emits_run_record_manifest_and_observability_even_on_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            os.environ["KB_ROOT"] = td
            cfg = load_config()
            res = run_pdf(Path(td) / "missing.pdf", cfg=cfg, do_post_grobid=False)

            rr_path = res.run_record_path
            self.assertTrue(rr_path.exists(), "run record should exist")

            rr = json.loads(rr_path.read_text(encoding="utf-8"))
            self.assertIn("manifest_path", rr.get("outputs", {}))
            self.assertIn("observability_latest_path", rr.get("outputs", {}))

            manifest_path = Path(rr["outputs"]["manifest_path"])
            latest_path = Path(rr["outputs"]["observability_latest_path"])
            self.assertTrue(manifest_path.exists(), "manifest should exist")
            self.assertTrue(latest_path.exists(), "observability latest should exist")

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["run_id"], rr["run_id"])
            self.assertEqual(latest["run_id"], rr["run_id"])


if __name__ == "__main__":
    unittest.main()
