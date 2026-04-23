from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kb.config.kb_config import load_config
from kb.pipelines.chat_ingest import ingest_paths
from kb.storage.processed_files import ProcessedFiles


class ChatIngestSmokeTests(unittest.TestCase):
    def test_smoke_succeeds_without_provider_credentials_and_emits_contract_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            input_path = td_path / "2026-04-23.jsonl"
            input_path.write_text(
                json.dumps(
                    {
                        "role": "assistant",
                        "title": "smoke",
                        "timestamp": 1713830400000,
                        "content": "Line one\nLine two",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            original_env = {k: os.environ.get(k) for k in ["KB_ROOT", "KB_EMBED_PROVIDER", "JINAAI_API_KEY", "OPENAI_API_KEY"]}
            try:
                os.environ["KB_ROOT"] = str(td_path)
                os.environ["KB_EMBED_PROVIDER"] = "jina"
                os.environ.pop("JINAAI_API_KEY", None)
                os.environ.pop("OPENAI_API_KEY", None)
                cfg = load_config()

                with patch("kb.pipelines.chat_ingest._make_embed_fn", side_effect=AssertionError("embed provider should not be used in smoke")):
                    res = ingest_paths([input_path], cfg=cfg, smoke=True)

                self.assertEqual(res.run_record["status"], "success")
                self.assertEqual(res.run_record.get("entrypoint"), "kb_chat_ingest")
                self.assertIn("created_at", res.run_record)
                self.assertIn("completed_at", res.run_record)
                self.assertIn("smoke_artifact_path", res.run_record.get("outputs", {}))

                rr_path = res.run_record_path
                self.assertTrue(rr_path.exists(), "run record should exist")
                self.assertIn("manifest_path", res.run_record["outputs"])
                self.assertIn("observability_latest_path", res.run_record["outputs"])
                self.assertIn("chunk_set_artifact_path", res.run_record["outputs"])
                self.assertTrue(Path(res.run_record["outputs"]["manifest_path"]).exists(), "manifest should exist")
                self.assertTrue(Path(res.run_record["outputs"]["observability_latest_path"]).exists(), "observability latest should exist")

                chunk_set_path = Path(res.run_record["outputs"]["chunk_set_artifact_path"])
                self.assertTrue(chunk_set_path.exists(), "chunk set artifact should exist")
                chunk_set = json.loads(chunk_set_path.read_text(encoding="utf-8"))
                self.assertEqual(chunk_set["artifact_family"], "chunk_bus")
                self.assertEqual(chunk_set["artifact_kind"], "chunk_set")
                self.assertEqual(chunk_set["entrypoint"], "kb_chat_ingest")
                self.assertIn("source_items", chunk_set)
                self.assertIn("chunks", chunk_set)
                self.assertIn("chunk_count", chunk_set)

                manifest_path = Path(res.run_record["outputs"]["manifest_path"])
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                artifact_by_kind = {a["artifact_kind"]: a for a in manifest["artifacts"]}
                self.assertIn("chunk_set", artifact_by_kind)
                self.assertEqual(Path(artifact_by_kind["chunk_set"]["path"]), chunk_set_path)

                smoke_artifact_path = Path(res.run_record["outputs"]["smoke_artifact_path"])
                self.assertTrue(smoke_artifact_path.exists(), "smoke artifact should exist")
                smoke_artifact = json.loads(smoke_artifact_path.read_text(encoding="utf-8"))
                self.assertGreaterEqual(len(smoke_artifact.get("sample", [])), 1)

                pf = ProcessedFiles.open(cfg.cache_db)
                try:
                    self.assertEqual(pf.all_processed(), [], "smoke should not mark processed files")
                finally:
                    pf.close()

                self.assertEqual(int(res.run_record["counters"]["chroma_attempted"]), 0, "smoke should not write to chroma")
            finally:
                for key, value in original_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
