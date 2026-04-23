from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from kb.config.kb_config import load_config
from kb.pipelines.chat_analyze import analyze


class ChatAnalyzeArtifactTests(unittest.TestCase):
    def test_analyze_emits_summary_bus_artifact_and_manifest_linkage(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            os.environ["KB_ROOT"] = str(td_path)
            cfg = load_config()
            cfg.ensure_dirs()

            chunk_set_path = cfg.chunk_sets_dir / "seed.chunk_set.json"
            chunk_set_path.write_text(
                json.dumps(
                    {
                        "artifact_family": "chunk_bus",
                        "artifact_kind": "chunk_set",
                        "schema_version": 1,
                        "run_id": "kb_chat_ingest_20260423T000000Z",
                        "producer": "kb",
                        "entrypoint": "kb_chat_ingest",
                        "source_items": ["input.jsonl"],
                        "chunks": [
                            {"chunk_id": "c1", "source_file": "input.jsonl", "header_path": ["h1"], "text": "alpha"},
                            {"chunk_id": "c2", "source_file": "input.jsonl", "header_path": ["h2"], "text": "beta"},
                        ],
                        "chunk_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            res = analyze(cfg=cfg)
            self.assertEqual(res.run_record["entrypoint"], "kb_chat_analyze")
            self.assertIn("summary_artifact_path", res.run_record["outputs"])
            self.assertIn("manifest_path", res.run_record["outputs"])

            summary_path = Path(res.run_record["outputs"]["summary_artifact_path"])
            self.assertTrue(summary_path.exists(), "summary artifact should exist")
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["artifact_family"], "summary_bus")
            self.assertEqual(summary["artifact_kind"], "chunk_set_summary")
            self.assertEqual(summary["entrypoint"], "kb_chat_analyze")
            self.assertIn("input_artifacts", summary)
            self.assertIn("summary_text", summary)
            self.assertIn("export_path", summary)

            manifest = json.loads(Path(res.run_record["outputs"]["manifest_path"]).read_text(encoding="utf-8"))
            artifact_by_kind = {a["artifact_kind"]: a for a in manifest["artifacts"]}
            self.assertIn("chunk_set_summary", artifact_by_kind)
            self.assertEqual(Path(artifact_by_kind["chunk_set_summary"]["path"]), summary_path)


if __name__ == "__main__":
    unittest.main()
