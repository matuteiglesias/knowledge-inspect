from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kb.config.kb_config import load_config
from kb.contracts.chunk_set import validate_chunk_set_file
from kb.pipelines.chat_analyze import analyze


FIXTURE = Path(__file__).parent / "fixtures" / "governed_smoke.chunk_set.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class W4GovernedSmokeTests(unittest.TestCase):
    def test_governed_fixture_is_valid_and_analysis_does_not_use_source_parsing_or_vectors(self) -> None:
        before = _sha256(FIXTURE)
        payload = validate_chunk_set_file(FIXTURE)
        self.assertEqual(payload["producer"], "fixture.source-owner")
        self.assertEqual(payload["chunk_count"], 1)

        with tempfile.TemporaryDirectory() as td:
            with patch.dict(os.environ, {"KB_ROOT": td}, clear=False):
                cfg = load_config()
                result = analyze(
                    cfg=cfg,
                    chunk_set_path=FIXTURE,
                    export_name="w4-smoke.md",
                )

        self.assertEqual(result.run_record["status"], "success")
        self.assertEqual(result.run_record["inputs"]["selection_mode"], "explicit_chunk_set")
        semantic = result.run_record["inputs"]["semantic_operation"]
        self.assertFalse(semantic["vector_store"]["used"])
        self.assertFalse(semantic["retrieval"]["used"])
        self.assertFalse(semantic["clustering"]["used"])
        self.assertEqual(semantic["ordering"]["mode"], "governed_chunk_set_order")
        self.assertEqual(
            result.run_record["outputs"]["result_membership"]["member_id_sample"],
            ["fixture-governed-smoke-chunk-1"],
        )
        self.assertEqual(_sha256(FIXTURE), before, "canonical smoke must not mutate governed input")


if __name__ == "__main__":
    unittest.main()
