from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from kb.cli.kb_validate_chunk_set import main


class ChunkSetValidateCliTests(unittest.TestCase):
    def _valid_payload(self) -> dict:
        return {
            "artifact_family": "chunk_bus",
            "artifact_kind": "chunk_set",
            "schema_version": 1,
            "run_id": "run-1",
            "producer": "kb",
            "entrypoint": "kb.cli.kb_papers_grobid",
            "source_items": ["paper.pdf"],
            "chunk_count": 1,
            "chunks": [
                {
                    "chunk_id": "c1",
                    "text": "sample",
                    "chunk_index": 0,
                    "char_len": 6,
                    "metadata": {},
                    "paper_id": "p1",
                }
            ],
        }

    def test_cli_returns_zero_for_valid_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "ok.chunk_set.json"
            path.write_text(json.dumps(self._valid_payload()), encoding="utf-8")
            rc = main([str(path)])
            self.assertEqual(0, rc)

    def test_cli_returns_one_for_invalid_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bad.chunk_set.json"
            payload = self._valid_payload()
            del payload["chunks"][0]["chunk_id"]
            path.write_text(json.dumps(payload), encoding="utf-8")
            rc = main([str(path)])
            self.assertEqual(1, rc)

    def test_cli_returns_one_if_any_file_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ok = Path(td) / "ok.chunk_set.json"
            bad = Path(td) / "bad.chunk_set.json"
            ok.write_text(json.dumps(self._valid_payload()), encoding="utf-8")

            payload = self._valid_payload()
            del payload["chunks"][0]["text"]
            bad.write_text(json.dumps(payload), encoding="utf-8")

            rc = main([str(ok), str(bad)])
            self.assertEqual(1, rc)


if __name__ == "__main__":
    unittest.main()
