from __future__ import annotations

import unittest

from kb.contracts.chunk_set import ChunkSetValidationError, validate_chunk_set_dict


class ChunkSetContractTests(unittest.TestCase):
    def _minimal_payload(self) -> dict:
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

    def test_valid_minimal_chunk_set_passes(self) -> None:
        validate_chunk_set_dict(self._minimal_payload())

    def test_missing_required_top_level_fields_fail(self) -> None:
        payload = self._minimal_payload()
        del payload["run_id"]
        with self.assertRaises(ChunkSetValidationError):
            validate_chunk_set_dict(payload)

    def test_missing_chunk_id_fails(self) -> None:
        payload = self._minimal_payload()
        del payload["chunks"][0]["chunk_id"]
        with self.assertRaises(ChunkSetValidationError):
            validate_chunk_set_dict(payload)

    def test_missing_text_fails(self) -> None:
        payload = self._minimal_payload()
        del payload["chunks"][0]["text"]
        with self.assertRaises(ChunkSetValidationError):
            validate_chunk_set_dict(payload)

    def test_missing_chunk_index_fails(self) -> None:
        payload = self._minimal_payload()
        del payload["chunks"][0]["chunk_index"]
        with self.assertRaises(ChunkSetValidationError):
            validate_chunk_set_dict(payload)

    def test_missing_paper_id_and_document_id_fails(self) -> None:
        payload = self._minimal_payload()
        del payload["chunks"][0]["paper_id"]
        with self.assertRaises(ChunkSetValidationError):
            validate_chunk_set_dict(payload)


if __name__ == "__main__":
    unittest.main()
