from __future__ import annotations

from dataclasses import replace
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from kb.config.kb_config import load_config
from kb.pipelines.chat_ingest import ingest_paths
from kb.vectorstore.chroma_client import ChromaConfig, get_collection


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/smoke_chat.jsonl"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _deterministic_embedding(text: str) -> np.ndarray:
    """Small offline representation used only for the rebuildability proof."""
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    vec = np.asarray([digest[0] + 1, digest[1] + 1, digest[2] + 1], dtype=np.float32)
    norm = float(np.linalg.norm(vec))
    return vec / norm if norm else vec


class VectorStoreRebuildabilityTests(unittest.TestCase):
    def _collection(self, cfg, resolved_collection: str):
        chroma_cfg = ChromaConfig(
            chroma_dir=cfg.chroma_dir,
            collection_name=resolved_collection,
            allow_reset=True,
        )
        return get_collection(chroma_cfg, reset=False)

    def test_governed_fixture_survives_targeted_chroma_delete_and_rebuild(self) -> None:
        fixture_before = _sha256(FIXTURE)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with patch.dict(os.environ, {"KB_ROOT": str(root)}, clear=False):
                base_cfg = load_config()

            cfg = replace(
                base_cfg,
                cache_db=root / "state.sqlite",
                chroma_dir=root / "chroma",
                collection_name="w3-rebuild-proof",
                embed_provider="fixture",
                embed_model="offline-sha256-v1",
                embed_task="retrieval.passage",
                embed_dim=3,
            )

            with (
                patch(
                    "kb.pipelines.chat_ingest._make_embed_fn",
                    return_value=_deterministic_embedding,
                ),
                patch(
                    "kb.pipelines.chat_ingest.make_run_id",
                    side_effect=["w3-build-first", "w3-build-second"],
                ),
            ):
                first = ingest_paths([FIXTURE], cfg=cfg)

                self.assertEqual(first.run_record["status"], "success")
                self.assertEqual(first.run_record["counters"]["files_processed"], 1)
                self.assertEqual(first.run_record["counters"]["chroma_added"], 1)

                first_chunk_set_path = Path(
                    first.run_record["outputs"]["chunk_set_artifact_path"]
                )
                import json

                first_chunk_set = json.loads(first_chunk_set_path.read_text(encoding="utf-8"))
                first_ids = [chunk["chunk_id"] for chunk in first_chunk_set["chunks"]]
                self.assertTrue(first_ids)
                resolved = first.run_record["config"]["resolved_collection"]

                _, first_collection = self._collection(cfg, resolved)
                self.assertEqual(sorted(first_collection.get()["ids"]), sorted(first_ids))

                first_text = first_chunk_set["chunks"][0]["text"]
                query_vec = _deterministic_embedding(first_text).tolist()
                first_query = first_collection.query(
                    query_embeddings=[query_vec],
                    n_results=1,
                )
                self.assertEqual(first_query["ids"][0][0], first_ids[0])

                # Create unrelated derivative state in the same persistent Chroma
                # database. Resetting the target collection must not destroy it.
                unrelated_cfg = ChromaConfig(
                    chroma_dir=cfg.chroma_dir,
                    collection_name="w3-unrelated-proof",
                    allow_reset=True,
                )
                _, unrelated = get_collection(unrelated_cfg, reset=False)
                unrelated.add(
                    ids=["unrelated-id"],
                    embeddings=[[0.0, 0.0, 1.0]],
                    documents=["unrelated derivative"],
                )
                self.assertEqual(unrelated.get()["ids"], ["unrelated-id"])

                # Explicit reset is a rebuild request. It must bypass processed-file
                # state, delete only this representation collection, and repopulate
                # it from the governed source fixture.
                second = ingest_paths([FIXTURE], cfg=cfg, reset_collection=True)

            self.assertEqual(second.run_record["status"], "success")
            self.assertEqual(second.run_record["counters"]["files_processed"], 1)
            self.assertEqual(second.run_record["counters"]["files_skipped_processed"], 0)
            self.assertEqual(second.run_record["counters"]["chroma_added"], 1)
            self.assertEqual(second.run_record["config"]["resolved_collection"], resolved)
            self.assertEqual(
                second.run_record["config"]["embedding_representation_id"],
                first.run_record["config"]["embedding_representation_id"],
            )

            second_chunk_set = json.loads(
                Path(second.run_record["outputs"]["chunk_set_artifact_path"]).read_text(
                    encoding="utf-8"
                )
            )
            second_ids = [chunk["chunk_id"] for chunk in second_chunk_set["chunks"]]
            self.assertEqual(second_ids, first_ids)

            _, rebuilt_collection = self._collection(cfg, resolved)
            self.assertEqual(sorted(rebuilt_collection.get()["ids"]), sorted(first_ids))
            rebuilt_query = rebuilt_collection.query(
                query_embeddings=[query_vec],
                n_results=1,
            )
            self.assertEqual(rebuilt_query["ids"][0][0], first_query["ids"][0][0])

            _, unrelated_after = get_collection(unrelated_cfg, reset=False)
            self.assertEqual(unrelated_after.get()["ids"], ["unrelated-id"])

        self.assertEqual(_sha256(FIXTURE), fixture_before, "governed fixture mutated")


if __name__ == "__main__":
    unittest.main()
