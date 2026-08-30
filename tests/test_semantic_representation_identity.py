from __future__ import annotations

from dataclasses import replace
import json
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from kb.config.kb_config import load_config
from kb.embedding.representation import EmbeddingRepresentation
from kb.pipelines.chat_ingest import ingest_paths
from kb.storage.sqlite_cache import SQLiteVecCache


class SemanticRepresentationIdentityTests(unittest.TestCase):
    def test_same_dimension_different_models_have_distinct_representation_identity(self) -> None:
        rep_a = EmbeddingRepresentation("fixture", "model-a", "passage", 3)
        rep_b = EmbeddingRepresentation("fixture", "model-b", "passage", 3)
        rep_a_again = EmbeddingRepresentation("fixture", "model-a", "passage", 3)

        self.assertEqual(rep_a.representation_id, rep_a_again.representation_id)
        self.assertNotEqual(rep_a.representation_id, rep_b.representation_id)
        self.assertNotEqual(rep_a.collection_name("knowledge"), rep_b.collection_name("knowledge"))

    def test_cache_reuses_same_representation_but_not_same_dimensional_other_model(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache = SQLiteVecCache.open(Path(td) / "cache.sqlite")
            calls = {"a": 0, "b": 0}
            rep_a = EmbeddingRepresentation("fixture", "model-a", "passage", 3)
            rep_b = EmbeddingRepresentation("fixture", "model-b", "passage", 3)

            def embed_a(text: str) -> np.ndarray:
                calls["a"] += 1
                return np.asarray([1.0, 1.0, 1.0], dtype=np.float32)

            def embed_b(text: str) -> np.ndarray:
                calls["b"] += 1
                return np.asarray([2.0, 2.0, 2.0], dtype=np.float32)

            cached_a = cache.cached_embedder(
                embed_a, expected_dim=3, representation_id=rep_a.representation_id
            )
            cached_b = cache.cached_embedder(
                embed_b, expected_dim=3, representation_id=rep_b.representation_id
            )

            first_a = cached_a("chunk-1", "same text")
            second_a = cached_a("chunk-1", "same text")
            first_b = cached_b("chunk-1", "same text")

            np.testing.assert_array_equal(first_a, second_a)
            self.assertFalse(np.array_equal(first_a, first_b))
            self.assertEqual(calls, {"a": 1, "b": 1})

            rows = cache.con.execute("SELECT id FROM vecs ORDER BY id").fetchall()
            row_ids = [row[0] for row in rows]
            self.assertEqual(len(row_ids), 2)
            self.assertTrue(any(rep_a.representation_id in row for row in row_ids))
            self.assertTrue(any(rep_b.representation_id in row for row in row_ids))
            cache.close()

    def test_chat_ingest_keeps_chunk_identity_but_reprocesses_new_representation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            input_path = root / "fixture.jsonl"
            input_path.write_text(
                json.dumps(
                    {
                        "role": "assistant",
                        "title": "identity",
                        "timestamp": 1713830400000,
                        "content": "Line one\nLine two",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.dict("os.environ", {"KB_ROOT": str(root)}, clear=False):
                base_cfg = load_config()
            cfg_a = replace(
                base_cfg,
                embed_provider="fixture",
                embed_model="model-a",
                embed_task="passage",
                embed_dim=3,
            )
            cfg_b = replace(cfg_a, embed_model="model-b")
            rep_a = EmbeddingRepresentation.from_config(cfg_a)
            rep_b = EmbeddingRepresentation.from_config(cfg_b)

            calls: list[tuple] = []
            client_module = types.ModuleType("kb.vectorstore.chroma_client")
            io_module = types.ModuleType("kb.vectorstore.chroma_io")

            class FakeChromaConfig:
                def __init__(self, **kwargs):
                    self.collection_name = kwargs["collection_name"]
                    calls.append(("config", kwargs))

            def fake_get_collection(chroma_cfg, *, reset, metadata=None):
                calls.append(("get_collection", chroma_cfg.collection_name, reset, metadata))
                return object(), object()

            def fake_add_nodes(coll, *, ids, embeddings, documents, metadatas, idempotent):
                calls.append(("add", tuple(ids), tuple(tuple(float(x) for x in e) for e in embeddings)))
                return SimpleNamespace(
                    attempted=len(ids), added=len(ids), skipped_existing=0, errors=0
                )

            client_module.ChromaConfig = FakeChromaConfig
            client_module.get_collection = fake_get_collection
            io_module.add_nodes = fake_add_nodes

            embed_calls: list[str] = []

            def fake_make_embed_fn(cfg):
                value = 1.0 if cfg.embed_model == "model-a" else 2.0

                def embed(text: str) -> np.ndarray:
                    embed_calls.append(cfg.embed_model)
                    return np.asarray([value, value, value], dtype=np.float32)

                return embed

            with (
                patch.dict(
                    sys.modules,
                    {
                        "kb.vectorstore.chroma_client": client_module,
                        "kb.vectorstore.chroma_io": io_module,
                    },
                ),
                patch("kb.pipelines.chat_ingest._make_embed_fn", side_effect=fake_make_embed_fn),
                patch(
                    "kb.pipelines.chat_ingest.make_run_id",
                    side_effect=["rep-a-first", "rep-a-second", "rep-b-first"],
                ),
            ):
                first_a = ingest_paths([input_path], cfg=cfg_a)
                second_a = ingest_paths([input_path], cfg=cfg_a)
                first_b = ingest_paths([input_path], cfg=cfg_b)

            self.assertEqual(first_a.run_record["counters"]["files_processed"], 1)
            self.assertEqual(second_a.run_record["counters"]["files_skipped_processed"], 1)
            self.assertEqual(first_b.run_record["counters"]["files_processed"], 1)
            self.assertEqual(embed_calls, ["model-a", "model-b"])

            chunk_a = json.loads(
                Path(first_a.run_record["outputs"]["chunk_set_artifact_path"]).read_text()
            )["chunks"][0]
            chunk_b = json.loads(
                Path(first_b.run_record["outputs"]["chunk_set_artifact_path"]).read_text()
            )["chunks"][0]
            self.assertEqual(chunk_a["chunk_id"], chunk_b["chunk_id"])

            self.assertEqual(
                first_a.run_record["config"]["embedding_representation_id"],
                rep_a.representation_id,
            )
            self.assertEqual(
                first_b.run_record["config"]["embedding_representation_id"],
                rep_b.representation_id,
            )
            self.assertNotEqual(
                first_a.run_record["config"]["resolved_collection"],
                first_b.run_record["config"]["resolved_collection"],
            )

            add_calls = [call for call in calls if call[0] == "add"]
            self.assertEqual(len(add_calls), 2)
            self.assertEqual(add_calls[0][1], add_calls[1][1])
            self.assertNotEqual(add_calls[0][2], add_calls[1][2])

            with sqlite3.connect(str(base_cfg.cache_db)) as con:
                vec_ids = [row[0] for row in con.execute("SELECT id FROM vecs ORDER BY id")]
                processed = [
                    row[0]
                    for row in con.execute("SELECT fname FROM processed_files ORDER BY fname")
                ]
            self.assertEqual(len(vec_ids), 2)
            self.assertEqual(len(processed), 2)
            self.assertTrue(any(rep_a.representation_id in value for value in vec_ids))
            self.assertTrue(any(rep_b.representation_id in value for value in vec_ids))
            self.assertTrue(any(rep_a.representation_id in value for value in processed))
            self.assertTrue(any(rep_b.representation_id in value for value in processed))


if __name__ == "__main__":
    unittest.main()
