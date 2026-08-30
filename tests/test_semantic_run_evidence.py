from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from kb.config.kb_config import load_config
from kb.embedding.representation import EmbeddingRepresentation
from kb.pipelines.chat_analyze import analyze
from kb.pipelines.chat_ingest import ingest_paths
from kb.vectorstore.chroma_io import load_vectors_and_min_nodes


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/smoke_chat.jsonl"


def _write_chunk_set(path: Path) -> Path:
    payload = {
        "artifact_family": "chunk_bus",
        "artifact_kind": "chunk_set",
        "schema_version": 1,
        "run_id": "fixture-run",
        "producer": "fixture",
        "entrypoint": "fixture",
        "source_items": ["fixture.jsonl"],
        "chunks": [
            {
                "chunk_id": "fixture-chunk-1",
                "chunk_index": 0,
                "char_len": 12,
                "document_id": "fixture.jsonl",
                "source_file": "fixture.jsonl",
                "header_path": ["fixture"],
                "text": "fixture text",
                "metadata": {},
            }
        ],
        "chunk_count": 1,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _offline_embedding(text: str) -> np.ndarray:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    vec = np.asarray([digest[0] + 1, digest[1] + 1, digest[2] + 1], dtype=np.float32)
    norm = float(np.linalg.norm(vec))
    return vec / norm if norm else vec


class _FakeCollection:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, *, limit, offset, include):
        self.calls += 1
        if self.calls == 1:
            return {
                "ids": ["logical-id-1"],
                "documents": ["doc"],
                "embeddings": [[1.0, 0.0, 0.0]],
                "metadatas": [{"header_path": "fixture"}],
            }
        return {"ids": [], "documents": [], "embeddings": [], "metadatas": []}


class SemanticRunEvidenceTests(unittest.TestCase):
    def test_chroma_loader_preserves_logical_ids(self) -> None:
        vecs, nodes = load_vectors_and_min_nodes(_FakeCollection(), batch_size=10)
        self.assertEqual(tuple(vecs.shape), (1, 3))
        self.assertEqual(nodes[0].node_id, "logical-id-1")
        self.assertEqual(nodes[0].text, "doc")

    def test_governed_chunk_set_truthfully_records_no_vector_or_clustering_use(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with patch.dict(os.environ, {"KB_ROOT": str(root)}, clear=False):
                cfg = load_config()
            cfg.ensure_dirs()
            chunk_set = _write_chunk_set(cfg.chunk_sets_dir / "fixture.chunk_set.json")

            result = analyze(cfg=cfg, chunk_set_path=chunk_set)

            self.assertEqual(result.run_record["status"], "success")
            semantic = result.run_record["config"]["semantic_runtime"]
            self.assertFalse(semantic["used"])
            self.assertFalse(semantic["retrieval"]["used"])
            self.assertIn("no public query", semantic["retrieval"]["reason"])

            artifact_evidence = result.run_record["inputs"]["artifact_evidence"]
            self.assertEqual(artifact_evidence["member_count"], 1)
            self.assertTrue(artifact_evidence["identity_complete"])
            self.assertEqual(artifact_evidence["member_id_sample"], ["fixture-chunk-1"])
            self.assertEqual(len(artifact_evidence["sha256"]), 64)

            side_effects = result.run_record["outputs"]["internal_side_effects"]
            self.assertFalse(side_effects["vector_store_used"])
            self.assertEqual(side_effects["clustering_ordering"], "not_used")
            self.assertEqual(side_effects["ordering_mode"], "chunk_set_order")
            self.assertEqual(
                result.run_record["outputs"]["result_identity"]["member_id_sample"],
                ["fixture-chunk-1"],
            )

    def test_chroma_fallback_uses_resolved_representation_and_emits_membership_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with patch.dict(os.environ, {"KB_ROOT": str(root)}, clear=False):
                base_cfg = load_config()
            cfg = replace(
                base_cfg,
                cache_db=root / "state.sqlite",
                chroma_dir=root / "chroma",
                collection_name="w3-analysis-proof",
                embed_provider="fixture",
                embed_model="offline-sha256-v1",
                embed_task="retrieval.passage",
                embed_dim=3,
            )
            representation = EmbeddingRepresentation.from_config(cfg)

            with (
                patch(
                    "kb.pipelines.chat_ingest._make_embed_fn",
                    return_value=_offline_embedding,
                ),
                patch("kb.pipelines.chat_ingest.make_run_id", return_value="w3-analyze-ingest"),
            ):
                ingested = ingest_paths([FIXTURE], cfg=cfg)

            self.assertEqual(ingested.run_record["status"], "success")
            chunk_set_path = Path(ingested.run_record["outputs"]["chunk_set_artifact_path"])
            chunk_set = json.loads(chunk_set_path.read_text(encoding="utf-8"))
            chunk_id = chunk_set["chunks"][0]["chunk_id"]

            # Force the characterized legacy fallback while leaving the governed
            # source fixture and Chroma derivative untouched.
            chunk_set_path.unlink()

            with patch("kb.pipelines.chat_analyze.make_run_id", return_value="w3-analyze-fallback"):
                result = analyze(cfg=cfg)

            self.assertEqual(result.run_record["status"], "success")
            self.assertEqual(result.run_record["inputs"]["selection_mode"], "chroma_fallback")
            self.assertEqual(
                result.run_record["inputs"]["items"],
                [{"input_kind": "collection", "collection": cfg.collection_name}],
            )

            semantic = result.run_record["config"]["semantic_runtime"]
            self.assertTrue(semantic["used"])
            self.assertFalse(semantic["retrieval"]["used"])
            self.assertIn("no query/top_k/cutoff/reranking/filter", semantic["retrieval"]["reason"])
            self.assertEqual(
                semantic["resolved_collection"],
                representation.collection_name(cfg.collection_name),
            )
            self.assertEqual(
                semantic["embedding_representation"]["representation_id"],
                representation.representation_id,
            )

            evidence = result.run_record["inputs"]["semantic_evidence"]
            self.assertEqual(evidence["embedding_representation_id"], representation.representation_id)
            self.assertEqual(evidence["member_count"], 1)
            self.assertTrue(evidence["identity_complete"])
            self.assertEqual(evidence["member_id_sample"], [chunk_id])
            self.assertEqual(len(evidence["membership_sha256"]), 64)

            result_identity = result.run_record["outputs"]["result_identity"]
            self.assertEqual(result_identity["member_id_sample"], [chunk_id])
            self.assertEqual(result_identity["ordering_mode"], "single_member_identity_order")
            self.assertEqual(len(result_identity["membership_sha256"]), 64)

            side_effects = result.run_record["outputs"]["internal_side_effects"]
            self.assertTrue(side_effects["vector_store_used"])
            self.assertEqual(side_effects["embedding_representation_id"], representation.representation_id)
            self.assertEqual(side_effects["clustering_ordering"], "not_used")
            self.assertIn("Provider-free smoke input", result.export_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
