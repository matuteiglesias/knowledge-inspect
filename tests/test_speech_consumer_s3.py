import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from kb.speech_inspect import build_index, jsonl, query_index, sha256_text


def write_projection(path: Path) -> dict:
    chunks = [
        {
            "chunk_uid": "speech-chunk:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "speech_uid": "speech:fixture:111111111111111111111111",
            "capture_id": "capture:11111111111111111111111111111111",
            "source_text_sha256": "1" * 64,
            "chunk_index": 0,
            "word_start": 0,
            "word_end": 6,
            "text": "Educación pública y producción industrial con escuelas.",
            "text_sha256": "",
            "source_id": "fixture-source",
            "actor_id": "fixture-actor",
            "source_url": "https://example.test/speech/1",
            "title": "Speech one",
            "published_date": "2026-08-30",
        },
        {
            "chunk_uid": "speech-chunk:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "speech_uid": "speech:fixture:222222222222222222222222",
            "capture_id": "capture:22222222222222222222222222222222",
            "source_text_sha256": "2" * 64,
            "chunk_index": 0,
            "word_start": 0,
            "word_end": 6,
            "text": "La deuda y la inflación aparecen en este fragmento.",
            "text_sha256": "",
            "source_id": "fixture-source",
            "actor_id": "fixture-actor",
            "source_url": "https://example.test/speech/2",
            "title": "Speech two",
            "published_date": "2026-08-30",
        },
        {
            "chunk_uid": "speech-chunk:cccccccccccccccccccccccccccccccc",
            "speech_uid": "speech:fixture:222222222222222222222222",
            "capture_id": "capture:22222222222222222222222222222222",
            "source_text_sha256": "2" * 64,
            "chunk_index": 1,
            "word_start": 5,
            "word_end": 12,
            "text": "Producción producción y desarrollo, sin interpretación política.",
            "text_sha256": "",
            "source_id": "fixture-source",
            "actor_id": "fixture-actor",
            "source_url": "https://example.test/speech/2",
            "title": "Speech two",
            "published_date": "2026-08-30",
        },
    ]
    for chunk in chunks:
        chunk["text_sha256"] = sha256_text(chunk["text"])
    payload = {
        "schema_name": "speech_chunk_set.s3.v1",
        "schema_status": "experimental",
        "producer": "matuteiglesias/politics-wiki",
        "projection_id": "speech-chunks:fixture0000000000000000",
        "source_release_id": "speech-release:fixture0000000000000000",
        "source_catalog_sha256": "a" * 64,
        "source_capture_inventory_sha256": "b" * 64,
        "speech_count": 2,
        "chunk_count": len(chunks),
        "chunk_inventory_sha256": sha256_text(jsonl(chunks)),
        "chunking": {
            "algorithm": "word_windows",
            "version": "speech_word_windows.s3.v1",
            "max_words": 180,
            "overlap_words": 30,
        },
        "chunks": chunks,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return payload


class SpeechConsumerS3Tests(unittest.TestCase):
    def test_index_query_preserves_producer_identity_and_does_not_mutate_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projection_path = root / "projection.json"
            payload = write_projection(projection_path)
            before = hashlib.sha256(projection_path.read_bytes()).hexdigest()

            manifest = build_index(projection_path, root / "indexes")
            result = query_index(manifest["index_dir"], "educacion produccion", top_k=2)
            after = hashlib.sha256(projection_path.read_bytes()).hexdigest()

            self.assertEqual(before, after)
            self.assertEqual(manifest["projection_id"], payload["projection_id"])
            self.assertEqual(manifest["source_release_id"], payload["source_release_id"])
            self.assertEqual(result["top_k"], 2)
            self.assertGreaterEqual(result["result_count"], 1)
            evidence = result["results"][0]["evidence"]
            self.assertEqual(evidence["speech_uid"], payload["chunks"][0]["speech_uid"])
            self.assertEqual(evidence["capture_id"], payload["chunks"][0]["capture_id"])
            self.assertEqual(evidence["chunk_uid"], payload["chunks"][0]["chunk_uid"])
            self.assertEqual(evidence["source_url"], payload["chunks"][0]["source_url"])

    def test_query_is_deterministic_and_accent_folding_is_consumer_semantics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projection_path = root / "projection.json"
            write_projection(projection_path)
            first_index = build_index(projection_path, root / "indexes")
            second_index = build_index(projection_path, root / "indexes")
            self.assertEqual(first_index["index_id"], second_index["index_id"])

            accented = query_index(first_index["index_dir"], "inflación", top_k=5)
            plain = query_index(first_index["index_dir"], "inflacion", top_k=5)
            self.assertEqual(accented["results"], plain["results"])
            self.assertEqual(accented["result_count"], 1)

    def test_term_frequency_is_the_only_ranking_signal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projection_path = root / "projection.json"
            payload = write_projection(projection_path)
            manifest = build_index(projection_path, root / "indexes")
            result = query_index(manifest["index_dir"], "produccion", top_k=2)

            self.assertEqual(result["results"][0]["score"], 2)
            self.assertEqual(
                result["results"][0]["evidence"]["chunk_uid"],
                payload["chunks"][2]["chunk_uid"],
            )

    def test_adapter_rejects_tampered_producer_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projection_path = root / "projection.json"
            payload = write_projection(projection_path)
            payload["chunks"][0]["text"] += " alterado"
            projection_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "chunk text hash mismatch"):
                build_index(projection_path, root / "indexes")

    def test_top_k_is_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projection_path = root / "projection.json"
            write_projection(projection_path)
            manifest = build_index(projection_path, root / "indexes")
            with self.assertRaisesRegex(ValueError, "top_k"):
                query_index(manifest["index_dir"], "deuda", top_k=0)
            with self.assertRaisesRegex(ValueError, "top_k"):
                query_index(manifest["index_dir"], "deuda", top_k=101)


if __name__ == "__main__":
    unittest.main()
