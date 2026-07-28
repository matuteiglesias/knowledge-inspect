from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from kb.cli.kb_chat_analyze import _parse_args
from kb.config.kb_config import load_config
from kb.pipelines.chat_analyze import _latest_chunk_set_path, analyze


def _write_chunk_set(path: Path, *, run_id: str, text: str = "payload") -> Path:
    path.write_text(
        json.dumps(
            {
                "artifact_family": "chunk_bus",
                "artifact_kind": "chunk_set",
                "schema_version": 1,
                "run_id": run_id,
                "producer": "kb",
                "entrypoint": "kb_chat_ingest",
                "source_items": ["fixture.jsonl"],
                "chunks": [
                    {
                        "chunk_id": f"{run_id}-chunk",
                        "chunk_index": 0,
                        "char_len": len(text),
                        "document_id": "fixture.jsonl",
                        "source_file": "fixture.jsonl",
                        "header_path": [run_id],
                        "text": text,
                        "metadata": {},
                    }
                ],
                "chunk_count": 1,
            }
        ),
        encoding="utf-8",
    )
    return path


class ChatAnalyzeInputDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.env = patch.dict(os.environ, {"KB_ROOT": self.temp_dir.name})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.cfg = load_config()
        self.cfg.ensure_dirs()

    def test_no_chunk_set_has_no_filesystem_selection(self) -> None:
        self.assertIsNone(_latest_chunk_set_path(self.cfg))

    def test_one_chunk_set_is_selected(self) -> None:
        only = _write_chunk_set(self.cfg.chunk_sets_dir / "only.chunk_set.json", run_id="only")
        self.assertEqual(_latest_chunk_set_path(self.cfg), only)

    def test_multiple_chunk_sets_select_greatest_mtime_not_name_or_run_id(self) -> None:
        semantically_newer = _write_chunk_set(
            self.cfg.chunk_sets_dir / "z-new-run.chunk_set.json", run_id="new-run"
        )
        touched_later = _write_chunk_set(
            self.cfg.chunk_sets_dir / "a-old-run.chunk_set.json", run_id="old-run"
        )
        os.utime(semantically_newer, (100, 100))
        os.utime(touched_later, (200, 200))

        self.assertEqual(_latest_chunk_set_path(self.cfg), touched_later)

    def test_equal_mtimes_have_no_documented_tie_breaker(self) -> None:
        first = _write_chunk_set(self.cfg.chunk_sets_dir / "first.chunk_set.json", run_id="first")
        second = _write_chunk_set(self.cfg.chunk_sets_dir / "second.chunk_set.json", run_id="second")
        os.utime(first, (100, 100))
        os.utime(second, (100, 100))

        # The stable sort preserves Path.glob's filesystem enumeration for a tie;
        # the implementation supplies no semantic or filename tie-breaker.
        self.assertIn(_latest_chunk_set_path(self.cfg), {first, second})

    def test_invalid_newest_chunk_set_fails_without_trying_older_file(self) -> None:
        older = _write_chunk_set(self.cfg.chunk_sets_dir / "older.chunk_set.json", run_id="older")
        newest = self.cfg.chunk_sets_dir / "newest.chunk_set.json"
        newest.write_text("{not-json", encoding="utf-8")
        os.utime(older, (100, 100))
        os.utime(newest, (200, 200))

        result = analyze(cfg=self.cfg)

        self.assertEqual(_latest_chunk_set_path(self.cfg), newest)
        self.assertEqual(result.run_record["status"], "error")
        self.assertEqual(result.run_record["inputs"]["selection_mode"], "legacy_mtime_chunk_set")
        self.assertEqual(result.run_record["counters"]["nodes_loaded"], 0)
        self.assertEqual(result.run_record["inputs"]["items"], [])
        self.assertFalse(result.export_path.exists())

    def test_copy_or_restore_timestamp_controls_selection(self) -> None:
        current = _write_chunk_set(self.cfg.chunk_sets_dir / "current.chunk_set.json", run_id="current")
        restored = _write_chunk_set(self.cfg.chunk_sets_dir / "restored.chunk_set.json", run_id="restored")
        os.utime(current, (200, 200))
        os.utime(restored, (100, 100))
        self.assertEqual(_latest_chunk_set_path(self.cfg), current)

        # A later copy/touch makes the restored payload authoritative even though
        # neither artifact metadata nor its run_id changed.
        os.utime(restored, (300, 300))
        self.assertEqual(_latest_chunk_set_path(self.cfg), restored)

    def test_no_chunk_set_falls_back_to_chroma_and_preserves_empty_outputs(self) -> None:
        calls: list[object] = []
        client_module = types.ModuleType("kb.vectorstore.chroma_client")
        io_module = types.ModuleType("kb.vectorstore.chroma_io")

        class FakeChromaConfig:
            def __init__(self, **kwargs):
                calls.append(("config", kwargs))

        def fake_get_collection(chroma_cfg, *, reset):
            calls.append(("get_collection", reset))
            return object(), object()

        def fake_load_vectors_and_min_nodes(collection, *, batch_size):
            calls.append(("load", batch_size))
            return object(), []

        client_module.ChromaConfig = FakeChromaConfig
        client_module.get_collection = fake_get_collection
        io_module.load_vectors_and_min_nodes = fake_load_vectors_and_min_nodes

        with patch.dict(
            sys.modules,
            {
                "kb.vectorstore.chroma_client": client_module,
                "kb.vectorstore.chroma_io": io_module,
            },
        ):
            result = analyze(cfg=self.cfg, batch_size=17)

        self.assertEqual(result.run_record["status"], "empty_success")
        self.assertEqual(result.run_record["inputs"]["selection_mode"], "chroma_fallback")
        self.assertEqual(
            result.run_record["inputs"]["items"],
            [{"input_kind": "collection", "collection": self.cfg.collection_name}],
        )
        self.assertIn(("get_collection", False), calls)
        self.assertIn(("load", 17), calls)
        self.assertEqual(result.export_path.read_text(encoding="utf-8"), "# combined_notes\n\n(no nodes in collection)\n")

    def test_explicit_chunk_set_overrides_newer_discovered_chunk_set(self) -> None:
        explicit = _write_chunk_set(
            self.cfg.chunk_sets_dir / "explicit.chunk_set.json", run_id="explicit", text="explicit text"
        )
        discovered = _write_chunk_set(
            self.cfg.chunk_sets_dir / "discovered.chunk_set.json", run_id="discovered", text="discovered text"
        )
        os.utime(explicit, (100, 100))
        os.utime(discovered, (200, 200))

        result = analyze(cfg=self.cfg, chunk_set_path=explicit)

        self.assertEqual(result.run_record["status"], "success")
        self.assertEqual(result.run_record["inputs"]["selection_mode"], "explicit_chunk_set")
        self.assertEqual(result.run_record["inputs"]["items"][0]["path"], str(explicit))
        export = result.export_path.read_text(encoding="utf-8")
        self.assertIn("explicit text", export)
        self.assertNotIn("discovered text", export)

    def test_invalid_explicit_chunk_set_fails_closed_without_legacy_fallback(self) -> None:
        discovered = _write_chunk_set(
            self.cfg.chunk_sets_dir / "discovered.chunk_set.json", run_id="discovered"
        )
        invalid = self.cfg.kb_root / "invalid.json"
        invalid.write_text(json.dumps({"chunks": []}), encoding="utf-8")
        os.utime(discovered, (200, 200))

        result = analyze(cfg=self.cfg, chunk_set_path=invalid)

        self.assertEqual(result.run_record["status"], "error")
        self.assertEqual(result.run_record["inputs"]["selection_mode"], "explicit_chunk_set")
        self.assertEqual(result.run_record["inputs"]["items"], [])
        self.assertEqual(result.run_record["errors"][0]["type"], "exception")
        self.assertIn("required", result.run_record["errors"][0]["message"])
        self.assertFalse(result.export_path.exists())

    def test_explicit_relative_and_absolute_paths_are_accepted(self) -> None:
        external = _write_chunk_set(
            self.cfg.kb_root / "external.chunk_set.json", run_id="external"
        )

        absolute_result = analyze(
            cfg=self.cfg, chunk_set_path=external, export_name="absolute.md"
        )
        previous_cwd = Path.cwd()
        try:
            os.chdir(self.cfg.kb_root)
            relative_result = analyze(
                cfg=self.cfg,
                chunk_set_path=Path("external.chunk_set.json"),
                export_name="relative.md",
            )
        finally:
            os.chdir(previous_cwd)

        self.assertEqual(absolute_result.run_record["status"], "success")
        self.assertEqual(relative_result.run_record["status"], "success")
        self.assertEqual(relative_result.run_record["inputs"]["items"][0]["path"], "external.chunk_set.json")

    def test_explicit_and_legacy_selection_have_equivalent_export_bytes(self) -> None:
        selected = _write_chunk_set(
            self.cfg.chunk_sets_dir / "selected.chunk_set.json", run_id="selected", text="same text"
        )

        with (
            patch("kb.pipelines.chat_analyze.utc_now_iso", return_value="2026-07-28T00:00:00Z"),
            patch("kb.pipelines.chat_analyze.make_run_id", return_value="fixed-analysis-run"),
        ):
            legacy = analyze(cfg=self.cfg, export_name="same.md")
            legacy_export_bytes = legacy.export_path.read_bytes()
            legacy_summary_bytes = Path(
                legacy.run_record["outputs"]["summary_artifact_path"]
            ).read_bytes()
            explicit = analyze(cfg=self.cfg, chunk_set_path=selected, export_name="same.md")

        self.assertEqual(legacy.run_record["inputs"]["selection_mode"], "legacy_mtime_chunk_set")
        self.assertEqual(explicit.run_record["inputs"]["selection_mode"], "explicit_chunk_set")
        self.assertEqual(legacy_export_bytes, explicit.export_path.read_bytes())
        self.assertEqual(
            legacy_summary_bytes,
            Path(explicit.run_record["outputs"]["summary_artifact_path"]).read_bytes(),
        )

    def test_cli_parser_accepts_chunk_set_without_changing_default(self) -> None:
        self.assertIsNone(_parse_args([]).chunk_set)
        self.assertEqual(
            _parse_args(["--chunk-set", "chosen.chunk_set.json"]).chunk_set,
            Path("chosen.chunk_set.json"),
        )


if __name__ == "__main__":
    unittest.main()
