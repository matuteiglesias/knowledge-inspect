from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from kb.evidence_export import (
    ADAPTER_CONTRACT,
    EvidenceExportError,
    export_summary_bus,
)


FIXTURE = Path(__file__).parent / "fixtures" / "evidence_export" / "summary_bus.json"


class EvidenceExportTests(unittest.TestCase):
    def test_export_is_deterministic_safe_and_kb_artifacts_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first = root / "first.jsonl"
            second = root / "second.jsonl"

            receipt_one = export_summary_bus([FIXTURE], output=first)
            receipt_two = export_summary_bus([FIXTURE], output=second)

            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(receipt_one["contract"], ADAPTER_CONTRACT)
            self.assertEqual(receipt_one["output_sha256"], receipt_two["output_sha256"])

            record = json.loads(first.read_text(encoding="utf-8"))
            self.assertEqual(
                record["source_ref"],
                "knowledge-inspect:summary:kb_chat_analyze_20261007T120000Z",
            )
            self.assertIn("summary", record)
            self.assertEqual(record["meta"]["adapter_contract"], ADAPTER_CONTRACT)
            self.assertEqual(record["meta"]["input_artifacts"][0]["sha256"], "a" * 64)

            serialized = first.read_text(encoding="utf-8")
            self.assertNotIn("/private/local/path", serialized)
            self.assertNotIn("export_path", serialized)
            self.assertEqual(
                record["text_sha256"],
                hashlib.sha256(record["summary"].encode("utf-8")).hexdigest(),
            )

    def test_invalid_family_and_overwrite_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bad = root / "bad.json"
            payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
            payload["artifact_family"] = "not-summary-bus"
            bad.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(EvidenceExportError):
                export_summary_bus([bad], output=root / "out.jsonl")

            existing = root / "existing.jsonl"
            existing.write_text("keep\n", encoding="utf-8")
            with self.assertRaises(EvidenceExportError):
                export_summary_bus([FIXTURE], output=existing)
            self.assertEqual(existing.read_text(encoding="utf-8"), "keep\n")

    def test_cli_writes_receipt_and_one_jsonl_record(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / "evidence.jsonl"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "kb.cli.kb_evidence_export",
                    str(FIXTURE),
                    "--output",
                    str(output),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            receipt = json.loads(result.stdout)
            self.assertEqual(receipt["contract"], ADAPTER_CONTRACT)
            self.assertEqual(receipt["record_count"], 1)
            self.assertEqual(len(output.read_text(encoding="utf-8").splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
