from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kb.config.kb_config import load_config
from kb.pipelines import run_record_contract as contract


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = ROOT / "tests/fixtures/run_evidence_states.v1.json"


class RunEvidenceCharacterizationTests(unittest.TestCase):
    """Pin current v2 behavior without defining a production verifier."""

    def setUp(self) -> None:
        self.fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _new_evidence(
        self,
        root: Path,
        *,
        requested_status: str = "success",
        nodes_loaded: int = 1,
        errors: bool = False,
        payload_exists: bool = True,
    ) -> tuple[dict, Path, Path, Path, Path]:
        with patch.dict("os.environ", {"KB_ROOT": str(root)}, clear=False):
            cfg = load_config()
        cfg.ensure_dirs()
        run_id = f"fixture_{requested_status}_{nodes_loaded}_{int(errors)}"
        rr_path = cfg.run_records_dir / f"{run_id}.json"
        payload_path = cfg.chunk_sets_dir / f"{run_id}.chunk_set.json"
        if payload_exists:
            contract.write_json_atomic(payload_path, {"schema_version": 1, "chunks": [{"id": "one"}]})

        record = contract.make_run_record(
            cfg=cfg,
            run_id=run_id,
            entrypoint="fixture_entrypoint",
            operator="kb.fixture",
            config={},
            inputs={"fixture": True},
            stage_defs=[{"name": "contract_artifact_emission"}],
            counters={"nodes_loaded": nodes_loaded},
        )
        if errors:
            record["errors"].append({"type": "fixture_error", "message": "characterized failure"})
        contract.add_output_artifact(
            record,
            path=payload_path,
            artifact_kind="chunk_set",
            artifact_family="chunk_bus",
            schema_version=1,
        )
        paths = contract.finalize_and_write_contract_artifacts(
            cfg=cfg,
            run_record=record,
            rr_path=rr_path,
            requested_status=requested_status,
        )
        return record, rr_path, Path(paths["manifest_path"]), Path(paths["observability_latest_path"]), payload_path

    @staticmethod
    def _artifact(manifest_path: Path, kind: str) -> dict:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return next(item for item in manifest["artifacts"] if item["artifact_kind"] == kind)

    def test_fixture_names_all_required_states_and_distinctions(self) -> None:
        states = self.fixture["states"]
        required_cases = {
            "successful_run",
            "empty_success_run",
            "partial_success_run",
            "error_run",
            "missing_payload_member",
            "altered_payload_member",
            "latest_missing_run_record",
            "latest_missing_bundle",
            "current_run_record_checksum_ordering",
            "finalizer_run_record_write_failure",
        }
        distinctions = {
            "known_legacy_limitation",
            "incomplete_evidence",
            "structurally_invalid_evidence",
            "missing_member",
            "proven_checksum_mismatch",
            "stale_latest_pointer",
        }
        self.assertTrue(required_cases.issubset(states))
        self.assertTrue(distinctions.issubset({label for labels in states.values() for label in labels}))

    def test_final_status_matrix_is_fixture_pinned(self) -> None:
        cases = [
            ("successful_run", "success", 1, False, "success"),
            ("empty_success_run", "success", 0, False, "empty_success"),
            ("partial_success_run", "success", 1, True, "partial_success"),
            ("error_run", "error", 1, True, "error"),
        ]
        for fixture_name, requested, nodes, errors, expected in cases:
            with self.subTest(fixture=fixture_name), tempfile.TemporaryDirectory() as td:
                _, rr_path, manifest_path, latest_path, _ = self._new_evidence(
                    Path(td), requested_status=requested, nodes_loaded=nodes, errors=errors
                )
                rr = json.loads(rr_path.read_text(encoding="utf-8"))
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                latest = json.loads(latest_path.read_text(encoding="utf-8"))
                self.assertEqual(rr["status"], expected)
                self.assertEqual(manifest["status"], expected)
                self.assertEqual(latest["status"], expected)
                self.assertEqual(self.fixture["states"][fixture_name], ["complete_evidence"])

    def test_missing_payload_member_has_no_checksum_at_finalization(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, _, manifest_path, _, payload_path = self._new_evidence(Path(td), payload_exists=False)
            payload_entry = self._artifact(manifest_path, "chunk_set")
            self.assertFalse(payload_path.exists())
            self.assertNotIn("sha256", payload_entry)
            self.assertEqual(
                self.fixture["states"]["missing_payload_member"],
                ["incomplete_evidence", "missing_member"],
            )

    def test_altered_payload_member_proves_checksum_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, _, manifest_path, _, payload_path = self._new_evidence(Path(td))
            recorded = self._artifact(manifest_path, "chunk_set")["sha256"]
            contract.write_json_atomic(payload_path, {"schema_version": 1, "chunks": []})
            self.assertNotEqual(recorded, self._sha256(payload_path))
            self.assertEqual(
                self.fixture["states"]["altered_payload_member"],
                ["proven_checksum_mismatch"],
            )

    def test_latest_can_point_to_missing_run_record_or_bundle(self) -> None:
        for fixture_name, target_name in [
            ("latest_missing_run_record", "run_record_path"),
            ("latest_missing_bundle", "manifest_path"),
        ]:
            with self.subTest(fixture=fixture_name), tempfile.TemporaryDirectory() as td:
                _, _, _, latest_path, _ = self._new_evidence(Path(td))
                latest = json.loads(latest_path.read_text(encoding="utf-8"))
                target = Path(latest[target_name])
                target.unlink()
                self.assertFalse(target.exists())
                self.assertEqual(
                    self.fixture["states"][fixture_name],
                    ["incomplete_evidence", "missing_member", "stale_latest_pointer"],
                )

    def test_current_run_record_checksum_is_computed_after_run_record_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, rr_path, manifest_path, _, _ = self._new_evidence(Path(td))
            recorded = self._artifact(manifest_path, "run_record")["sha256"]
            self.assertEqual(recorded, self._sha256(rr_path))
            self.assertEqual(
                self.fixture["states"]["current_run_record_checksum_ordering"],
                ["complete_evidence"],
            )

    def test_run_record_write_failure_is_swallowed_and_publishes_stale_latest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with patch.dict("os.environ", {"KB_ROOT": str(root)}, clear=False):
                cfg = load_config()
            cfg.ensure_dirs()
            rr_path = cfg.run_records_dir / "write_failure.json"
            record = contract.make_run_record(
                cfg=cfg,
                run_id="write_failure",
                entrypoint="fixture_entrypoint",
                operator="kb.fixture",
                config={},
                inputs={},
                stage_defs=[{"name": "contract_artifact_emission"}],
                counters={"nodes_loaded": 1},
            )
            real_write = contract.write_json_atomic

            def fail_run_record(path: Path, obj: dict) -> None:
                if path == rr_path:
                    raise OSError("injected run-record write failure")
                real_write(path, obj)

            with patch.object(contract, "write_json_atomic", side_effect=fail_run_record):
                paths = contract.finalize_and_write_contract_artifacts(
                    cfg=cfg,
                    run_record=record,
                    rr_path=rr_path,
                    requested_status="success",
                )

            latest_path = Path(paths["observability_latest_path"])
            manifest_path = Path(paths["manifest_path"])
            self.assertFalse(rr_path.exists())
            self.assertTrue(latest_path.exists())
            self.assertTrue(manifest_path.exists())
            self.assertEqual(Path(json.loads(latest_path.read_text())["run_record_path"]), rr_path)
            self.assertNotIn("sha256", self._artifact(manifest_path, "run_record"))
            self.assertEqual(
                self.fixture["states"]["finalizer_run_record_write_failure"],
                ["known_legacy_limitation", "incomplete_evidence", "missing_member", "stale_latest_pointer"],
            )

    def test_malformed_bundle_is_structurally_invalid_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, _, manifest_path, _, _ = self._new_evidence(Path(td))
            manifest_path.write_text("{not-json", encoding="utf-8")
            with self.assertRaises(json.JSONDecodeError):
                json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                self.fixture["states"]["malformed_bundle_member"],
                ["structurally_invalid_evidence"],
            )


if __name__ == "__main__":
    unittest.main()
