# PR 2 / Task 2C — operator evidence and Human Gate 2 closure

**Scope:** operator-facing evidence for the existing read-only verifier. This task
does not change finalization, repair artifacts, modify historical evidence, or open
the Profile 2 migration.

## Reproducible request/response transcripts

Command: `make verify-run-evidence-demo`

```text
BEFORE_SHA256=bb8161806960e5362023f924f4c2676a95d176cc861ba5f16b4fa2582388e54f
REQUEST: KB_ROOT=tests/fixtures/run_evidence_demo python3 -m kb.cli.kb_verify_run demo_valid --operator kb.demo
RESPONSE: {"details": [], "exit_code": 0, "findings": ["verified"], "operator": "kb.demo", "run_id": "demo_valid", "status": "verified", "verifier_version": 1}
EXIT_CODE=0
REQUEST: KB_ROOT=tests/fixtures/run_evidence_demo python3 -m kb.cli.kb_verify_run demo_mismatch --operator kb.demo
RESPONSE: {"details": ["checksum mismatch: payloads/demo_mismatch.json"], "exit_code": 4, "findings": ["checksum_mismatch"], "operator": "kb.demo", "run_id": "demo_mismatch", "status": "checksum_mismatch", "verifier_version": 1}
EXIT_CODE=4
REQUEST: KB_ROOT=tests/fixtures/run_evidence_demo python3 -m kb.cli.kb_verify_run demo_legacy_write_failure --operator kb.demo_legacy
RESPONSE: {"details": ["missing run record: run_records/demo_legacy_write_failure.json", "missing member: run_records/demo_legacy_write_failure.json", "latest run_record_path is stale"], "exit_code": 3, "findings": ["legacy_unverified", "missing_member", "partial", "stale_latest"], "operator": "kb.demo_legacy", "run_id": "demo_legacy_write_failure", "status": "partial", "verifier_version": 1}
EXIT_CODE=3
AFTER_SHA256=bb8161806960e5362023f924f4c2676a95d176cc861ba5f16b4fa2582388e54f
NO_MUTATION=PASS
```

The mismatch is a proven comparison of existing bytes with a recorded digest. The
legacy case has no digest for its absent run record, so it remains a characterized
legacy-unverified partial condition and is not mislabeled as corruption. Equal
before/after fixture-tree hashes prove the verifier did not modify the evidence.

## Human Gate 2

| Confirmation | Closure evidence |
|---|---|
| WRITER SEMANTICS CHANGED: NO | no pipeline/finalizer files changed in PR 2 |
| RUN-RECORD SCHEMA CHANGED: NO | no schema or writer changes |
| BUNDLE SCHEMA CHANGED: NO | no schema or writer changes |
| LATEST MUTATED BY VERIFIER: NO | equal before/after hashes; read-only test |
| HISTORICAL FILES MODIFIED: NO | demonstrations use new synthetic fixtures only |
| PROFILE 2 CLAIMED: NO | verifier is explicitly limited to current producer-owned v2 evidence |
| ARBITRARY FILESYSTEM READ: NO | CLI accepts identifiers; references must remain under the artifact root |
| LEGACY LIMITATIONS DISTINGUISHED: YES | legacy transcript reports `legacy_unverified` without mismatch |
| PROVEN MISMATCHES DISTINGUISHED: YES | invalid transcript reports `checksum_mismatch` from compared bytes |
| TESTS: PASS | complete PR gate below |
| MANUAL VALID CASE: PASS | `demo_valid`, exit 0 |
| MANUAL INVALID CASE: PASS | `demo_mismatch`, expected exit 4 |

**Recommended decision after PR review and merge:** **ACCEPT AND MERGE**.

Any writer hardening remains a separately authorized future PR. It is not included
or implied here.

## Complete PR gate

- `python3 -m pytest -q`
- `make health`
- `make smoke`
- `make verify-run-evidence-demo`
- `git diff --check`
- clean `git status --short` after committing the bounded Task 2C additions
