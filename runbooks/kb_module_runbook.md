# KB Module Runbook

## Purpose
Operate and debug `kb/` via contractual artifacts, not internals.

## Canonical entrypoints
1. `python -m kb.cli.kb_chat_ingest --paths <...>`
2. `python -m kb.cli.kb_chat_analyze --export-name combined_notes.md`
3. `python -m kb.cli.kb_papers_grobid <paper.pdf>`

## Contractual run-record status set
Final persisted run statuses are only:
- `success`
- `empty_success`
- `partial_success`
- `error`

## Smoke and real runs
- Cheap smoke (canonical): `python -m kb.cli.kb_chat_ingest --paths <...> --smoke`
- Real ingest: same command without `--smoke`
- Dev/debug dry-run: `python -m kb.cli.kb_chat_ingest --paths <...> --dry-run`

## Outputs to inspect first
1. `artifacts/observability/<operator>.latest.json`
2. `artifacts/run_records/<run_id>.run_record.json`
3. `artifacts/manifests/<run_id>.manifest.json`
4. `artifacts/chunk_sets/<run_id>.chunk_set.json` (for ingest canonical output)
5. `artifacts/summaries/<run_id>.summary.json` (for analyze canonical output)
6. `artifacts/exports/*` (analyze companion markdown export)

## Stage model to verify
Per seam, `stages` must explicitly cover:
- `config_load`
- `input_resolution`
- `parse`
- `embed_persist` when applicable
- `export` when applicable
- `contract_artifact_emission`

## Debug order
1. latest observability index
2. run record status/errors/warnings/counters
3. manifest artifact inventory (`artifacts[]`, producer metadata, checksums)
4. only then inspect lower-level internals/logs

## Observability boundary
- `artifacts/observability/<operator>.latest.json` is **module-local latest status** for `kb`.
- It is not an ecosystem-wide aggregator.
- Use it to jump to canonical run artifacts (`run_record_path`, `manifest_path`) and verify `run_id` + `status` + `completed_at` linkage.

## Read-only run-evidence verification

Use the producer-owned diagnostic interface for a single known run:

```bash
python -m kb.cli.kb_verify_run <run_id> --operator <dotted.operator.identity>
```

The operator is optional when the run record exists, but supplying it also checks a
latest pointer that claims to represent that run. The command accepts identifiers,
not paths, and only follows manifest references contained by the configured
`KB_ROOT/artifacts` directory. It performs no recursive discovery, repair, provider
calls, Chroma access, GROBID access, or SQLite mutation.

JSON is the default output. `--format text` gives a human-readable summary. Statuses
and exit codes are:

| Status | Exit | Meaning |
|---|---:|---|
| `verified` | 0 | Every checksum-bearing member exists and matches. |
| `complete_legacy_unverified` | 2 | Evidence is complete, but at least one v2 member has no checksum. |
| `partial` | 3 | A member is missing; findings may also name `stale_latest` or the known legacy finalizer limitation. |
| `checksum_mismatch` | 4 | Existing bytes differ from a valid recorded SHA-256. |
| `invalid_structure` | 5 | Required JSON structure, identity, or checksum syntax is invalid. |
| `unsafe_reference` | 5 | An identifier or evidence reference escapes the bounded artifact surface. |
| `unknown_run` | 6 | Neither the direct run record nor direct bundle path exists. |

The `findings` array preserves distinctions such as `missing_member`,
`stale_latest`, and `legacy_unverified`; a missing unhashed run record is recognized
as the characterized legacy finalizer failure, not promoted to a checksum mismatch.
The verifier assesses current producer-owned v2 shapes only. It does not establish
Profile 2 compliance, authenticate evidence, inspect unlisted files, or prove why a
member is absent. Rollback is deletion of `kb/cli/kb_verify_run.py` and
`kb/contracts/run_evidence.py`; because verification is read-only, no artifact or
state rollback is required.

### Reproducible operator evidence

Run `make verify-run-evidence-demo` from the repository root. It invokes the real
CLI against the checked-in, artifact-root-relative synthetic evidence under
`tests/fixtures/run_evidence_demo`; asserts exits 0, 4, and 3; and compares a digest
of every fixture byte before and after all three requests. The cases demonstrate:

1. `demo_valid`: a verified bundle and payload;
2. `demo_mismatch`: present payload bytes that provably differ from the recorded
   SHA-256 (an expected failing request);
3. `demo_legacy_write_failure`: the Task 2A-characterized missing, unhashed run
   record with a published bundle/latest pointer (an expected partial result with
   `legacy_unverified`, `missing_member`, and `stale_latest` findings).

The target fails if any exit differs from its documented value or if any fixture
byte changes. These fixtures are synthetic and contain no historical run evidence.
