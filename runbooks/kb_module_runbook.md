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
