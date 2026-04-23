# KB Module Runbook

## Purpose
Operate and debug `kb/` via contracts, not internals.

## Canonical entrypoints
1. `python -m kb.cli.kb_chat_ingest --paths <...>`
2. `python -m kb.cli.kb_chat_analyze --export-name combined_notes.md`
3. `python -m kb.cli.kb_papers_grobid <paper.pdf>`

## Smoke and real runs
- Cheap smoke: `python -m kb.cli.kb_chat_ingest --paths <...> --dry-run`
- Real ingest: same command without `--dry-run`

## Outputs to inspect first
1. `artifacts/observability/<operator>.latest.json`
2. `artifacts/run_records/<run_id>.run_record.json`
3. `artifacts/manifests/<run_id>.manifest.json`
4. `artifacts/exports/*` (for analyze)

## Failure modes
- Missing input paths.
- Missing provider credentials.
- External provider/tooling failures (e.g., GROBID adapter/import).

## Debug order
1. latest observability index
2. run record status/errors
3. manifest outputs
4. only then inspect lower-level internals/logs
