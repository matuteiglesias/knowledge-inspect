# `kb/` Module Note

## Purpose (canonical)
`kb/` ingests document-like sources and emits inspectable knowledge artifacts through sanctioned seams.
It standardizes contractual run recording for every execution.
It provides one canonical operator command per seam.
It keeps storage + parsing internals private to avoid accidental contract coupling.
It publishes run-level evidence for health/debug before logs are required.

## Non-goals
- Not a generic workflow orchestrator.
- Not the owner of external provider reliability.
- Not a public API over parser/storage internals.

## Three canonical seams
1. `kb_chat_ingest`
2. `kb_chat_analyze`
3. `kb_papers_grobid`

## Seam bus roles
- `kb_chat_ingest`: consumer + producer. Canonical public output is `artifacts/chunk_sets/<run_id>.chunk_set.json` (Chunk Bus-compatible).
- `kb_chat_analyze`: consumer + producer. Canonical public output is `artifacts/summaries/<run_id>.summary.json` (Summary Bus-compatible), with markdown export as companion output.
- `kb_papers_grobid`: external parser adapter, transitional (not yet promoted to full bus-native producer).

## Contract highlights
- Shared run-record constructor/finalizer is used by all three seams.
- All seams emit the same top-level run-record shape.
- `counters` are canonical (legacy `stats` may exist for compatibility).
- Outputs are structured artifact records (with path/kind/family/schema/promotion status).
- Manifest now includes stronger identity + producer metadata (`manifest_version`, `artifact_family`, `artifact_kind`, `schema_version_emitted`, `producer`, `producer_version`), plus per-artifact hash fields when feasible.
- Observability latest is explicitly module-local observability, with direct links to run record + manifest.
- Contract records are emitted even when runs fail.

## Public contract vs internals
- **Public contract:** CLI entrypoints, run records, manifests, observability indexes, ingest chunk-set artifacts, analyze summary artifacts, and companion export artifact paths.
- **Internals:** parser classes/functions, sqlite cache layout, direct chroma helpers/storage layout, processed-files state, and per-module utility functions.

## Integration mapping
- `kb_chat_ingest`: document-like input -> ingest seam -> chunk-set artifact + run record + manifest + observability latest.
- `kb_chat_analyze`: chunk-set/collection-derived input -> summary artifact (+ companion markdown export) + run record + manifest + observability latest.
- `kb_papers_grobid`: provider adapter execution -> run record + manifest + observability latest (plus optional TEI/chroma side effects).
