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

## Contract highlights
- Shared run-record constructor/finalizer is used by all three seams.
- All seams emit the same top-level run-record shape.
- `counters` are canonical (legacy `stats` may exist for compatibility).
- Outputs are structured artifact records (with path/kind/family/schema/promotion status).
- Contract records are emitted even when runs fail.

## Public contract vs internals
- **Public contract:** CLI entrypoints, run records, manifests, observability indexes, export artifact paths.
- **Internals:** parser classes/functions, sqlite cache layout, direct chroma helpers, per-module utility functions.

## Integration mapping
- `kb_chat_ingest`: document-like input -> ingest seam -> run record + manifest + observability latest.
- `kb_chat_analyze`: sanctioned collection read -> export artifact + run record + manifest + observability latest.
- `kb_papers_grobid`: provider adapter execution -> run record + manifest + observability latest (plus optional TEI/chroma side effects).
