# KB State

## Phase
`closure/hardening`

## Current needs (contract language)
- Keep canonical seam commands singular and operator-facing.
- Preserve run record + manifest + observability emission invariants for every seam.
- Maintain one shared run-record contract helper so seams do not drift.
- Maintain separation between public artifact surface and internal storage/parser/vectorstore implementation details.
- Treat `kb_chat_ingest` canonical output as Chunk Bus-compatible chunk-set artifacts.
- Treat `kb_chat_analyze` canonical output as Summary Bus-compatible summary artifacts (markdown export remains companion).
- Keep Chroma/SQLite/processed-files state as internal operational side effects, not contract outputs.
- Keep `kb_papers_grobid` explicitly transitional adapter.

## Known blocker
- `kb_papers_grobid` depends on importable external `grobid_ingest.py` adapter; provider/runtime availability remains an external dependency.

## Definition of Done status
- Canonical module purpose documented.
- Three canonical seams with one command each documented.
- Cheap smoke vs real ingest semantics documented.
- Public artifact surface explicitly documented.
- Contractual run-record schema (v2) emitted by all seams.
- Final persisted statuses constrained to `success|empty_success|partial_success|error`.
- Failures still emit run records/manifests/observability indexes.
- Contract compliance tests added as non-drift gate.
