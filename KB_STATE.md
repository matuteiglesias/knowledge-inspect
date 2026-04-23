# KB State

## Phase
`closure/hardening`

## Current needs (contract language)
- Keep canonical seam commands singular and operator-facing.
- Preserve run record + manifest + observability emission invariants for every seam.
- Maintain separation between public artifact surface and internal storage/parser/vectorstore implementation details.

## Known blocker
- `kb_papers_grobid` depends on importable external `grobid_ingest.py` adapter; provider/runtime availability remains an external dependency.

## Definition of Done status
- Canonical module purpose documented.
- Three canonical seams with one command each documented.
- Cheap smoke vs real ingest semantics documented.
- Public artifact surface explicitly documented.
- Run records + observability + manifests emitted by all seams.
- Contract compliance tests added as non-drift gate.
