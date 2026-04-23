# KB Module Definition (`kb/`)

## Canonical purpose
`kb/` is the contract-facing ingestion + analysis module for chat and paper-derived knowledge artifacts. It owns three sanctioned seams (`kb_chat_ingest`, `kb_chat_analyze`, `kb_papers_grobid`) and emits inspectable artifacts (run records, manifests, observability indexes, exports) under `artifacts/`.

## Ecosystem role
- **Producer** on artifact surfaces: run records, manifests, observability latest indexes, analysis export artifacts.
- **Consumer** of document-like inputs for ingest/analyze flows.
- **Mixed role** for papers seam: wraps provider-facing GROBID execution but still emits contract artifacts.

## Governance / canonical boundaries
- Consumers MUST rely on run records/manifests/observability indexes and documented commands, not private package internals.
- Public IO is limited to documented CLI seams and emitted artifacts.
- Internal helper modules (`parsers`, `storage`, `vectorstore`) are implementation details and non-canonical for cross-repo coupling.

## ADR status
No new ADR is required in this hardening pass; this change formalizes existing seams, adds explicit artifact contracts, and introduces compliance tests to gate drift.
