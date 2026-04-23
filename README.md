# knowledge-inspect

This folder contains the core, reusable building blocks used by higher-level pipelines.

Included in this bundle:
- `config/kb_config.py`: environment + path configuration
- `storage/sqlite_cache.py`: embedding cache (vecs)
- `storage/processed_files.py`: processed-files state
- `parsers/chat_jsonl.py`: Chat export JSONL -> Markdown `Document` + node parsing helpers
- `vectorstore/chroma_client.py`: Chroma client + collection access
- `vectorstore/chroma_io.py`: batch add/get helpers for Chroma
- `pipelines/chat_ingest.py`: canonical ingest seam with run record + manifest + observability emission
- `pipelines/chat_analyze.py`: canonical analyze seam with run record + manifest + observability emission
- `pipelines/papers_grobid.py`: canonical papers seam wrapper with run record + manifest + observability emission
- `cli/kb_chat_ingest.py`, `cli/kb_chat_analyze.py`, `cli/kb_papers_grobid.py`: canonical operator entrypoints

Current contract scope:
- Each run emits run record + manifest + module-local observability latest.
- Manifest includes producer metadata, artifact identity fields, and explicit artifact linkage/checksums where feasible.
- Observability latest is module-local pointer/index, not a global observability aggregator.

Contract hardening notes and runbook:
- `docs/modules/kb-module-definition.md`
- `kb_module_note.md`
- `kb_entrypoints.md`
- `kb_artifact_surface.md`
- `kb_health_contract.md`
- `runbooks/kb_module_runbook.md`
