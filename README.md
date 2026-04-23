# kb (core modules only)

This folder contains the core, reusable building blocks used by higher-level pipelines.

Included in this bundle (v1):
- `config/kb_config.py`: environment + path configuration
- `storage/sqlite_cache.py`: embedding cache (vecs)
- `storage/processed_files.py`: processed-files state
- `parsers/chat_jsonl.py`: Chat export JSONL -> Markdown `Document` + node parsing helpers
- `vectorstore/chroma_client.py`: Chroma client + collection access
- `vectorstore/chroma_io.py`: batch add/get helpers for Chroma

Not included yet: pipelines and CLI wrappers.
# knowledge-inspect
