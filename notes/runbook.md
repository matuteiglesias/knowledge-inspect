# KB Pipelines Runbook v1

This runbook covers the **six integration seams** we just created:

- `pipelines/chat_ingest.py`
- `pipelines/chat_analyze.py`
- `pipelines/papers_grobid.py`
- `cli/kb_chat_ingest.py`
- `cli/kb_chat_analyze.py`
- `cli/kb_papers_grobid.py`

The goal is not elegance. The goal is **repeatable execution** and **artifacts you can inspect**.

## 0) Preconditions

### Python deps (minimum)
- `chromadb`
- `numpy`
- `scipy`
- `llama-index-core`
- One embedding provider:
  - Jina: `llama-index-embeddings-jinaai`
  - OpenAI: `llama-index-embeddings-openai`

### Required modules (already in your "core")
These pipelines expect the "core" modules you already generated:

- `kb/config/kb_config.py`
- `kb/storage/sqlite_cache.py`
- `kb/storage/processed_files.py`
- `kb/parsers/chat_jsonl.py`
- `kb/vectorstore/chroma_client.py`
- `kb/vectorstore/chroma_io.py`

## 1) Directory conventions

Inside your KB root (default `KB_ROOT=.`), we assume:

- `test_data/` or any folder of `*.jsonl` chat exports
- `store/` for vector DB files
- `embedding_cache.sqlite` for the embedding cache and processed_files state
- `artifacts/run_records/` for run records
- `artifacts/exports/` for analysis exports

All of those are configurable via env vars (next section).

## 2) Configuration by env vars

The config loader is in `kb/config/kb_config.py`.

### Paths
- `KB_ROOT` (default `.`)
- `KB_CHAT_JSONL_DIR` (default `${KB_ROOT}/test_data`)
- `KB_CACHE_DB` (default `${KB_ROOT}/embedding_cache.sqlite`)
- `KB_CHROMA_DIR` (default `${KB_ROOT}/store/chroma_jina_v3`)
- `KB_COLLECTION` (default `gpt_logs_jina_v3`)

### Embeddings
- `KB_EMBED_PROVIDER` (`jina` or `openai`; default `jina`)
- `KB_EMBED_MODEL`
  - default for jina: `jina-embeddings-v3`
  - default for openai: whatever you set, e.g. `text-embedding-3-small`
- `KB_EMBED_TASK` (only relevant for jina; default `retrieval.passage`)
- `KB_EMBED_DIM` (optional; if set, we assert the embedding length)

### Secrets
- For Jina:
  - `JINAAI_API_KEY` must be set
- For OpenAI:
  - `OPENAI_API_KEY` must be set

## 3) Pipeline 1: Chat ingest

### What it does
- Reads each JSONL file
- Builds a daily markdown document
- Parses markdown to nodes (llama_index MarkdownNodeParser)
- Filters out trivial nodes (single-line)
- Computes stable node ids (file + header_path + node_text hash)
- Embeds via cached SQLite vec cache
- Adds to Chroma (idempotent; skips existing IDs)
- Marks the input file as processed (processed_files table) after success
- Writes a `run_record.json`

### Run it
Option A: CLI wrapper
```bash
export KB_ROOT=~/Documents/KB
export KB_EMBED_PROVIDER=jina
export JINAAI_API_KEY=...   # required for jina

python -m kb.cli.kb_chat_ingest --glob "~/Documents/GPT_n/test_data/2025*.jsonl"
```

Option B: Python call
```python
from pathlib import Path
from kb.pipelines.chat_ingest import ingest_paths

paths = sorted(Path("~/Documents/GPT_n/test_data").expanduser().glob("2025*.jsonl"))
res = ingest_paths(paths)
print(res.run_record_path)
```

### Artifacts and evidence
- `artifacts/run_records/<run_id>.run_record.json`
- Chroma collection updated in `KB_CHROMA_DIR`

### Failure modes to watch
- Missing API key env var
- Wrong embed_dim (if `KB_EMBED_DIM` set)
- Chroma directory permission issues
- JSONL schema drift (handled by skipping malformed rows)

## 4) Pipeline 2: Chat analyze

### What it does
- Loads all embeddings + documents from Chroma
- Orders nodes using hierarchical clustering leaf order
- Writes `artifacts/exports/combined_notes.md`
- Writes a `run_record.json`

### Run it
```bash
export KB_ROOT=~/Documents/KB
python -m kb.cli.kb_chat_analyze --export-name combined_notes.md
```

Artifacts:
- `artifacts/exports/combined_notes.md`
- `artifacts/run_records/<run_id>.run_record.json`

## 5) Pipeline 3: Papers GROBID wrapper

### What it does
- Runs your existing `grobid_ingest.py` runner
- Adds a run_record.json around it

This is deliberately a wrapper: we are not normalizing paper chunks yet.

### Run it
```bash
export KB_ROOT=~/Documents/KB
python -m kb.cli.kb_papers_grobid ~/papers/foo.pdf --save-tei ./artifacts/exports/foo.tei.xml
```

Important:
- `grobid_ingest.py` must be importable (on `PYTHONPATH`) or vendored inside your project.

## 6) Run record contract (what to expect)

Every pipeline writes a JSON file under `artifacts/run_records/` with keys:

- `run_id` (unique, timestamped)
- `operator` (stable string)
- `started_at`, `finished_at` (UTC ISO)
- `status` (`ok` or `error`)
- `config` (snapshot of important config)
- `inputs` (paths and flags)
- `outputs` (paths written)
- `stats` (counts)
- `errors` (structured list, includes traceback on exceptions)

This is the **control surface**: you can build monitoring, dashboards, and VAC registry links off run_records.

## 7) Recommended usage pattern (today)

1) Ingest one day or small glob
2) Run analyze
3) Inspect:
   - export markdown
   - run_record stats
4) Only then decide if you need refinements (node filtering, header_path, chunking, etc.)

That keeps the loop artifact-driven and prevents drift into essay-land.
