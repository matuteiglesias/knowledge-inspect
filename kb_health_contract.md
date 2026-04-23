# KB Health Contract

## Run record invariant
Every canonical seam run emits:
- `run_id`
- `operator`
- `started_at` / `finished_at`
- `status` (`ok` or `error`)
- `inputs`, `outputs`, `stats`, `errors`

## Cheap smoke vs real ingest
- **Cheap smoke (canonical):** `kb_chat_ingest --smoke` (provider-independent contract smoke).
  - Must load config, resolve input paths, import parser/pipeline modules, parse minimal JSONL, emit run record + manifest + observability, and write a tiny local smoke artifact.
  - Must **not** call embedding providers, require API keys, write embeddings, write to Chroma, or mark processed files.
- **Real ingest:** `kb_chat_ingest` without `--smoke`, producing actual persistence side effects (provider, embeddings, Chroma writes, processed-file marks).
- **Optional dev mode:** `kb_chat_ingest --dry-run` remains parse+embed without persistence and is not the canonical cheap smoke.

## Failure semantics
- Provider/external failures are reflected as run status `error` with traceback evidence in `errors`.
- Wiring/configuration failures are similarly explicit in run record errors and are not inferred from private logs.

## Required evidence artifacts per run
- run record JSON
- manifest JSON
- observability latest index JSON
