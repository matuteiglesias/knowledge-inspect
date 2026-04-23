# KB Health Contract

## Run record invariant
Every canonical seam run emits the same contractual shape:
- `run_record_version`
- `project` (`"kb"`)
- `entrypoint` (`kb_chat_ingest` | `kb_chat_analyze` | `kb_papers_grobid`)
- `created_at` / `completed_at`
- `status` (`success` | `empty_success` | `partial_success` | `error`)
- `stages`
- `schema_versions`
- `warnings`
- `environment` (safe metadata only)
- `counters` (canonical stats)
- structured `inputs`
- structured `outputs` (artifact records include `path`, `artifact_kind`, `artifact_family`, `schema_version`, `promotion_status`)
- `errors`

`started_at` / `finished_at` and `ok` / `running` are no longer canonical persisted fields.

## Cheap smoke vs real ingest
- **Cheap smoke (canonical):** `kb_chat_ingest --smoke` (provider-independent contract smoke).
  - Must load config, resolve input paths, import parser/pipeline modules, parse minimal JSONL, emit run record + manifest + observability, and write a tiny local smoke artifact.
  - Must **not** call embedding providers, require API keys, write embeddings, write to Chroma, or mark processed files.
- **Real ingest:** `kb_chat_ingest` without `--smoke`, producing actual persistence side effects (provider, embeddings, Chroma writes, processed-file marks).
- **Optional dev mode:** `kb_chat_ingest --dry-run` remains parse+embed without persistence and is not the canonical cheap smoke.

## Failure semantics
- Failures still emit final contract-shape run records.
- Provider/external failures are reflected as status `error` with traceback evidence in `errors`.
- Recoverable/data issues may produce `partial_success` with populated `warnings` and/or `errors`.

## Required evidence artifacts per run
- run record JSON
- manifest JSON
- observability latest index JSON
