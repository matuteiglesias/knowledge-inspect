# KB Health Contract

## Run record invariant
Every canonical seam run emits:
- `run_id`
- `operator`
- `started_at` / `finished_at`
- `status` (`ok` or `error`)
- `inputs`, `outputs`, `stats`, `errors`

## Cheap smoke vs real ingest
- **Cheap smoke:** `kb_chat_ingest --dry-run` (parsing + embedding path without vectorstore writes/processed-files marks).
- **Real ingest:** `kb_chat_ingest` without `--dry-run`, producing actual persistence side effects.

## Failure semantics
- Provider/external failures are reflected as run status `error` with traceback evidence in `errors`.
- Wiring/configuration failures are similarly explicit in run record errors and are not inferred from private logs.

## Required evidence artifacts per run
- run record JSON
- manifest JSON
- observability latest index JSON
