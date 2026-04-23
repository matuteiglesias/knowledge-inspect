# KB Artifact Surface (Public)

## Public outputs
- `artifacts/run_records/<run_id>.run_record.json`
- `artifacts/manifests/<run_id>.manifest.json`
- `artifacts/observability/<operator>.latest.json`
- `artifacts/exports/<export_name>` (analyze seam)

## Separation from internals
- Public artifacts above are stable, inspectable surfaces.
- Internal caches/state such as `embedding_cache.sqlite`, `store/`, parser metadata internals, and private module functions are non-contract internals.

## Consumer contract
Consumers and UI integrations should discover status/results via run records, manifests, and observability indexes; they should not parse arbitrary internal directories.

### Manifest minimum contract (v2)
- `manifest_version`
- `run_id`
- `artifact_family` (`contract`)
- `artifact_kind` (`manifest`)
- `schema_version_emitted`
- `producer` (stable module id, `kb`)
- `producer_version`
- `status`
- explicit `artifacts[]` entries for every public output (not only a nested bag)
- per-artifact `sha256` when file hashing is feasible

### Observability latest minimum contract (v2)
- module-local boundary fields:
  - `artifact_family: module_observability`
  - `artifact_kind: module_latest`
  - `scope: module_local`
- linkage fields:
  - `run_id`, `entrypoint`, `status`
  - `run_record_path`, `manifest_path`
  - `completed_at`

## Naming and stable IDs
- `run_id` pattern: `<operator>_YYYYMMDDTHHMMSSZ`
- Run record + manifest filenames include `run_id`.
- Observability latest index is stable per operator name, but is explicitly module-local (not a global ecosystem aggregate).
