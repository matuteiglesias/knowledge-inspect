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

## Naming and stable IDs
- `run_id` pattern: `<operator>_YYYYMMDDTHHMMSSZ`
- Run record + manifest filenames include `run_id`.
- Observability latest index is stable per operator name.
