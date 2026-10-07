# Evidence JSONL export

Knowledge Inspect owns a narrow producer-side adapter from its own governed
`summary_bus / chunk_set_summary` artifacts into generic JSONL evidence records
that can be consumed by producer-agnostic selectors such as KB Artifacts.

This is a deterministic representation adapter, not evidence selection or
promotion.

## Contract

Producer-local interface:

```text
producer-local:knowledge-inspect.evidence-jsonl@1
```

Input:

```text
artifact:kb.summary-bus@1
artifact_kind: chunk_set_summary
schema_version: 1
producer: kb
```

Output records use only fields already accepted by the generic KB Artifacts JSONL
reader: `source_ref`, `summary`, `title`, `tags`, `text_sha256`, and
`meta`.

The adapter preserves run identity and checksums while deliberately dropping
physical paths such as `export_path` and source artifact `path`.

## Command

```bash
python3 -m kb.cli.kb_evidence_export \
  artifacts/summaries/<run>.summary.json \
  --output /new/path/evidence.jsonl
```

The output path must not already exist. Inputs are sorted before projection,
records are serialized with sorted JSON keys, and publication uses an atomic
replace from a temporary file.

## Authority

Knowledge Inspect owns only the source-specific projection because it owns the
meaning of its native summary artifact.

It does **not** own:

- downstream eligibility or ranking;
- deduplication or selection policy;
- selected-evidence identity;
- promotion or publication;
- KB Artifacts manifests;
- shared interoperability schema authority;
- MCP transport.

Those remain with their existing owners.
