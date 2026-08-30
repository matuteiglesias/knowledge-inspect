# Textflow W3 — semantic runtime hardening

Status: **complete; W3 gate passed on 2026-08-30**.

This wave reconciles the useful semantic-runtime invariants recovered from `textflow-core` with the current Knowledge Inspect implementation. It is a comparison-and-hardening wave, not a Textflow code port.

The governing invariants are KI-3 (representation-aware derivative identity), KI-4 (vector/search state is rebuildable derivative state), and KI-7 (material semantic configuration/result identity is observable).

## Baseline correction

The original migration plan assumed `kb/embedding/engine.py` was the active embedding entry point. Repository tracing showed that assumption was wrong for the sanctioned chat ingest path:

```text
kb_chat_ingest
    ↓
kb.pipelines.chat_ingest
    ├── provider adapter created by _make_embed_fn
    ├── kb.storage.sqlite_cache
    ├── kb.storage.processed_files
    └── kb.vectorstore.chroma_client / chroma_io
```

`kb/embedding/engine.py` and its older adapter surface were therefore not modernized merely because they existed. W3 changed the path that actually executes.

A second baseline correction was equally important: `kb_chat_analyze` is **not a public semantic-query API**. It accepts a governed `chunk_set` or, historically, falls back to a full Chroma collection scan used for ordering/export. There is no current public `query`, `top_k`, score cutoff, reranker, or semantic-filter interface.

Consequently W3 does not invent those settings to mimic the predecessor RAG CLI.

## W3A — KI-3 representation identity

Before W3, three pieces of private semantic state could all be reused across a same-dimensional model change:

- SQLite vector cache rows were keyed by logical node/chunk ID;
- processed-file markers were keyed by source filename;
- Chroma records reused logical chunk IDs in one configured collection.

Dimension checks alone cannot distinguish two models that emit the same dimensionality.

W3 introduces a repository-local embedding representation identity:

```text
provider
+ model
+ task
+ configured dimension
        ↓
embedding-representation.v1 fingerprint
```

This identity namespaces **private derivative state only**. It does not replace or alter producer/logical `chunk_id`.

The sanctioned ingest path now:

- keys cached vectors by representation identity + logical chunk ID;
- keys processed-file state by representation identity + source file;
- resolves the operator-facing collection base name to a representation-specific physical Chroma collection;
- records the representation ID and resolved collection in run evidence and vector metadata.

Adversarial proof: unchanged logical chunk + same dimension + different model produces a different representation/cache/processed/collection namespace while the governed `chunk_id` stays unchanged.

## W3B — KI-4 rebuildability

W3B uses real persistent Chroma with the repository's existing sanitized smoke fixture and a deterministic offline test embedder. It proves:

```text
governed fixture
    ↓
canonical chunk_set + representation
    ↓
Chroma derivative
    ↓
direct query succeeds
    ↓
target collection deleted
    ↓
same governed fixture reprocessed
    ↓
same logical chunk IDs + same representation ID
    ↓
query result identity remains valid/equivalent
```

The proof also creates an unrelated collection in the same Chroma database and verifies that it survives the target reset.

### Failure found by the real integration proof

The first real-Chroma run failed before rebuild. Chroma 1.5.9 shares a process-level System per persistent path and rejects clients constructed for that path with different `Settings.allow_reset` values.

W3A had made that low-level Chroma setting follow each call's reset intent. A normal ingest followed by an explicit reset in the same process could therefore fail with incompatible client settings.

The corrected boundary is:

- Chroma's process/path client capability is stable;
- `ChromaConfig.allow_reset` remains the application-level authorization gate;
- the repository helper never calls global `client.reset()`;
- an authorized reset deletes only the named representation-specific collection.

The integration test then passed against Chroma 1.5.9.

## W3C — KI-7 semantic run evidence

W3C first fixes a seam introduced by W3A: ingest writes to the representation-resolved physical collection, so legacy analysis fallback must open that same resolved collection rather than the base name.

The analyzer now reports the operation that actually occurred.

### Governed `chunk_set` mode

Run evidence states:

- selected artifact path/run identity;
- SHA-256 of the selected artifact;
- logical member count/digest/sample;
- vector store: **not used**;
- retrieval: **not used**;
- clustering: **not used**;
- ordering: governed chunk-set order;
- result membership identity/digest.

This removes the previous false `clustering_ordering` claim for chunk-set analysis.

### Chroma fallback mode

Run evidence states:

- operator-facing base collection;
- resolved representation-specific collection;
- embedding representation ID and material provider/model/task/dimension configuration;
- logical collection membership count/digest/sample;
- operation: full collection scan for ordering;
- retrieval: **not used**;
- actual ordering mode;
- result membership identity/digest.

Chroma-loaded minimal nodes now retain their logical Chroma ID so this evidence can refer to the represented knowledge units rather than only their text/metadata.

For a one-member collection, deterministic identity order is used because hierarchical linkage is undefined for one observation.

## Retrieval configuration: current status

The original W3 sketch listed:

- query;
- `top_k`;
- cutoff;
- reranking configuration;
- filters;
- result identities.

Only the last item is currently applicable to the public analyze surface.

For the others, the correct W3 result is **N/A, not missing defaults**. `kb_chat_analyze` does not perform query-driven retrieval. The Chroma fallback scans the full represented collection for ordering. Run evidence explicitly says this rather than inventing query/retrieval fields.

If a future public retrieval consumer is introduced, KI-7 requires that its material query, `top_k`, cutoff, reranking/filter configuration, representation identity, warnings, and returned logical IDs become run evidence at that time.

## W3D — deliberately conditional capabilities

W3 adds none of the following:

- reranker;
- new hierarchical-clustering subsystem;
- RAPTOR/tree retrieval;
- general retrieval/query CLI;
- shared `kb-contracts` representation schema;
- Textflow framework/provider glue.

The existing hierarchical ordering remains only in the characterized legacy fallback for collections with more than one member. It has not been promoted into a general architecture capability.

## Verification surface

The bounded CI/local proof suite is intentionally provider-independent. External embedding credentials are not required.

```bash
python -m pip install 'numpy>=1.26,<3' 'chromadb>=1,<2'
make health
make smoke
make verify-semantic-runtime
```

`verify-semantic-runtime` covers:

1. same-dimensional cross-model representation/cache/processed-state isolation;
2. stable logical chunk identity across representation changes;
3. actual Chroma build/query/target-reset/rebuild/query equivalence;
4. unrelated collection survival;
5. sanitized fixture non-mutation;
6. Chroma logical-ID preservation;
7. truthful governed chunk-set run evidence;
8. representation-resolved Chroma fallback run evidence;
9. existing analysis input-discovery compatibility.

The CI workflow `.github/workflows/w3-semantic-runtime.yml` runs the same bounded proof family on pull requests and `main`.

## W3 gate

**PASS — 2026-08-30.** The final branch verification completed successfully with the bounded provider-independent suite, including real persistent Chroma 1.5.9.

The accepted gate proves that:

- one sanitized governed fixture can be represented and indexed without external provider access;
- logical identity is independent of embedding representation identity;
- the index can be targeted for deletion and rebuilt from the governed input;
- direct vector queryability is valid before and after rebuild;
- unrelated derivative collections survive reset;
- the current analyzer uses the correct representation-specific derivative when it falls back to Chroma;
- run evidence distinguishes governed-artifact analysis from semantic-derivative analysis and records the material identity/configuration actually used;
- no Textflow runtime/code dependency exists;
- reranking, generic clustering, RAPTOR, and a public retrieval API remain unintroduced unless a real consumer later pulls them.
