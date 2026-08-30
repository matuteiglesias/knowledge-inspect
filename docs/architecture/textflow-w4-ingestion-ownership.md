# Textflow W4 — ingestion ownership resolution

Status: **implementation-complete pending branch verification**.

W4 resolves one predecessor question: where should Textflow's historical JSONL/chat/day-file ingestion live in the mature knowledge ecosystem?

The answer is: **nowhere centrally, unless a real source-owning producer is pulled by an active consumer.**

## Governing boundary

Knowledge Inspect is an inspection producer. Its repository governance says it inspects approved inputs and does not own source-repository semantics. `SYSTEM.yaml` declares producer-owned chunk-set consumption and bounded analysis/run evidence; it does not declare raw chat/day-file source authority.

The mature flow is therefore:

```text
raw source
    ↓
source-owning producer
    ↓
producer-owned canonical units
    ↓
governed artifact
    ↓
Knowledge Inspect / other consumers
```

Paper KB is the positive example: paper acquisition/parsing/identity remain producer-owned and Knowledge Inspect consumes governed artifacts at the seam.

## Repository-visible Textflow dependency result

The predecessor capability census found no repository-visible downstream GitHub reference to `textflow-core`, `snippetflow`, the historical collection names, or Textflow's ignored Chroma/SQLite stores. Textflow also has no tracked GitHub Actions workflow at the frozen baseline.

That is sufficient for the **repository-visible W4 source-ownership decision**: no current workflow needs Textflow itself to remain source authority.

It is **not** sufficient to archive Textflow. Its ignored runtime state means GitHub cannot prove absence of:

- cron entries;
- systemd units;
- shell aliases/functions;
- external local scripts importing the checkout;
- active processes using ignored Chroma/SQLite/tree state;
- irreplaceable data only in ignored stores/exports.

Those machine-local checks remain an explicit archive-readiness blocker.

## Knowledge Inspect legacy chat path

`kb_chat_ingest`, `kb.pipelines.chat_ingest`, and `kb.parsers.chat_jsonl` remain in the repository because W3 uses the path as a bounded regression fixture for representation/cache/index behavior and because deleting compatibility code is not required to resolve ownership.

W4 changes its authority status instead of pretending the code does not exist:

- CLI invocation emits an explicit deprecation/source-boundary warning;
- run evidence records `source_authority_status = legacy_compatibility_non_authoritative`;
- the run warning list contains a structured `legacy_source_seam` warning;
- manifest evidence for the emitted compatibility chunk set carries the same status;
- `AGENTS.md` forbids treating the path as a new source-authority seam;
- `SYSTEM.yaml` explicitly says Knowledge Inspect does not own raw chat/day-file source interpretation or universal ingestion authority.

Historical compatibility artifacts are still structurally valid. That validity does not promote their parser into a current architectural authority.

## Canonical smoke moves to governed input

Before W4, `make smoke` began from `tests/fixtures/smoke_chat.jsonl` and invoked the legacy raw-chat parser. That made an obsolete source seam look canonical even though repository governance said otherwise.

W4 adds `tests/fixtures/governed_smoke.chunk_set.json`, a sanitized fixture representing output from a source-owning producer. Canonical smoke now:

1. validates that governed artifact;
2. uses the explicit chunk-set analysis path;
3. writes generated evidence under a temporary `KB_ROOT`;
4. proves the governed fixture is not mutated;
5. bypasses raw source parsing, Chroma, retrieval, and clustering.

The raw-chat tests remain only under `make verify-semantic-runtime` as compatibility/regression proofs for W3.

## Decision on a chat producer

No repository-visible active consumer was found that requires a governed chat/day-file corpus producer.

Therefore W4 does **not** create:

- a `chat-kb` repository;
- a shared chat schema in `kb-contracts`;
- generic chat parsing in Knowledge Inspect;
- a universal JSONL ingestion framework.

If a concrete current consumer later requires governed chat history, the pull condition is explicit: identify the source authority and consumer, then build the smallest producer artifact that preserves source-native identity/provenance. Knowledge Inspect should consume that artifact rather than own its raw source interpretation.

## W4 gate

W4 passes when:

- canonical Knowledge Inspect smoke begins from a governed producer artifact;
- raw chat/day-file interpretation is not declared as Knowledge Inspect authority;
- the retained raw-chat path is explicitly compatibility-only in operator and run evidence;
- W3 semantic-runtime regression coverage remains intact;
- no repository-visible active workflow obtains source truth from Textflow or Textflow's Chroma/SQLite state;
- Textflow archive-readiness remains honestly blocked on the separate machine-local checks rather than falsely inferred from GitHub evidence.
