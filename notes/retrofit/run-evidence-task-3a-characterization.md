# PR 3 / Task 3A — chat-analysis input-discovery characterization

**Scope:** characterization and compatibility tests only. No explicit-input CLI option or production resolution change is included.

## Verified current selection decision table

`kb_chat_analyze` calls `_latest_chunk_set_path`, which considers only direct children of `artifacts/chunk_sets` matching `*.chunk_set.json`, sorts them by descending filesystem `st_mtime`, and takes the first candidate. It uses Chroma only when that search returns no candidate.

| Filesystem state | Selected input | If selected input cannot be read/decoded | Compatibility consequence |
|---|---|---|---|
| no matching chunk set | configured Chroma collection | Chroma/load exception makes the run an error | Chroma is the absence-only fallback |
| one matching chunk set | that file | run errors; Chroma is not attempted | filename metadata is not validated during discovery |
| multiple matching chunk sets, distinct mtimes | greatest `st_mtime` | run errors; neither an older file nor Chroma is attempted | filename and payload `run_id` do not affect precedence |
| multiple matching chunk sets, equal mtimes | first path in the filesystem enumeration retained by Python's stable sort | same as above | no explicit tie-breaker exists; selection is ambiguous across filesystem/enumeration behavior |
| newest matching file contains invalid JSON | invalid newest file | run errors before an input item is recorded | no validity-aware retry or fallback exists |
| a chunk set is copied/restored | whichever matching file has the greatest resulting mtime | same as above | copy/restore/touch operations can change semantic selection without changing payload metadata |

Once a chunk set is selected, its `chunks` array is used in stored order. Chroma vectors are loaded and hierarchically ordered. The existing `max_nodes`, summary payload, Markdown export, artifact IDs/schema versions, and run-evidence behavior remain untouched by Task 3A.

## Bounded verification fixtures

The focused tests create only temporary, one-chunk JSON payloads and a zero-node in-memory Chroma adapter. They cover absence, one candidate, multiple candidates, distinct and equal mtimes, an invalid newest candidate, copy/restore timestamp effects, and the Chroma fallback. No provider, persistent Chroma instance, corpus, schema, or production artifact is used.

## Human micro-gate required before Task 3B

The current behavior does not resolve the following design choices. Human approval is required before implementation:

1. **Explicit flag name:** for example `--chunk-set` versus `--analysis-input`.
2. **Accepted reference type:** path, artifact/run ID, latest-pointer reference, or a bounded combination.
3. **Resolution precedence:** explicit input versus mtime discovery and Chroma fallback.
4. **Invalid explicit input:** fail closed, retry discovery, or fall back to Chroma.
5. **Absolute-path policy:** reject, allow, or constrain paths outside `KB_ROOT` / `artifacts/chunk_sets`.

Two additional ambiguities should be approved with that table: whether equal-mtime fallback selection needs a deterministic tie-breaker, and whether an invalid newest implicit candidate must continue to suppress both older candidates and Chroma. Changing either would alter current precedence and therefore is outside Task 3A.

## Closure

Task 3A is complete when the characterization tests pass. Task 3B remains intentionally blocked on the human micro-gate above; this packet makes no recommendation that silently changes existing invocation, fallback, summary, export, schema, or ID semantics.
