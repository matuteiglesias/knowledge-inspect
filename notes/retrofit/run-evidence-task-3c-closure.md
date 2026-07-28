# PR 3 / Task 3C — input-selection evidence and Human Gate 3

## Producer-owned evidence

Chat analysis now records `inputs.selection_mode` in its existing run record. The bounded values are:

| Value | Selected source |
|---|---|
| `explicit_chunk_set` | operator-provided `--chunk-set` path |
| `legacy_mtime_chunk_set` | newest matching chunk set by filesystem mtime |
| `chroma_fallback` | configured Chroma collection because no implicit chunk set exists |

The mode is assigned before an explicit or implicit chunk-set payload is read, or before Chroma is opened. Consequently, an error run still identifies the attempted selection route. Successful chunk-set and Chroma inputs continue to use the existing `inputs.items` details.

This is an additive field inside the current producer-owned `inputs` object. It does not change a schema version, define a shared field, or claim Profile 2 compliance.

## Operator behavior

CLI help and the canonical module runbook describe the complete precedence chain, validation/fail-closed behavior, relative and absolute path handling, all three evidence values, and retention of both legacy fallbacks. No fallback is deprecated or removed.

Compatibility tests run legacy and explicit resolution against the same chunk-set path with the same run ID, export name, and generated timestamp. The resulting Markdown export bytes and complete summary JSON bytes are equal. The selection mode is deliberately confined to the run record, so it does not alter either output.

## Human Gate 3 closure packet

| Confirmation | Result | Evidence |
|---|---|---|
| OLD INVOCATIONS WORK | **YES** | default CLI argument remains `None`; legacy discovery tests and full suite pass |
| EXPLICIT INPUT WORKS | **YES** | explicit path validates, overrides a newer implicit candidate, and exports its chunks |
| INVALID EXPLICIT INPUT FAILS CLEARLY | **YES** | run status is `error`, selection mode is retained, and no legacy/Chroma fallback occurs |
| MTIME FALLBACK RETAINED | **YES** | no-explicit-path branch still calls `_latest_chunk_set_path` |
| CHROMA FALLBACK RETAINED | **YES** | no-candidate branch still opens and loads the configured collection |
| SUMMARY BYTES FOR SAME INPUT | **UNCHANGED** | byte-equivalence test compares complete summary JSON bytes |
| EXPORT BYTES FOR SAME INPUT | **UNCHANGED** | byte-equivalence test compares complete Markdown bytes |
| CURRENT SCHEMA VERSIONS | **UNCHANGED** | no schema/version constant was modified |
| PROFILE 2 CLAIMED | **NO** | selection mode is documented as producer-owned only |
| TESTS | **PASS** | complete PR gate recorded below |

## Complete PR gate

- focused chat-analysis discovery, explicit-input, evidence, and byte-equivalence tests;
- complete pytest suite;
- repository health/compile check;
- whitespace/error-marker check;
- clean committed worktree check.

## Decision

Human reviewer must choose one:

- **ACCEPT AND MERGE**
- **REQUEST NARROW CORRECTIONS**
- **REJECT**
- **AUTHORIZE LATER MTIME-DEPRECATION DESIGN**

No fallback is removed at this gate. Authorization of a later design is not authorization to deprecate or remove mtime discovery in this PR.
