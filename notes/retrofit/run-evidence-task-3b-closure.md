# PR 3 / Task 3B — explicit chat-analysis chunk-set input

## Implemented boundary

`kb_chat_analyze` now accepts the additive `--chunk-set PATH` option. The pipeline API accepts the same path as `chunk_set_path`. Omitting it preserves the legacy invocation and selection behavior.

The bounded precedence is:

1. an explicit chunk-set path;
2. the legacy newest direct `artifacts/chunk_sets/*.chunk_set.json` child by filesystem mtime;
3. the configured Chroma collection when no implicit chunk set exists.

The reference is a filesystem path, not a logical artifact/run reference. Relative paths are interpreted from the process working directory after `~` expansion, and absolute paths (including paths outside `KB_ROOT`) are accepted. No governed reference layer was introduced.

## Validation and failure behavior

Explicit inputs are validated against the existing `chunk_set.v1` contract before their chunks are analyzed. A missing, unreadable, malformed, or contract-invalid explicit input fails the run. It does not fall through to mtime discovery or Chroma. This fail-closed behavior prevents an operator typo from silently analyzing a different input.

Legacy mtime-selected inputs retain their characterized behavior, including the existing absence-only Chroma fallback and the existing failure behavior for an invalid newest implicit candidate. Task 3B does not make implicit discovery validity-aware.

## Compatibility evidence

Focused tests cover the additive CLI default, explicit-over-implicit precedence, contract validation, fail-closed behavior, relative and absolute paths, unchanged legacy discovery, Chroma fallback, and byte-equivalent Markdown when explicit and legacy resolution select the same chunk set under a fixed generation timestamp.

No chunk-set, summary, export, run-record, or bundle schema version changed. Summary construction, export ordering/content, IDs, newest-by-mtime discovery, and Chroma fallback remain in place. No Profile 2 field or logical-reference abstraction was added.

## Closure

Task 3B is complete. The remaining PR 3 work may build on the explicit path seam but must continue to preserve the precedence and compatibility behavior above unless separately approved.
