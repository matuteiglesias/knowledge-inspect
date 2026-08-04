# AGENTS.md — Knowledge Inspect

## Mission

Maintain bounded selection, inspection, run-manifest, and analysis-output production with explicit provenance and no hidden mutation of source knowledge.

This repository inspects and summarizes approved inputs. It does not own paper parsing, shared interoperability contracts, evidence-promotion authority, context routing, MCP transport, or source-repository semantics.

## Authority boundary

Matías owns inspection purpose, source selection, interpretation, publication, and approval of any result promoted into downstream knowledge.

Agents may:

- repair a reproduced inspection, manifest, export, or verification defect;
- improve deterministic selection, codecs, fixtures, and run evidence;
- implement an explicitly approved output or contract change;
- prepare a discrepancy report when source and manifest evidence disagree.

Agents must not independently:

- mutate source repositories, source documents, fixtures, or historical run evidence;
- reinterpret producer identity through fuzzy name matching;
- promote an inspection result into selected evidence;
- copy shared schemas into a competing local authority;
- expose physical paths, secrets, private source content, or large bodies in outputs or fixtures;
- report a partial, malformed, or unverifiable run as successful;
- add general retrieval, routing, MCP, paper-ingestion, or orchestration behavior.

## Run evidence contract

Every accepted run should make explicit, as applicable:

- run ID and producer identity;
- source identities and exact versions/checksums;
- command and relevant configuration;
- environment assumptions;
- start/end timestamps and status;
- input, output, selected, excluded, failed, and skipped counts;
- output manifests and checksums;
- warnings, errors, partial completion, and mutation status;
- artifact paths that are safe to record.

Do not rewrite old run records to conform to a new schema. Preserve them and add compatibility or migration handling.

## No-mutation rule

Inspection and verification commands must not change fixture trees, source content, or prior evidence unless the command explicitly declares a generated-output target.

When adding or modifying a verifier:

1. hash or otherwise capture the relevant fixture state before execution;
2. run all independent safe checks;
3. report PASS, FAIL, or SKIP explicitly;
4. preserve structured failure evidence;
5. verify no unintended mutation afterward;
6. return nonzero when a required check fails.

Do not hide expected nonzero outcomes. Assert them as part of the evidence contract.

## Commands

Current bounded surfaces include:

```bash
make health
make smoke
make verify-run-evidence-demo
```

`make health` checks import/compile health. `make smoke` uses a bounded fixture. `make verify-run-evidence-demo` exercises expected success and failure evidence while asserting fixture immutability.

`make inspect-last` is an operator convenience that lists recent artifacts; it is not a validation command and does not establish correctness.

Before adding `make check` or `make test`, identify the authoritative suite and confirm cost, fixtures, offline behavior, and mutation boundaries.

## Generated and sensitive artifacts

Run records, chunk sets, exports, selected views, diagnostics, and evidence bundles are generated artifacts.

- Do not hand-edit them.
- Do not commit physical local roots, secrets, user data, or substantial copied source content.
- Use sanitized representative fixtures with the same production codecs.
- Preserve failed and partial evidence when it is safe and useful.
- Keep generated outputs distinct from source fixtures and normative contracts.

## Contract changes

For changes to manifest identity, producer identity, output schemas, codecs, or shared interfaces:

1. identify local versus shared authority;
2. preserve producer-native IDs and case where required;
3. add valid, invalid, compatibility, and malformed fixtures;
4. state migration and old-run behavior;
5. verify downstream consumers;
6. update `kb-contracts` only through a separate approved contract change when shared semantics are involved.

## Change discipline

- Prefer one inspection or evidence defect per PR.
- Keep interpretation separate from mechanical validation.
- Do not broaden source access to make a test easier.
- Avoid broad storage, framework, or orchestration refactors.
- Unknown source semantics, malformed evidence, missing runs, and unavailable upstream repositories are valid blocked outcomes.
- Never claim an inspection, source read, run verification, artifact generation, or no-mutation result that was not actually observed.

## Completion report

```text
Changed:
Inspection purpose:
Sources/fixtures accessed:
Commands run:
Expected nonzero results:
Run IDs:
Artifacts/evidence:
Mutation check:
Contracts affected:
Private/source content accessed:
Failures/skips:
Blocked:
Next:
```
