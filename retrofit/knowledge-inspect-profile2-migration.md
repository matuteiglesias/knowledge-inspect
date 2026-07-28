# Knowledge Inspect Profile 2 migration audit

**Phase:** 1 — compatibility and migration audit only
**Baseline inspected:** `3ac7a47a87ad7fe9b3cec7a452a2966b304b85ec`
**Repository:** `matuteiglesias/knowledge-inspect`
**Decision status:** recommendations below require Human Gate 1 approval from Matías. No production contract, schema, alias, or historical artifact was changed by this audit.

## Executive finding

Knowledge Inspect is already a mature run-level producer. Its three sanctioned seams share a v2 run-record/finalization implementation, bounded CLIs, structured stages and failures, atomic JSON writes, run-bundle inventory with checksums, and module-local latest pointers. The existing `<run_id>.manifest.json` is a **run-bundle manifest**, despite its generic `artifact_kind: manifest`; it is not a per-artifact manifest. The safest Profile 2 migration is therefore additive: preserve all current artifacts and identities, add explicit identity mapping and mixed-version readers, emit per-artifact manifests beside a versioned bundle, and migrate the run record only after those readers exist.

The proposed shared release itself is not present in this repository. Consequently, exact shared field spellings, required status/error taxonomies, ID algorithms, and a contract-release identifier remain **uncertain** and must not be guessed. This packet is implementation-ready as a repository-side sequence and test plan, but not ready to authorize schema emission until Matías supplies/approves the shared release and Human Gate 1 decisions listed at the end.

## 1. Current producer map

| Seam | CLI entrypoint | Orchestrator | Run-ID function | Run record | Manifest | Observability | Primary artifacts | Tests | Smoke/health command |
|---|---|---|---|---|---|---|---|---|---|
| `kb_chat_ingest` | `kb/cli/kb_chat_ingest.py::main`; `python -m kb.cli.kb_chat_ingest` | `kb/pipelines/chat_ingest.py::ingest_paths` | shared `kb/pipelines/run_record_contract.py::make_run_id("kb_chat_ingest")` | shared `make_run_record` + `finalize_and_write_contract_artifacts`; `artifacts/run_records/<run_id>.run_record.json` | shared finalizer; `artifacts/manifests/<run_id>.manifest.json` | shared finalizer; operator `kb.chat_ingest` gives `artifacts/observability/kb.chat_ingest.latest.json` | v1 chunk set; smoke preview; Chroma/processed-file state is internal | `tests/test_chat_ingest_smoke.py`, `tests/test_chunk_set_contract.py`, `tests/test_cli_validate_chunk_set.py`, `tests/test_contract_compliance.py` | `make smoke`; direct: `python3 -m kb.cli.kb_chat_ingest --paths test_data/2025-06-16.jsonl --smoke` |
| `kb_chat_analyze` | `kb/cli/kb_chat_analyze.py::main`; `python -m kb.cli.kb_chat_analyze` | `kb/pipelines/chat_analyze.py::analyze` | shared `make_run_id("kb_chat_analyze")` | same shared writer/path | same shared bundle writer/path | operator `kb.chat_analyze` gives `artifacts/observability/kb.chat_analyze.latest.json` | v1 summary plus named Markdown export; consumes newest chunk set by mtime or collection fallback | `tests/test_chat_analyze_artifacts.py`, `tests/test_contract_compliance.py` | no provider-independent dedicated smoke; focused test is the safe check |
| `kb_papers_grobid` | `kb/cli/kb_papers_grobid.py::main`; `python -m kb.cli.kb_papers_grobid` | `kb/pipelines/papers_grobid.py::run_pdf` | shared `make_run_id("kb_papers_grobid")` | same shared writer/path | same shared bundle writer/path | operator `kb.papers_grobid` gives `artifacts/observability/kb.papers_grobid.latest.json` | optional TEI v1 record; optional Chroma side effect; external `grobid_ingest.run` | structural coverage in `tests/test_contract_compliance.py`; no provider-independent end-to-end fixture | no safe end-to-end smoke; external adapter/service is required |

All CLI `main` functions regard `success`, `empty_success`, and `partial_success` as exit 0, print structured artifact locations/status, and return 1 for `error`. Ingest additionally returns 2 for absent input or mutually exclusive `--smoke`/`--dry-run`. Migration implication: these exit semantics and operator command names are public compatibility surfaces.

## 2. Current public artifact surface

The authoritative documentation is `kb_artifact_surface.md` and the operator discovery order is in `runbooks/kb_module_runbook.md`.

| Artifact path | Current version/writer | Consumers and dependencies | Compatibility implication |
|---|---|---|---|
| `artifacts/run_records/<run_id>.run_record.json` | `run_record_version: 2`, `schema_versions.run_record: 2`; shared constructor/finalizer | all CLIs return/print it; latest and bundle contain its path; tests inspect fixed top-level shape | Filename suffix and `run_id` linkage are stable. Preserve v2 parsing and do not rewrite historical records. |
| `artifacts/manifests/<run_id>.manifest.json` | `manifest_version: 2`; shared finalizer | latest links it; run record lists it; runbook directs operators to inventory/checksums | Generic filename is relied upon as one manifest per run. Retain it as legacy/current bundle while adding distinct per-artifact files. |
| `artifacts/observability/<operator>.latest.json` | `observability_version: 2`; shared finalizer | operators/consumers use stable operator filename to find latest run and bundle | It is an atomic, mutable module-local pointer, not history and not an ecosystem aggregator. Preserve last-writer-wins semantics unless explicitly versioned. |
| `artifacts/chunk_sets/<run_id>.chunk_set.json` | `schema_version: 1`; `chat_ingest.ingest_paths` | analyzer selects newest matching file by filesystem mtime; validator/schema/tests consume it | Filename glob and mtime are behavioral dependencies. New sidecars must not match `*.chunk_set.json`; stable chunk IDs and chunk semantics must not change. |
| `artifacts/summaries/<run_id>.summary.json` | `schema_version: 1`; `chat_analyze.analyze` | run record and manifest inventory; external Summary Bus consumers implied by public docs | Keep summary body and path stable. Add identity/integrity only through approved additive fields or sidecars. |
| `artifacts/exports/<export_name>` | artifact record uses `schema_version: 1`; analyzer writes atomic text | CLI accepts a caller-controlled filename and prints the path; summary embeds `export_path` and complete `summary_text` | Filename is explicitly caller-selected and may be overwritten across runs. Do not silently convert it to run-scoped naming; manifests must identify the actual instance/checksum. |

Paths are built by `KBConfig` under `KB_ROOT/artifacts`, but persisted paths are `str(Path)` and can therefore be absolute or relative depending on configuration. Run record, manifest, latest, summary, and input records repeat those strings. This is a portability and disclosure concern for a shared release: readers must initially accept existing path strings, while new portable references should be repository/artifact-root-relative aliases. Historical artifacts remain read-only.

No artifacts are tracked as fixtures at the inspected baseline. Tests generate isolated artifacts under pytest temporary roots. Local operational artifacts, secrets, caches, and corpora were deliberately not used as report evidence.

## 3. Exact current shapes

### 3.1 Run record v2

Writer: `kb/pipelines/run_record_contract.py::make_run_record`, seam-specific mutation, then `finalize_and_write_contract_artifacts`.

| Concern | Current JSON path and behavior | Migration implication |
|---|---|---|
| schema | `$.run_record_version = 2`; `$.schema_versions = {run_record:2, manifest:2, observability:2}` | Shared contract name/version needs an additive discriminator; retain numeric fields as aliases. |
| producer/module identity | `$.project = "kb"`; no top-level `producer` | Do not infer repository identity from `project`; add explicit mapped IDs. |
| run identity | `$.run_id = <entrypoint>_YYYYMMDDTHHMMSSZ`; second resolution, no collision guard | Preserve as legacy ID; an approved stable ID may be an alias/new field, not an in-place algorithm swap. |
| CLI/operator | `$.entrypoint` is underscore CLI name; `$.operator` is dotted name | Treat as distinct identities. |
| status/timestamps | starts `$.status = error`; `created_at`; null `completed_at`; finalizer derives one of four statuses and sets UTC `Z` completion | Map rather than replace statuses; timestamps are second-resolution UTC. Stage timestamps use same helper. |
| inputs/config | `$.config` and `$.inputs`; each seam records paths/options; ingest input is `items[].paths`, analyze adds chunk-set or collection, papers adds PDF/provider options | Define redaction/path portability policy before claiming shared safe metadata. Inputs are bounded by CLI but path strings are not normalized. |
| outputs/artifact identity | `$.outputs.artifacts[]` has `path`, `artifact_kind`, `artifact_family`, `schema_version`, `promotion_status`, optional flags/companions; convenience paths coexist | Mixed-version reader must accept both structured entries and convenience fields. Current entries lack artifact ID and checksum (checksums live in bundle). |
| integrity | no checksum in the run-record artifact entries | Add references/digests without removing current entries. Beware run record self-hash ordering described below. |
| warnings/errors | arrays; ingest missing input is both warning and error; exception has `type`, `message`, and full traceback | Shared error taxonomy requires aliases. Tracebacks can reveal paths and need an approved safe-public policy. |
| environment | `$.environment.{python_version,platform,kb_root,artifacts_dir}` | It excludes secret values but includes path/platform metadata; “safe” for local ops is not automatically safe for publication. Add sanitized shared metadata, preserve legacy locally. |
| counters | canonical `$.counters`; compatibility copy `$.stats`; seam-specific integer keys | Keep `stats` through observation/deprecation. Specify counter namespace/units additively. |
| provenance | input records, config, stages, entrypoint and outputs provide operational provenance; no explicit shared provenance object | Add shared provenance references; do not reinterpret current fields. |
| stages | `$.stages[] = {name,status,started_at,completed_at,details}`; initial `pending`, helpers write `running/success/error` | Profile 2 should retain this richer stage evidence even if shared minimum is smaller. |

Final status derivation (`_derive_final_status`) makes explicit `error` terminal; errors otherwise cause `partial_success`; explicit empty with errors also becomes partial; otherwise zero `nodes_loaded`/`nodes_kept` becomes `empty_success`, else `success`. A papers run has no node counter, so a requested success currently derives `empty_success`, despite the orchestrator requesting `success`. This existing behavior must be fixture-pinned before any status mapping.

Failure handling is best-effort: seam exceptions are attached and the finalizer runs in `finally`. During finalization, the latest pointer is written before the final run record and manifest; the first run-record write is inside a swallowed exception handler, while manifest writing is not swallowed. A crash can therefore leave latest pointing to incomplete/missing bundle evidence. Profile 2 hardening should test and document this ordering, but changing it is a separately approved semantic migration.

### 3.2 Current manifest v2: run bundle

Writer: `finalize_and_write_contract_artifacts`; path `artifacts/manifests/<run_id>.manifest.json`.

Fields are `$.manifest_version`, `$.run_id`, `$.artifact_family = contract`, `$.artifact_kind = manifest`, `$.schema_version_emitted`, `$.project`, `$.entrypoint`, `$.producer = kb`, `$.producer_version = 0.1.0`, `$.status`, `$.created_at`, `$.completed_at`, and `$.artifacts[]`. Each artifact entry carries kind, family, path, emitted schema version, promotion status, and normally `sha256` when the referenced file exists.

This is a **run-bundle manifest**: `artifacts[]` inventories several outputs rather than describing one payload. The manifest lists itself but intentionally omits its own digest. It hashes the latest pointer after writing it, and hashes the run record before its later rewrite; therefore the recorded run-record digest can be stale because the run record is mutated/written again after checksum collection. This is artifact-inventory integrity, but not yet reliably self-consistent artifact-level integrity. Migration must add tests/ordering rather than claim full Profile 2 checksum compliance prematurely.

### 3.3 Observability latest v2

Writer: shared finalizer; path uses `$.operator` literally. Shape: `observability_version`, `artifact_family: module_observability`, `artifact_kind: module_latest`, `scope: module_local`, `run_id`, `project`, `entrypoint`, `operator`, final `status`, `run_record_path`, `manifest_path`, and `completed_at`. Atomic replacement prevents partial JSON, but it is overwritten per operator and contains no generation/CAS protection. Historical observability is the run/bundle evidence, not prior latest files.

### 3.4 Chunk set v1

Writer: `chat_ingest.ingest_paths`; validator: `kb/contracts/chunk_set.py`; JSON Schema: `contracts/chunk_set.v1.schema.json`.

Top level: `artifact_family: chunk_bus`, `artifact_kind: chunk_set`, `schema_version: 1`, `run_id`, `producer: kb`, `entrypoint: kb_chat_ingest`, `source_items[]` (basenames), `chunk_count`, and `chunks[]`. Writer chunks contain `chunk_id`, `source_file`, `header_path`, `text`, `metadata.date`, then add `chunk_index`, `char_len`, and `document_id = source_file`. Schema permits additional properties and requires identity/text/index/length/metadata; Python validation additionally requires `paper_id` or `document_id`. Chunk IDs come from `node_id_from_node_text(text, source_file, header_path)` and are content/context-derived, while run IDs are time-derived. Preserve both meanings.

### 3.5 Summary v1 and export

Writer: `chat_analyze.analyze`. Summary fields are `artifact_family: summary_bus`, `artifact_kind: chunk_set_summary`, `schema_version: 1`, `run_id`, `producer: kb`, `entrypoint: kb_chat_analyze`, `input_artifacts[]`, `summary_text`, and `export_path`. There is no summary schema file, explicit status/timestamps, checksum, warnings/errors, counter block, artifact ID, or explicit provenance object. Those details exist in the run record/bundle. The Markdown export is either an empty marker or combined ordered node text; with a chunk set it preserves chunk order, while collection fallback uses hierarchical clustering. The export has no embedded schema envelope. Per-artifact sidecars are safer than changing export bytes.

### 3.6 Papers outputs

`papers_grobid.run_pdf` delegates to external `grobid_ingest.run`. When `save_tei` is provided it records a `tei_xml`/`grobid` v1 artifact; optional Chroma is recorded as a convenience output/side effect, not a canonical bus artifact. No chunk-set or summary is emitted. This seam is explicitly transitional and must not be normalized into chat semantics.

## 4. Explicit producer-identity mapping

| Identity dimension | Current/approved mapping | Evidence and rule for migration |
|---|---|---|
| repository identity | `matuteiglesias/knowledge-inspect` (human governance); descriptive product name `knowledge-inspect` | Not currently serialized. Add descriptor metadata only after Matías approves canonical spelling. Never derive it from `project`. |
| Python module identity | `kb` | Package path and module docs. This is a code boundary, not necessarily a gateway producer ID. |
| gateway producer identity | not represented / **uncertain** | No gateway contract exists in repository. Do not invent or fuzzy-normalize one. Candidate must be supplied by shared release/Human Gate 1. |
| manifest producer identity | `kb` | `run_record_contract.PRODUCER`; chunk set and summary also emit `producer: kb`. Preserve exactly as legacy identity. |
| CLI identity | `kb_chat_ingest`, `kb_chat_analyze`, `kb_papers_grobid` | `entrypoint`, CLI `prog`, run-ID prefix. Preserve exactly. |
| operator identity | `kb.chat_ingest`, `kb.chat_analyze`, `kb.papers_grobid` | `operator` and latest filename. Preserve dots and per-seam distinctions. |
| run-record project identity | `kb` | `run_record_contract.PROJECT`; currently same spelling as producer/module but semantically distinct. |

Recommended explicit descriptor mapping: separate keys such as `repository_id`, `module_id`, `gateway_producer_id`, `manifest_producer_id`, `entrypoint_id`, and `operator_id`, each populated by a reviewed table—not normalization code. Whether the shared canonical producer becomes `knowledge-inspect`, `kb`, or another registry ID is a Matías decision. Existing `kb` values must remain as compatibility fields during migration.

## 5. Shared-release delta matrix

Because no proposed shared schema/fixtures are checked in, classifications compare concepts, not unverified field spellings.

| Target concept | Classification | Current evidence / required delta |
|---|---|---|
| `module.v1` | additive | No module descriptor artifact. Add without changing runtime seams. |
| `artifact_manifest.v1` | additive | Current manifest is bundle-level. Add one sidecar per primary/companion artifact. |
| `run_bundle_manifest.v1` | alias required + version bump required | Current v2 generic manifest already has bundle semantics. Preserve filename/shape while emitting an explicitly typed/versioned bundle representation. Exact coexistence shape requires approval. |
| shared run record | alias required + version bump required | Rich v2 exists, but lacks confirmed shared discriminator/IDs/provenance and uses legacy names. Add aliases first; do not overwrite v2. |
| stable IDs | uncertain | Chunk IDs are stable for content/context; run IDs are timestamp/entrypoint and can collide. Shared algorithms unavailable. |
| producer IDs | alias required | `kb` exists but identity dimensions are conflated. Add explicit mapping; retain values. |
| statuses | already compatible conceptually; alias required | Four contractual final statuses plus stage statuses exist. Exact shared enum/mapping is unknown. |
| timestamps | already compatible conceptually; alias required | UTC `Z` created/completed and stage times exist; target names/precision unknown. |
| warnings and errors | already compatible conceptually; alias required | Structured arrays exist, but missing-input duplication and traceback taxonomy need mapping/sanitization. |
| counters | already compatible conceptually | Canonical `counters`, deprecated-compatible `stats`; shared namespaces/units unknown. |
| safe environment | uncertain | No secrets are copied, but local root paths, platform, and tracebacks can disclose local details. |
| artifact checksums | additive + version bump required | Bundle SHA-256 exists, but run-record digest ordering can stale and per-artifact manifests do not exist. |
| provenance | additive | Inputs/config/stages provide fragments; explicit shared provenance and parent artifact IDs are absent. |
| contract-release claim | additive, currently prohibited | No shared release dependency or embedded release ID. Claim only after exact fixtures/tests pass. |
| exports | not applicable to JSON envelope; additive sidecar | Preserve Markdown bytes and caller filename; describe through artifact manifest. |
| historical artifacts | deprecation required only for readers, never rewrite | Readers must accept v1/v2 history indefinitely per policy; removal timing is Human Gate 1. |

“Version bump required” means a new explicit shared/bundle contract must not masquerade as the current numeric v2 shape. It does **not** authorize changing existing versions in Phase 1.

## 6. Artifact manifest versus bundle manifest recommendation

**Determination:** current `<run_id>.manifest.json` is a run-bundle manifest.

**Safest strategy, pending Human Gate 1:** retain it as the legacy bundle; introduce per-artifact manifest sidecars additively; emit both legacy and new bundle shapes (either separate filename or an additive namespaced block approved by the shared schema); link the new bundle to per-artifact manifests; preserve current `artifacts[]` entries; migrate consumers gradually; only deprecate the generic manifest after a measured observation period. Sidecar names must not collide with analyzer’s `*.chunk_set.json` glob.

Per-artifact manifests should minimally bind an approved artifact ID, exact artifact relative reference, family/kind/schema, producer mapping, originating run, byte size and SHA-256, creation/completion time where meaningful, status, provenance/input references, and contract-release ID. Checksums must be computed after payload finalization. The bundle should be finalized after its members and should avoid unverifiable self-hashing cycles.

## 7. Run-record compatibility plan

- **Additive fields:** shared contract discriminator/version, explicit identity object, stable/shared run ID alias if required, portable artifact references, artifact-manifest references, explicit provenance, counter definitions, safe-environment view, and contract-release ID.
- **Aliases:** keep `run_record_version`, `project`, `entrypoint`, `operator`, `created_at`, `completed_at`, `outputs.artifacts`, `counters`, and `stats`. Map shared names alongside them; never silently reinterpret `project` as producer.
- **Deprecated fields:** only `stats` is already documented as compatibility. Marking any path convenience field, `project`, or generic `manifest_path` deprecated requires Human Gate 1 and usage evidence.
- **Status mapping:** preserve all four current final values. Build an exhaustive table to shared values, retain the raw legacy status, and separately map stage `pending/running/success/error`. Fixture-pin zero-node papers behavior.
- **Error mapping:** preserve legacy `type/message/traceback`; add shared category/code/retryability/safe detail fields. Map `missing_input` deterministically. Never discard traceback locally merely to fit a smaller contract.
- **Timestamp mapping:** retain `created_at/completed_at` and stage times; shared aliases must parse `Z`, maintain UTC, and tolerate second precision.
- **Output mapping:** parse `outputs.artifacts[]` first, then legacy convenience paths. New readers resolve explicit artifact-root-relative references but continue accepting existing absolute/relative strings.
- **Mixed-version parsing:** dispatch on explicit shared discriminator when present, otherwise `run_record_version`; tolerate additive properties; validate known required fields; preserve unknown data; expose normalized in-memory values plus raw source values.
- **Historical behavior:** never mutate or backfill emitted files in place. Historical checksum/path inconsistencies are reported as legacy observations, not “repaired.” New guarantees begin at a declared release boundary.

## 8. Profile 2 compliance evidence and gaps

| Profile 2 concern | Exact current evidence | Assessment / migration implication |
|---|---|---|
| sanctioned entrypoints | `kb_entrypoints.md`; three `kb/cli/*.py::main` functions | Compatible; keep singular operator commands. |
| bounded inputs | argparse surfaces: path lists/glob, export name/caps, one PDF/options | Present, though path containment/export traversal policy is not explicit. Do not broaden inputs. |
| explicit outputs | public surface docs; `add_output_artifact`; seam result dataclasses | Strong run-level evidence; add artifact IDs/manifests. |
| run records | shared v2 constructor/finalizer for all seams | Strong; requires shared aliases/version negotiation. |
| manifests | v2 run-bundle inventory | Strong bundle evidence; not per-artifact manifests. |
| artifact-level integrity | `_maybe_sha256` and bundle `artifacts[].sha256` | Partial: absent per-artifact sidecars and possible stale run-record hash. Do not claim full compliance yet. |
| observability | atomic operator latest v2, module-local scope | Strong, with known finalization window; retain semantics. |
| structured failures | exception/missing-input records; final artifact emission in `finally`; CLI nonzero on error | Strong locally; taxonomy/redaction and finalizer-failure robustness need tests. |
| safe environment | selected environment keys; secrets remain env-only | Partial/uncertain because paths/platform/traceback can expose local metadata. |
| validation | chunk-set JSON Schema/CLI; `make health`; `make smoke` | Strong for chunk set/ingest; no summary schema and no safe papers smoke. |
| compliance tests | `test_contract_compliance`, smoke, chunk, analyze artifact tests | Strong drift gate for current v2; add mixed-version/shared fixtures. |
| rollback/recovery | atomic writes; idempotent Chroma writes; processed-files/cache; dry-run/smoke | Mature internal recovery features; latest/bundle transaction is not atomic as a group. Preserve internals. |

Profile 2 adoption must add guarantees around artifact identity/integrity and compatibility; it must not delete stages, counters, structured evidence, or mature recovery behavior to resemble a smaller Profile 1 producer.

## 9. Safe bounded migration sequence

| Stage | Likely files | Risk | Rollback | Validation | Shared-release dependency |
|---|---|---|---|---|---|
| 1. Additive module descriptor | new reviewed descriptor under contract/docs location; docs/tests | low: wrong canonical identity | remove unconsumed descriptor commit | descriptor schema/fixture test; `make health` | exact `module.v1` schema and registry IDs |
| 2. Explicit producer mapping | descriptor/constants/docs; no renames | medium: conflating identities | retain current constants/fields; disable new mapping emission | table-driven identity tests across three seams | approved producer/gateway IDs |
| 3. Compatibility parser and fixtures | new contract parser + sanitized synthetic fixtures/tests | low runtime if not in writer path | stop calling parser; fixtures remain useful | old v1/v2, dual, malformed, unknown-field tests | exact shared schemas/status mappings |
| 4. Per-artifact manifests | new writer helper; seam finalization; tests | medium: checksum order/path leakage/filename collision | feature flag off; payload artifacts unchanged | recompute hashes; relative-path; no chunk glob collision; failure injection | `artifact_manifest.v1` |
| 5. Versioned run-bundle manifest | shared finalizer/new sidecar; latest optionally links both | medium-high: consumer discovery ambiguity | keep legacy `<run_id>.manifest.json` authoritative; disable new bundle | bundle/member linkage and mixed-reader tests | `run_bundle_manifest.v1` and naming decision |
| 6. Run-record additive migration | `run_record_contract.py`, all seam fixtures/tests | high: broad consumer surface and self-hash ordering | dual fields behind emission flag; legacy shape stays | all current compliance tests + shared fixture validation | shared run-record contract |
| 7. Deprecated aliases | parser/docs/telemetry, not removal | medium: premature warnings or consumer assumptions | cease deprecation marking; keep aliases | old-only and new-only consumer matrix | approved alias/deprecation policy |
| 8. Profile 2 tests | compliance/failure-injection tests, safe fixtures | low production risk | tests can be reverted independently | checksum, redaction, statuses, finalizer failure, three seams | final release candidate |
| 9. Mixed-version observation | operational docs/metrics; no historical rewrite | operational | revert new emission flag while readers stay dual | sample old/current/dual artifacts; latest resolution; CLI exits | release candidate stability window |
| 10. Later alias removal | writers/readers/docs only after separate approval | breaking/high | restore alias emission from retained code/tag | consumer inventory, historical corpus parser, full suite | Matías approval, published major/version policy |

Every stage is a separate reviewable commit/release. `make health`, `make smoke`, focused pytest, and `git diff --check` are minimum gates; papers provider calls and real-corpus ingestion are excluded from routine migration validation.

## 10. Rollback and mixed-version operation

1. **Read old forever, write dual temporarily.** Dispatch old records by existing numeric versions; dispatch new records by explicit shared identifiers. Preserve raw documents and unknown fields.
2. **Never rewrite history.** Old runs retain old filenames, paths, hashes, and known limitations. A catalog may annotate them externally without mutation.
3. **Legacy discovery remains authoritative initially.** Continue writing `<run_id>.manifest.json` and `<operator>.latest.json`. New latest fields may point to new bundle/per-artifact manifests, but old fields remain valid.
4. **Feature-gate new emission, not legacy evidence.** A rollout revert disables new sidecars/aliases while current run record, bundle, latest, chunk/summary/export outputs continue unchanged. Readers remain dual-version so rollback does not strand new history.
5. **Release-candidate changes are isolated.** Pin the shared contract release/fixture set; do not claim compliance for an unpinned candidate. If it changes, update the additive adapter and fixtures, not mature parsers/storage.
6. **Checksum semantics are release-bounded.** Validate new payloads after final writes; report legacy stale/missing hashes as `legacy_unverified`, not corruption, unless byte comparison proves corruption.
7. **Latest recovery is reconstructable.** A recovery tool may scan completed run records/bundles and atomically repoint latest, but must be explicit, dry-runnable, operator-scoped, and separately approved. It must not modify historical payloads.
8. **Consumer rollout order:** compatibility parser → shadow-read validation → per-artifact sidecars → dual bundle/run-record emission → latest links → consumer cutover → observation → separately authorized alias removal.

## 11. Explicit non-changes

The migration must not rewrite chat/PDF parsers, embedding/cache/processed-file logic, Chroma/vector-store internals, chunk ID/content/schema semantics, summary ordering/body semantics, Markdown export bytes/naming, the three operator entrypoints or exit meanings, successful historical artifacts, module-local latest-pointer semantics, or papers’ transitional boundary unless explicitly approved. It must not add a generic producer framework, cross-repository runtime imports, MCP integration, dependency upgrades, production ingestion, history rewrites, aliases removal, broad refactors, or a merge as part of this audit.

## 12. Inspection and validation record

### Material files, symbols, paths

- CLIs: `kb/cli/kb_chat_ingest.py::{_parse_args,main}`, `kb/cli/kb_chat_analyze.py::{_parse_args,main}`, `kb/cli/kb_papers_grobid.py::{_parse_args,main}`.
- Orchestrators: `kb/pipelines/chat_ingest.py::ingest_paths`, `kb/pipelines/chat_analyze.py::{analyze,_latest_chunk_set_path}`, `kb/pipelines/papers_grobid.py::run_pdf`.
- Shared contract: `kb/pipelines/run_record_contract.py::{make_run_id,make_run_record,start_stage,complete_stage,add_output_artifact,attach_exception,_derive_final_status,finalize_and_write_contract_artifacts,_maybe_sha256,_safe_environment,write_json_atomic}` and constants `RUN_RECORD_VERSION`, `PROJECT`, `PRODUCER`, `PRODUCER_VERSION`, `SCHEMA_VERSIONS`, `CONTRACTUAL_STATUSES`.
- Artifact contracts/config: `kb/contracts/chunk_set.py`, `contracts/chunk_set.v1.schema.json`, `kb/config/kb_config.py::KBConfig`.
- Public docs/runbook: `README.md`, `kb_artifact_surface.md`, `kb_entrypoints.md`, `kb_health_contract.md`, `kb_module_note.md`, `docs/modules/kb-module-definition.md`, `runbooks/kb_module_runbook.md`, `KB_STATE.md`, `Makefile`.
- Tests: `tests/test_chat_ingest_smoke.py`, `tests/test_chat_analyze_artifacts.py`, `tests/test_chunk_set_contract.py`, `tests/test_cli_validate_chunk_set.py`, `tests/test_contract_compliance.py`.
- Artifact paths inspected as code/documented contracts: `artifacts/{run_records,manifests,observability,chunk_sets,summaries,exports}`. No local untracked operational payload or secret was inspected for content.

### Commands and outcomes

- Local Write Gate 0B commands (`pwd`, `find .. -name AGENTS.md -print`, Git root/branch/HEAD/status/remotes/default checks): exit 0; repository/root and clean tree confirmed. Environment branch was `work` at baseline HEAD; no remotes were configured in this task checkout. Audit isolated on `audit/knowledge-inspect-profile2`.
- Source inspection used `rg`, `git ls-files`, `nl -ba`, and bounded `sed` ranges: exit 0.
- `make health`: exit 0 (`python3 -m compileall . -q`). Generated Python 3.12 bytecode was removed after the check.
- `make smoke`: exit 2 because the baseline checkout does not include `test_data/2025-06-16.jsonl`; the command still emitted temporary error evidence with `missing_input`, which was inspected only for status and then removed. No provider, embedding, Chroma write, or processed-file mark occurred.
- `python3 -m pytest -q tests/test_contract_compliance.py tests/test_chat_ingest_smoke.py tests/test_chat_analyze_artifacts.py tests/test_chunk_set_contract.py tests/test_cli_validate_chunk_set.py`: exit 0; 16 passed. Pytest temporary fixtures supplied sanitized inputs.
- `git diff --check`: exit 0.
- Expensive real ingestion and papers/GROBID end-to-end: skipped deliberately because they require real provider/corpus/external adapter behavior and are prohibited for this audit.

## 13. Human Gate 1 decisions required

Matías must approve: (1) exact Profile 2 semantics and shared release/schema/fixture commit; (2) canonical repository/module/gateway/manifest producer IDs; (3) whether generic manifest remains legacy bundle and the new bundle filename/version; (4) per-artifact manifest naming and which outputs/side effects qualify; (5) shared stable run/artifact ID algorithms and collision policy; (6) status and error mappings, including papers zero-node success and missing-input duplication; (7) path portability, traceback, and safe-environment redaction policy; (8) checksum finalization/self-reference rules; (9) compatibility aliases and deprecation/observation duration; (10) historical artifact retention/verification policy; (11) latest-pointer evolution/recovery semantics; and (12) producer rollout/merge order.

**Readiness:** the audit packet is ready to drive scoped implementation planning and compatibility fixtures. It is **not schema-emission-ready** until the shared release and Human Gate 1 decisions above are resolved.
