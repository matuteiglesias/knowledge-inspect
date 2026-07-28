# PR 2 / Task 2A — run-evidence characterization

**Scope:** characterization fixtures and tests only. This packet does not define or
implement a verifier, repair evidence, change a writer, or make a Profile 2 claim.

## Fixture vocabulary

The machine-readable characterization catalog is
`tests/fixtures/run_evidence_states.v1.json`. Its labels deliberately keep these
conditions separate:

- `known_legacy_limitation`: observed behavior that the current writer permits;
- `incomplete_evidence`: the expected evidence graph cannot be fully traversed;
- `structurally_invalid_evidence`: a present member cannot be decoded as its
  expected JSON structure;
- `missing_member`: a referenced or inventoried path is absent;
- `proven_checksum_mismatch`: a present member's bytes differ from its recorded
  SHA-256;
- `stale_latest_pointer`: latest references an absent run record or bundle.

These are fixture expectations, not a public diagnostic schema or verifier API.

## Characterized results

| Case | Current observed behavior | Characterization |
|---|---|---|
| successful run | run record, bundle, and latest all publish `success` | complete evidence |
| empty-success run | a requested success with zero loaded nodes becomes `empty_success` | complete evidence |
| partial-success run | a requested success with nodes and a recorded error becomes `partial_success` | complete evidence |
| error run | an explicitly requested error remains `error` | complete evidence |
| missing payload member | the bundle inventories the path but omits `sha256` | incomplete evidence; missing member |
| altered payload member | recomputing SHA-256 differs from the bundle value | proven checksum mismatch |
| malformed bundle | JSON decoding fails | structurally invalid evidence |
| latest → missing run record | latest remains readable but its run-record target is absent | incomplete evidence; missing member; stale latest pointer |
| latest → missing bundle | latest remains readable but its bundle target is absent | incomplete evidence; missing member; stale latest pointer |
| normal run-record checksum ordering | the run record is written before its bundle checksum is collected, and the recorded digest matches its final bytes | complete evidence |
| run-record write failure | the exception is swallowed; latest and bundle still publish, latest references the absent record, and the bundle run-record entry has no checksum | known legacy limitation; incomplete evidence; missing member; stale latest pointer |

## Audit comparison and limitations

The architecture audit correctly identifies that latest is written before the run
record and bundle, so latest can become stale. It also correctly identifies that a
run-record write exception is swallowed. The new failure-injection fixture pins
both effects without changing them.

The audit says the run-record digest "may" become stale because the run record may
be rewritten after its hash is collected. In the inspected implementation's normal
path, the run record is written once before checksum collection and is not rewritten
afterward; the fixture therefore observes a matching digest. The demonstrated
failure mode is different: when that write fails and no prior record exists, the
bundle omits the run-record digest while still publishing references. A pre-existing
record at the same path could instead be inventoried as stale bytes. Task 2B should
classify only what it can prove from the files it reads and must not generalize the
audit's possible ordering defect into an unconditional mismatch.

Partial success is directly supported by the shared status derivation, but current
seams request `error` whenever their run record already contains errors. The fixture
therefore pins the shared finalizer behavior without claiming that every production
seam currently reaches `partial_success`.

## Closure question

**Do the current fixtures provide enough evidence to define a truthful read-only
verifier?**

Recommended answer: **YES**, subject to human review. The fixtures cover all planned
status states, absent and malformed members, cryptographically proven alteration,
both stale-latest targets, normal checksum ordering, and the swallowed finalizer
write failure. Task 2B should consume the labels in the catalog rather than inventing
broader corruption or repair semantics.
