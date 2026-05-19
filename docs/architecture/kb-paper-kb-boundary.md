# KB / paper-kb Boundary

## KB owns

- chunk_set.v1 schema
- run_record and manifest conventions
- contract validation helpers
- generic embedding/vectorstore utilities
- generic artifact surfaces

## KB does not own

- paper-kb FastAPI product API
- paper frontend
- TEI/GROBID domain parsing internals
- paper metadata product model
- legacy paper-kb stores

## Dependency rule

KB must not import paper-kb.

## Public surface

Consumers should rely on:
- schemas
- CLI seams
- emitted artifacts
- validation tools

Consumers should not rely on:
- private parser internals
- vectorstore implementation details
