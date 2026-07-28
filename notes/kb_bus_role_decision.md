# KB Bus Role Decision (Source of Truth)

## `kb_chat_ingest`
- role: `consumer + producer`
- canonical input contract: document-like source input
- canonical output bus/family: Chunk Bus compatible `chunk_set` artifact (`artifact_family=chunk_bus`)
- internal non-contract side effects: Chroma writes, SQLite cache, processed-files state
- promotion status: `active`

## `kb_chat_analyze`
- role: `consumer + producer`
- canonical input contract: chunk-set or collection-derived input
- canonical output bus/family: Summary Bus compatible `chunk_set_summary` artifact (`artifact_family=summary_bus`)
- internal non-contract side effects: clustering/order mechanics
- promotion status: `active`

## `kb_papers_grobid`
- role: `adapter`
- canonical input contract: external PDF + GROBID runtime
- canonical output bus/family: adapter-normalized parse artifact
- target future output: Chunk Bus compatible `chunk_set` artifact
- promotion status: `transitional`
