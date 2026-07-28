# Canonical Entrypoints

## Operator-facing canonical commands (one per seam)
- `python -m kb.cli.kb_chat_ingest --paths <file1.jsonl> [<file2.jsonl> ...]`
- `python -m kb.cli.kb_chat_analyze --export-name combined_notes.md`
- `python -m kb.cli.kb_papers_grobid <paper.pdf> [--save-tei <path>]`

## Command status
- These three are the only canonical operator-facing seams.
- Any direct `kb.pipelines.*` invocation is implementation-level and non-canonical for ops docs.
- No duplicate aliases are documented as official.

## Debug/dev-only options
- `kb_chat_ingest`: `--smoke` (canonical cheap smoke), `--dry-run` (dev parse+embed), `--reset-collection`, `--batch-size`
- `kb_chat_analyze`: `--batch-size`, `--max-nodes`
- `kb_papers_grobid`: `--no-post`, `--chroma-dir`, `--langchain`

## Producer diagnostic

- `python -m kb.cli.kb_verify_run <run_id> [--operator <identity>]` is the bounded,
  offline, read-only verifier for existing producer evidence. It is a diagnostic
  consumer, not a fourth product-producing seam.
