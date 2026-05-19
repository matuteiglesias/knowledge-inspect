health:
	python3 -m compileall . -q

smoke:
	python3 -m kb.cli.kb_chat_ingest --paths test_data/2025-06-16.jsonl --smoke

inspect-last:
	ls -lt artifacts/run_records artifacts/chunk_sets artifacts/exports | head -40