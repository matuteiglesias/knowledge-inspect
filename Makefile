health:
	python3 -m compileall . -q

smoke:
	python3 -m kb.cli.kb_chat_ingest --paths tests/fixtures/smoke_chat.jsonl --smoke

inspect-last:
	ls -lt artifacts/run_records artifacts/chunk_sets artifacts/exports | head -40
