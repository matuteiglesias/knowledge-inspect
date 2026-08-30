SPEECH_CHUNK_SET ?=
SPEECH_INDEX_ROOT ?= artifacts/speech_indexes
SPEECH_INDEX_DIR ?=
SPEECH_QUERY ?=
SPEECH_TOP_K ?= 5

health:
	python3 -m compileall . -q

# Canonical smoke begins from an approved producer-owned artifact. It must not
# make raw chat/day-file parsing a Knowledge Inspect source-authority seam.
smoke:
	@set -eu; \
	fixture="$(CURDIR)/tests/fixtures/governed_smoke.chunk_set.json"; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	python3 -m kb.cli.kb_validate_chunk_set "$$fixture" --format text; \
	KB_ROOT="$$tmp" python3 -m kb.cli.kb_chat_analyze --chunk-set "$$fixture" --export-name smoke.md

inspect-last:
	ls -lt artifacts/run_records artifacts/chunk_sets artifacts/exports | head -40

# Provider-independent W3 semantic-runtime regression proof. The raw-chat
# compatibility tests remain here only to prove representation/cache/index invariants;
# they do not establish source authority.
verify-semantic-runtime:
	python3 -m unittest -v \
		tests.test_semantic_representation_identity \
		tests.test_chat_ingest_smoke \
		tests.test_vectorstore_rebuildability \
		tests.test_semantic_run_evidence \
		tests.test_chat_analyze_input_discovery

# Reproducible Task 2C operator evidence. The expected nonzero verifier results
# are asserted rather than hidden, and the fixture-tree digest proves no mutation.
verify-run-evidence-demo:
	@set -eu; \
	root="$(CURDIR)/tests/fixtures/run_evidence_demo"; \
	tree_hash() { find "$$root" -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | cut -d' ' -f1; }; \
	before="$$(tree_hash)"; echo "BEFORE_SHA256=$$before"; \
	run_case() { \
		run_id="$$1"; expected="$$2"; shift 2; \
		echo "REQUEST: KB_ROOT=tests/fixtures/run_evidence_demo python3 -m kb.cli.kb_verify_run $$run_id $$*"; \
		set +e; response="$$(KB_ROOT="$$root" python3 -m kb.cli.kb_verify_run "$$run_id" "$$@")"; actual="$$?"; set -e; \
		echo "RESPONSE: $$response"; echo "EXIT_CODE=$$actual"; \
		test "$$actual" -eq "$$expected"; \
	}; \
	run_case demo_valid 0 --operator kb.demo; \
	run_case demo_mismatch 4 --operator kb.demo; \
	run_case demo_legacy_write_failure 3 --operator kb.demo_legacy; \
	after="$$(tree_hash)"; echo "AFTER_SHA256=$$after"; test "$$before" = "$$after"; \
	echo "NO_MUTATION=PASS"

# S3 adds only the query semantics pulled by the Speech Atlas consumer: a
# deterministic lexical index plus query/top-k. Producer speech/capture/chunk IDs
# are read-only evidence and remain owned by politics-wiki.
verify-speech-consumer:
	python3 -m unittest -v tests.test_speech_consumer_s3

speech-index:
	@test -n "$(SPEECH_CHUNK_SET)" || (echo "SPEECH_CHUNK_SET is required" >&2; exit 2)
	python3 -m kb.cli.kb_speech_inspect index \
	  --chunk-set "$(SPEECH_CHUNK_SET)" \
	  --index-root "$(SPEECH_INDEX_ROOT)"

speech-query:
	@test -n "$(SPEECH_INDEX_DIR)" || (echo "SPEECH_INDEX_DIR is required" >&2; exit 2)
	@test -n "$(SPEECH_QUERY)" || (echo "SPEECH_QUERY is required" >&2; exit 2)
	python3 -m kb.cli.kb_speech_inspect query \
	  --index-dir "$(SPEECH_INDEX_DIR)" \
	  --query "$(SPEECH_QUERY)" \
	  --top-k $(SPEECH_TOP_K)
