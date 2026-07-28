health:
	python3 -m compileall . -q

smoke:
	python3 -m kb.cli.kb_chat_ingest --paths tests/fixtures/smoke_chat.jsonl --smoke

inspect-last:
	ls -lt artifacts/run_records artifacts/chunk_sets artifacts/exports | head -40

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
