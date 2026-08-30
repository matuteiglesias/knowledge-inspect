"""
Legacy compatibility raw-chat ingestion pipeline.

Purpose
- Preserve bounded regression/compatibility behavior for historical JSONL chat inputs.
- Exercise representation-aware embedding/cache/vector-store behavior used by W3 proofs.
- Emit contractual run evidence that explicitly marks this source seam as non-authoritative.

This module does **not** establish Knowledge Inspect as owner of raw chat/day-file
source semantics. Sanctioned inspection workflows should begin from producer-owned
governed artifacts.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set
import os

from kb.config.kb_config import KBConfig, load_config
from kb.embedding.representation import EmbeddingRepresentation
from kb.parsers.chat_jsonl import filter_substantive_nodes, jsonl_to_document, node_id_from_node_text, parse_markdown_nodes
from kb.pipelines.run_record_contract import (
    add_output_artifact,
    attach_exception,
    complete_stage,
    finalize_and_write_contract_artifacts,
    make_run_id,
    make_run_record,
    write_json_atomic,
)
from kb.storage.processed_files import ProcessedFiles


LEGACY_SOURCE_STATUS = "legacy_compatibility_non_authoritative"


def _make_embed_fn(cfg: KBConfig):
    prov = (cfg.embed_provider or "").strip().lower()
    if prov == "jina":
        import numpy as np
        from llama_index.embeddings.jinaai import JinaEmbedding

        api_key = os.environ.get(cfg.jina_api_key_env, "")
        if not api_key:
            raise RuntimeError(f"Missing {cfg.jina_api_key_env} in environment for embed_provider=jina")
        emb = JinaEmbedding(api_key=api_key, model=cfg.embed_model, task=cfg.embed_task or "retrieval.passage")
        return lambda text: np.asarray(emb.get_text_embedding(text), dtype=np.float32)

    if prov == "openai":
        import numpy as np
        from llama_index.embeddings.openai import OpenAIEmbedding

        api_key = os.environ.get(cfg.openai_api_key_env, "")
        if not api_key:
            raise RuntimeError(f"Missing {cfg.openai_api_key_env} in environment for embed_provider=openai")
        emb = OpenAIEmbedding(model_name=cfg.embed_model)
        return lambda text: np.asarray(emb.get_text_embedding(text), dtype=np.float32)

    raise RuntimeError(f"Unsupported embed_provider={cfg.embed_provider!r}. Expected 'jina' or 'openai'.")


@dataclass(frozen=True)
class IngestResult:
    run_record_path: Path
    run_record: Dict[str, Any]


def ingest_paths(
    paths: Sequence[Path],
    *,
    cfg: Optional[KBConfig] = None,
    reset_collection: bool = False,
    smoke: bool = False,
    dry_run: bool = False,
    batch_size: int = 128,
) -> IngestResult:
    cfg = cfg or load_config()
    cfg.ensure_dirs()

    representation = EmbeddingRepresentation.from_config(cfg)
    representation_id = representation.representation_id
    resolved_collection = representation.collection_name(cfg.collection_name)

    run_id = make_run_id("kb_chat_ingest")
    rr_path = cfg.run_records_dir / f"{run_id}.run_record.json"

    run_record = make_run_record(
        cfg=cfg,
        run_id=run_id,
        entrypoint="kb_chat_ingest",
        operator="kb.chat_ingest",
        config={
            "cache_db": str(cfg.cache_db),
            "chroma_dir": str(cfg.chroma_dir),
            # Compatibility: keep the operator-facing configured base name.
            "collection": cfg.collection_name,
            "resolved_collection": resolved_collection,
            "embed_provider": cfg.embed_provider,
            "embed_model": cfg.embed_model,
            "embed_task": cfg.embed_task,
            "embed_dim": cfg.embed_dim,
            "embedding_representation_id": representation_id,
            "embedding_representation": representation.evidence(),
            "reset_collection": bool(reset_collection),
            "smoke": bool(smoke),
            "dry_run": bool(dry_run),
            "batch_size": int(batch_size),
            "source_authority_status": LEGACY_SOURCE_STATUS,
        },
        inputs={"items": [{"input_kind": "paths", "paths": [str(Path(p)) for p in paths]}]},
        stage_defs=[
            {"name": "config_load"},
            {"name": "input_resolution"},
            {"name": "parse"},
            {"name": "embed_persist"},
            {"name": "contract_artifact_emission"},
        ],
        counters={
            "files_seen": 0,
            "files_skipped_processed": 0,
            "files_processed": 0,
            "nodes_parsed_total": 0,
            "nodes_kept": 0,
            "chroma_attempted": 0,
            "chroma_added": 0,
            "chroma_skipped_existing": 0,
            "chroma_errors": 0,
        },
    )
    run_record["warnings"].append(
        {
            "type": "legacy_source_seam",
            "status": LEGACY_SOURCE_STATUS,
            "message": (
                "Raw chat/day-file interpretation is retained for compatibility only; "
                "Knowledge Inspect does not claim source authority for this input."
            ),
        }
    )

    vec_cache = None
    pf = None
    client = None
    canonical_chunk_set_path = cfg.chunk_sets_dir / f"{run_id}.chunk_set.json"
    source_items: Set[str] = set()
    canonical_chunks: List[Dict[str, Any]] = []

    try:
        complete_stage(run_record, "config_load", success=True)
        complete_stage(run_record, "input_resolution", success=True, details={"input_count": len(paths)})

        smoke_artifact_path: Optional[Path] = None
        smoke_preview: Dict[str, Any] = {"sample": []}
        run_record["mode"] = "smoke" if smoke else ("dry_run" if dry_run else "ingest")

        cached_embed = None

        if not smoke:
            pf = ProcessedFiles.open(cfg.cache_db)
            from kb.storage.sqlite_cache import SQLiteVecCache
            from kb.vectorstore.chroma_client import ChromaConfig, get_collection
            from kb.vectorstore.chroma_io import add_nodes

            embed_fn = _make_embed_fn(cfg)
            vec_cache = SQLiteVecCache.open(cfg.cache_db)
            cached_embed = vec_cache.cached_embedder(
                embed_fn,
                expected_dim=cfg.embed_dim,
                representation_id=representation_id,
            )

            chroma_cfg = ChromaConfig(
                chroma_dir=cfg.chroma_dir,
                collection_name=resolved_collection,
                allow_reset=bool(reset_collection),
            )
            client, coll = get_collection(
                chroma_cfg,
                reset=bool(reset_collection),
                metadata={
                    "kb_embedding_representation_id": representation_id,
                    "kb_collection_base": cfg.collection_name,
                },
            )

            ids_batch: List[str] = []
            docs_batch: List[str] = []
            embs_batch: List[Any] = []
            metas_batch: List[Dict[str, Any]] = []

            def flush_batch() -> None:
                if not ids_batch:
                    return
                if dry_run:
                    run_record["counters"]["chroma_attempted"] += len(ids_batch)
                    ids_batch.clear(); docs_batch.clear(); embs_batch.clear(); metas_batch.clear()
                    return

                res = add_nodes(
                    coll,
                    ids=ids_batch,
                    embeddings=embs_batch,
                    documents=docs_batch,
                    metadatas=metas_batch,
                    idempotent=True,
                )
                run_record["counters"]["chroma_attempted"] += int(res.attempted)
                run_record["counters"]["chroma_added"] += int(res.added)
                run_record["counters"]["chroma_skipped_existing"] += int(res.skipped_existing)
                run_record["counters"]["chroma_errors"] += int(res.errors)
                ids_batch.clear(); docs_batch.clear(); embs_batch.clear(); metas_batch.clear()

        complete_stage(run_record, "parse", success=True, details={"state": "started"})
        if not smoke:
            complete_stage(run_record, "embed_persist", success=True, details={"state": "started"})

        for p in paths:
            p = Path(p).expanduser()
            run_record["counters"]["files_seen"] += 1

            if not p.exists():
                run_record["errors"].append({"type": "missing_input", "path": str(p)})
                run_record["warnings"].append({"type": "missing_input", "path": str(p)})
                continue

            # Processed-state is private derivative state. Namespace it by the
            # embedding representation and bypass it on explicit rebuild/reset.
            processed_state_key = representation.state_key(f"file:{p.name}")
            if (
                pf is not None
                and not reset_collection
                and pf.is_processed(processed_state_key)
            ):
                run_record["counters"]["files_skipped_processed"] += 1
                continue

            doc = jsonl_to_document(p)
            source_items.add(p.name)
            nodes = parse_markdown_nodes(doc, include_metadata=True)
            run_record["counters"]["nodes_parsed_total"] += int(len(nodes))

            nodes = filter_substantive_nodes(nodes, min_newlines=1)
            run_record["counters"]["nodes_kept"] += int(len(nodes))

            for n in nodes:
                text = getattr(n, "text", "") or ""
                try:
                    header_path = (n.metadata or {}).get("header_path")
                except Exception:
                    header_path = None
                header_path_str = "/".join(header_path) if isinstance(header_path, list) else (str(header_path) if header_path else "")
                uid = node_id_from_node_text(text, source_file=p.name, header_path=header_path_str)
                canonical_chunks.append(
                    {
                        "chunk_id": uid,
                        "chunk_index": len(canonical_chunks),
                        "char_len": len(text),
                        "document_id": p.name,
                        "source_file": p.name,
                        "header_path": header_path,
                        "text": text,
                        "metadata": {
                            "date": (doc.metadata or {}).get("date", p.stem),
                        },
                    }
                )

                if smoke:
                    if len(smoke_preview["sample"]) < 5:
                        smoke_preview["sample"].append({
                            "source_file": p.name,
                            "node_id": uid,
                            "chars": len(text),
                            "header_path": header_path,
                        })
                    continue

                vec = cached_embed(uid, text)
                ids_batch.append(uid)
                docs_batch.append(text)
                embs_batch.append(vec)
                metas_batch.append({
                    "source_file": p.name,
                    "date": (doc.metadata or {}).get("date", p.stem),
                    "header_path": header_path,
                    "embedding_representation_id": representation_id,
                })

                if len(ids_batch) >= int(batch_size):
                    flush_batch()

            if not smoke:
                flush_batch()

            if not dry_run and not smoke:
                pf.mark_processed(processed_state_key)
            run_record["counters"]["files_processed"] += 1

        if smoke:
            smoke_artifact_path = cfg.exports_dir / f"{run_id}.smoke.json"
            smoke_preview["run_id"] = run_id
            smoke_preview["inputs"] = run_record["inputs"]
            smoke_preview["stats"] = {
                "files_seen": run_record["counters"]["files_seen"],
                "files_processed": run_record["counters"]["files_processed"],
                "nodes_parsed_total": run_record["counters"]["nodes_parsed_total"],
                "nodes_kept": run_record["counters"]["nodes_kept"],
            }
            write_json_atomic(smoke_artifact_path, smoke_preview)
            add_output_artifact(
                run_record,
                path=smoke_artifact_path,
                artifact_kind="smoke_preview",
                artifact_family="export",
                schema_version=1,
            )

        chunk_set_artifact = {
            "artifact_family": "chunk_bus",
            "artifact_kind": "chunk_set",
            "schema_version": 1,
            "run_id": run_id,
            "producer": "kb",
            "entrypoint": "kb_chat_ingest",
            "source_items": sorted(source_items),
            "chunks": canonical_chunks,
            "chunk_count": len(canonical_chunks),
        }
        write_json_atomic(canonical_chunk_set_path, chunk_set_artifact)
        add_output_artifact(
            run_record,
            path=canonical_chunk_set_path,
            artifact_kind="chunk_set",
            artifact_family="chunk_bus",
            schema_version=1,
            promotion_status="active",
            extra={
                "is_primary": True,
                "source_authority_status": LEGACY_SOURCE_STATUS,
            },
        )

        run_record["outputs"].update({
            "chunk_set_artifact_path": str(canonical_chunk_set_path),
            "run_record_path": str(rr_path),
        })
        run_record["outputs"]["internal_side_effects"] = {
            "chroma_dir": str(cfg.chroma_dir),
            "collection": cfg.collection_name,
            "resolved_collection": resolved_collection,
            "embedding_representation_id": representation_id,
            "sqlite_cache_db": str(cfg.cache_db),
            "processed_files_state": str(cfg.cache_db),
            "source_authority_status": LEGACY_SOURCE_STATUS,
        }
        if smoke_artifact_path is not None:
            run_record["outputs"]["smoke_artifact_path"] = str(smoke_artifact_path)

        run_record["stats"] = dict(run_record["counters"])

    except Exception as e:
        complete_stage(run_record, "parse", success=False)
        complete_stage(run_record, "embed_persist", success=False)
        attach_exception(run_record, e)

    finally:
        run_record["outputs"]["run_record_path"] = str(rr_path)
        requested_status = "error" if run_record.get("errors") else "success"
        finalize_and_write_contract_artifacts(
            cfg=cfg,
            run_record=run_record,
            rr_path=rr_path,
            requested_status=requested_status,
        )

        try:
            if vec_cache is not None:
                vec_cache.close()
        except Exception:
            pass
        try:
            if pf is not None:
                pf.close()
        except Exception:
            pass
        client = None

    return IngestResult(run_record_path=rr_path, run_record=run_record)
