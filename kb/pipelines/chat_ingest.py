"""
pipelines/chat_ingest.py

Purpose
- Ingest one or more chat-export JSONL files into a Chroma collection.
- Enforce idempotency at the *file* level via processed_files table.
- Use SQLite vec cache to avoid re-embedding already-seen nodes.
- Emit a run_record.json artifact for each run.

This pipeline assumes the core modules exist:
  kb.config.kb_config
  kb.storage.sqlite_cache
  kb.storage.processed_files
  kb.parsers.chat_jsonl
  kb.vectorstore.chroma_client
  kb.vectorstore.chroma_io

No refactors: this is a stable integration seam you can execute against.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import json
import os
import time
import traceback
import datetime as dt

import numpy as np

from kb.config.kb_config import KBConfig, load_config
from kb.storage.sqlite_cache import SQLiteVecCache
from kb.storage.processed_files import ProcessedFiles
from kb.parsers.chat_jsonl import jsonl_to_document, parse_markdown_nodes, filter_substantive_nodes, node_id_from_node_text
from kb.vectorstore.chroma_client import ChromaConfig, get_collection
from kb.vectorstore.chroma_io import add_nodes


def _utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _make_run_id(prefix: str = "kb_chat_ingest") -> str:
    return f"{prefix}_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _write_contract_artifacts(cfg: KBConfig, run_record: Dict[str, Any], rr_path: Path) -> Dict[str, str]:
    manifest_dir = cfg.artifacts_dir / "manifests"
    observability_dir = cfg.artifacts_dir / "observability"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    observability_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = manifest_dir / f"{run_record['run_id']}.manifest.json"
    manifest = {
        "manifest_version": 1,
        "run_id": run_record["run_id"],
        "operator": run_record["operator"],
        "status": run_record["status"],
        "artifacts": {
            "run_record": str(rr_path),
            "public_outputs": run_record.get("outputs", {}),
        },
    }
    _write_json_atomic(manifest_path, manifest)

    latest_path = observability_dir / f"{run_record['operator']}.latest.json"
    _write_json_atomic(latest_path, {
        "run_id": run_record["run_id"],
        "operator": run_record["operator"],
        "status": run_record["status"],
        "started_at": run_record.get("started_at"),
        "finished_at": run_record.get("finished_at"),
        "run_record_path": str(rr_path),
        "manifest_path": str(manifest_path),
    })

    return {"manifest_path": str(manifest_path), "observability_latest_path": str(latest_path)}


def _make_embed_fn(cfg: KBConfig):
    """
    Returns a function embed(text:str)->np.ndarray[float32].

    Supported providers:
      - jina: llama_index.embeddings.jinaai.JinaEmbedding
      - openai: llama_index.embeddings.openai.OpenAIEmbedding

    Note: keys are read from env only.
    """
    prov = (cfg.embed_provider or "").strip().lower()
    if prov == "jina":
        from llama_index.embeddings.jinaai import JinaEmbedding
        api_key = os.environ.get(cfg.jina_api_key_env, "")
        if not api_key:
            raise RuntimeError(f"Missing {cfg.jina_api_key_env} in environment for embed_provider=jina")
        emb = JinaEmbedding(api_key=api_key, model=cfg.embed_model, task=cfg.embed_task or "retrieval.passage")
        return lambda text: np.asarray(emb.get_text_embedding(text), dtype=np.float32)

    if prov == "openai":
        from llama_index.embeddings.openai import OpenAIEmbedding
        api_key = os.environ.get(cfg.openai_api_key_env, "")
        if not api_key:
            raise RuntimeError(f"Missing {cfg.openai_api_key_env} in environment for embed_provider=openai")
        # OpenAIEmbedding uses OPENAI_API_KEY env internally, but we also check explicitly above.
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
    dry_run: bool = False,
    batch_size: int = 128,
) -> IngestResult:
    """
    Ingest a list of chat JSONL files.

    Idempotency model (deliberate):
      - processed_files marks per-filename completion (after all nodes attempted).
      - embeddings are cached per node_id, so re-runs are cheap even if file mark is reset.

    Returns: IngestResult containing run_record dict and path.
    """
    cfg = cfg or load_config()
    cfg.ensure_dirs()

    run_id = _make_run_id()
    started_at = _utc_now_iso()

    run_record: Dict[str, Any] = {
        "run_id": run_id,
        "operator": "kb.chat_ingest",
        "started_at": started_at,
        "finished_at": None,
        "status": "running",
        "config": {
            "kb_root": str(cfg.kb_root),
            "cache_db": str(cfg.cache_db),
            "chroma_dir": str(cfg.chroma_dir),
            "collection": cfg.collection_name,
            "embed_provider": cfg.embed_provider,
            "embed_model": cfg.embed_model,
            "embed_task": cfg.embed_task,
            "embed_dim": cfg.embed_dim,
            "reset_collection": bool(reset_collection),
            "dry_run": bool(dry_run),
            "batch_size": int(batch_size),
        },
        "inputs": {"paths": [str(Path(p)) for p in paths]},
        "outputs": {},
        "stats": {
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
        "errors": [],
    }

    rr_path = cfg.run_records_dir / f"{run_id}.run_record.json"

    vec_cache = None
    pf = None
    client = None

    try:
        embed_fn = _make_embed_fn(cfg)
        vec_cache = SQLiteVecCache.open(cfg.cache_db)
        cached_embed = vec_cache.cached_embedder(embed_fn, expected_dim=cfg.embed_dim)

        pf = ProcessedFiles.open(cfg.cache_db)

        chroma_cfg = ChromaConfig(chroma_dir=cfg.chroma_dir, collection_name=cfg.collection_name, allow_reset=...)
        client, coll = get_collection(chroma_cfg, reset=bool(reset_collection))

        # Ingest loop
        ids_batch: List[str] = []
        docs_batch: List[str] = []
        embs_batch: List[np.ndarray] = []
        metas_batch: List[Dict[str, Any]] = []

        def flush_batch():
            if not ids_batch:
                return
            if dry_run:
                # pretend we added them
                run_record["stats"]["chroma_attempted"] += len(ids_batch)
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
            run_record["stats"]["chroma_attempted"] += int(res.attempted)
            run_record["stats"]["chroma_added"] += int(res.added)
            run_record["stats"]["chroma_skipped_existing"] += int(res.skipped_existing)
            run_record["stats"]["chroma_errors"] += int(res.errors)

            ids_batch.clear(); docs_batch.clear(); embs_batch.clear(); metas_batch.clear()

        for p in paths:
            p = Path(p).expanduser()
            run_record["stats"]["files_seen"] += 1

            if not p.exists():
                run_record["errors"].append({"type": "missing_input", "path": str(p)})
                continue

            if pf.is_processed(p.name):
                run_record["stats"]["files_skipped_processed"] += 1
                continue

            doc = jsonl_to_document(p)
            nodes = parse_markdown_nodes(doc, include_metadata=True)
            run_record["stats"]["nodes_parsed_total"] += int(len(nodes))

            nodes = filter_substantive_nodes(nodes, min_newlines=1)
            run_record["stats"]["nodes_kept"] += int(len(nodes))

            for n in nodes:
                text = getattr(n, "text", "") or ""
                header_path = None
                try:
                    # llama_index markdown parser typically stores header_path in metadata when include_metadata=True
                    header_path = (n.metadata or {}).get("header_path")
                except Exception:
                    header_path = None
                header_path_str = "/".join(header_path) if isinstance(header_path, list) else (str(header_path) if header_path else "")
                uid = node_id_from_node_text(text, source_file=p.name, header_path=header_path_str)

                # Embed via cache (idempotent)
                vec = cached_embed(uid, getattr(n, "text", "") or "")

                ids_batch.append(uid)
                docs_batch.append(text)
                embs_batch.append(vec)
                metas_batch.append({
                    "source_file": p.name,
                    "date": (doc.metadata or {}).get("date", p.stem),
                    "header_path": header_path,
                })

                if len(ids_batch) >= int(batch_size):
                    flush_batch()

            # flush at end of file (keeps boundaries stable)
            flush_batch()

            if not dry_run:
                pf.mark_processed(p.name)
            run_record["stats"]["files_processed"] += 1

        run_record["outputs"] = {
            "chroma_dir": str(cfg.chroma_dir),
            "collection": cfg.collection_name,
            "run_record_path": str(rr_path),
        }
        run_record["status"] = "ok"
        run_record["finished_at"] = _utc_now_iso()

    except Exception as e:
        run_record["status"] = "error"
        run_record["finished_at"] = _utc_now_iso()
        run_record["errors"].append({
            "type": "exception",
            "message": str(e),
            "traceback": traceback.format_exc(),
        })

    finally:
        run_record["outputs"]["run_record_path"] = str(rr_path)
        contract_outputs = _write_contract_artifacts(cfg, run_record, rr_path)
        run_record["outputs"].update(contract_outputs)
        try:
            _write_json_atomic(rr_path, run_record)
        except Exception:
            pass

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
        try:
            # chroma client doesn't always need explicit close
            client = None
        except Exception:
            pass

    return IngestResult(run_record_path=rr_path, run_record=run_record)
