"""
chroma_io.py

Batch I/O helpers for Chroma collections.

Your drafts had:
- idempotent add with IDAlreadyExists handling fileciteturn2file3L8-L15
- batched get with offset/limit to load all embeddings fileciteturn2file7L19-L49

This module standardizes both patterns.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import chromadb


@dataclass(frozen=True)
class ChromaAddResult:
    attempted: int
    added: int
    skipped_existing: int
    errors: int


def add_nodes(
    coll,
    *,
    ids: Sequence[str],
    embeddings: Sequence[Sequence[float] | np.ndarray],
    documents: Sequence[str],
    metadatas: Optional[Sequence[Dict[str, Any]]] = None,
    idempotent: bool = True,
) -> ChromaAddResult:
    """
    Add records to a Chroma collection.

    Strategy:
    - If idempotent: attempt bulk add; if it fails due to existing IDs,
      fall back to per-id add and count skips.
    """
    attempted = len(ids)
    if not (len(embeddings) == len(documents) == attempted):
        raise ValueError("ids/embeddings/documents length mismatch")
    if metadatas is not None and len(metadatas) != attempted:
        raise ValueError("metadatas length mismatch")

    # Normalize embeddings to list[ list[float] ]
    emb_list: List[List[float]] = []
    for e in embeddings:
        arr = np.asarray(e, dtype=np.float32).reshape((-1,))
        emb_list.append(arr.tolist())

    added = 0
    skipped = 0
    errors = 0

    try:
        coll.add(ids=list(ids), embeddings=emb_list, documents=list(documents), metadatas=list(metadatas) if metadatas is not None else None)
        return ChromaAddResult(attempted=attempted, added=attempted, skipped_existing=0, errors=0)
    except Exception:
        if not idempotent:
            raise

    # Per-item fallback (keeps idempotency)
    for i, uid in enumerate(ids):
        try:
            coll.add(
                ids=[uid],
                embeddings=[emb_list[i]],
                documents=[documents[i]],
                metadatas=[metadatas[i]] if metadatas is not None else None,
            )
            added += 1
        except chromadb.errors.IDAlreadyExistsError:
            skipped += 1
        except Exception:
            errors += 1

    return ChromaAddResult(attempted=attempted, added=added, skipped_existing=skipped, errors=errors)


def get_all_batched(
    coll,
    *,
    include: Sequence[str] = ("embeddings", "documents", "metadatas"),
    batch_size: int = 500,
) -> Dict[str, Any]:
    """
    Fetch *all* records from collection using offset/limit batching.
    Mirrors your dev8 approach fileciteturn2file7L19-L49, but returns raw Chroma dict.
    """
    out_ids: List[str] = []
    out_docs: List[str] = []
    out_embs: List[Any] = []
    out_metas: List[Dict[str, Any]] = []

    offset = 0
    while True:
        data = coll.get(limit=batch_size, offset=offset, include=list(include))
        ids = data.get("ids") or []
        docs = data.get("documents") or []
        if not ids or not docs:
            break

        out_ids.extend(ids)
        if "documents" in include:
            out_docs.extend(docs)
        if "embeddings" in include:
            out_embs.extend(data.get("embeddings") or [])
        if "metadatas" in include:
            out_metas.extend(data.get("metadatas") or [])

        offset += batch_size

    res: Dict[str, Any] = {"ids": out_ids}
    if "documents" in include:
        res["documents"] = out_docs
    if "embeddings" in include:
        res["embeddings"] = out_embs
    if "metadatas" in include:
        res["metadatas"] = out_metas
    return res


def load_vectors_and_min_nodes(
    coll,
    *,
    batch_size: int = 500,
    header_path_key: str = "header_path",
) -> Tuple[np.ndarray, list]:
    """
    Convenience: return (vecs, nodes) where nodes are minimal objects compatible with your clustering drafts.
    Similar to dev8.SimpleNamespace wrapper fileciteturn2file7L39-L45.
    """
    from types import SimpleNamespace

    data = get_all_batched(coll, include=("embeddings", "documents", "metadatas"), batch_size=batch_size)
    embs = np.asarray(data.get("embeddings") or [], dtype=np.float32)
    nodes = []
    for doc, meta in zip(data.get("documents") or [], data.get("metadatas") or []):
        nodes.append(SimpleNamespace(text=doc, metadata={header_path_key: meta.get(header_path_key)}))
    return embs, nodes
