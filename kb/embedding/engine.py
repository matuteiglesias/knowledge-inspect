# pipeline/embedding/engine.py
"""
Embedding engine (finalized)

Responsibilities:
- Provide a single canonical embedding entrypoint: embed_records(models: List[CanonicalChunk]) -> List[List[float]]
- Provide text-based convenience wrappers: embed_text_batch, embed_text
- Provide ingest_files_to_chroma which reads chunk JSONL and upserts to Chroma using embed_records
- Handle caching (EmbeddingCache), adapter batch vs single-call differences, and normalization

Notes:
- Adapters implement a low-level signature adapter(text_id, text) -> Iterable[float] and optionally adapter.batch(ids, texts)
- This module hides those differences and returns strict List[List[float]] with float values
"""
from pathlib import Path
import logging
import json
import numbers
from typing import List, Optional, Callable, Iterable, Any, Dict

# allow running from repo root
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parents[2]))

from kb.embedding.adapters import _build_default_adapter
from kb.embedding.cache import EmbeddingCache
from shared.chroma_helpers import sanitize_meta_for_chroma
from shared.config import EMBED_DIM, EMBED_CACHE_DB
# canonical model
from backend.app.schemas import CanonicalChunk

# chunk file reader
from backend.app.chunks_fs import read_chunks_jsonl

logger = logging.getLogger("embedding")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


# -------------------------
# Utilities
# -------------------------
def _normalize_cached_embedding(cval: Any, expected_dim: Optional[int] = None) -> Optional[List[float]]:
    """
    Convert a cached embedding value into List[float] or return None if invalid.
    Accepts: list/tuple of numbers, numpy arrays, bytes (json list)
    """
    if cval is None:
        return None

    try:
        import numpy as _np
        if isinstance(cval, _np.ndarray):
            vec = cval.tolist()
            if expected_dim is not None and len(vec) != expected_dim:
                return None
            return [float(x) for x in vec]
    except Exception:
        pass

    if isinstance(cval, (list, tuple)):
        if all(isinstance(x, numbers.Real) for x in cval):
            if expected_dim is not None and len(cval) != expected_dim:
                return None
            return [float(x) for x in cval]
        return None

    if isinstance(cval, (bytes, bytearray)):
        try:
            s = cval.decode("utf8")
            parsed = json.loads(s)
            if isinstance(parsed, list) and all(isinstance(x, numbers.Real) for x in parsed):
                if expected_dim is not None and len(parsed) != expected_dim:
                    return None
                return [float(x) for x in parsed]
        except Exception:
            return None

    return None

import math

# --- small helpers ---
def normalize_text_field(s: Optional[str]) -> str:
    if s is None:
        return ""
    # Basic normalisation: strip and collapse whitespace. Adjust if you have a shared helper.
    return " ".join(str(s).split())

def _is_finite_number(x: float) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False

def is_valid_embedding(vec: List[float], expected_dim: int) -> bool:
    if not isinstance(vec, (list, tuple)):
        return False
    if len(vec) != expected_dim:
        return False
    # numeric and finite
    for x in vec:
        if not isinstance(x, numbers.Real):
            return False
        if not _is_finite_number(x):
            return False
    return True


def _coerce_to_list_of_float(raw: Any) -> List[float]:
    # Accept numpy arrays, tensors (with .tolist()), lists/tuples
    if raw is None:
        raise ValueError("raw embedding is None")
    if hasattr(raw, "tolist"):
        raw = raw.tolist()
    if not isinstance(raw, (list, tuple)):
        raise TypeError(f"cannot coerce raw embedding of type {type(raw)} to list")
    return [float(x) for x in raw]


# -------------------------
# Adapter wrapper
# -------------------------
def _adapter_wrapper(adapter: Optional[Callable[[str, str], Iterable[float]]]):
    """
    Return a uniform call_one(tid, txt) that returns a list[float].
    Supports adapters with .batch(ids, texts) or .batch_encode(ids, texts).
    """
    if adapter is None:
        adapter = _build_default_adapter()

    def call_one(tid: str, txt: str) -> List[float]:
        # try single-call signatures
        try:
            out = adapter(tid, txt)
            if hasattr(out, "tolist"):
                out = out.tolist()
            return [float(x) for x in out]
        except TypeError:
            # try adapter(text)
            out = adapter(txt)
            if hasattr(out, "tolist"):
                out = out.tolist()
            return [float(x) for x in out]
    return call_one



# -------------------------
# Canonical embedding API
# -------------------------



# 2) Unify embed_records signature & behavior (central API)

# Make embed_records(models: List[CanonicalChunk], *, adapter: EmbeddingAdapterProtocol | None = None, cache: Optional[EmbeddingCache] = None, expected_dim: Optional[int] = None) -> List[List[float]] — keyword-only for non-positional args beyond models.


# pipeline/embedding/engine.py
from typing import List, Optional, Any, Dict, Protocol
from pathlib import Path
import numbers, math, logging, time, traceback

logger = logging.getLogger(__name__)

# --- Config / defaults (adjust to your repo constants) ---
EMBED_DIM = 1536
# EMBED_CACHE_DB and EmbeddingCache exist in your codebase; embed_records assumes cache instance follows the small API used below.

# --- Adapter protocol --- (duck-typed protocol for clarity / typing)
class EmbeddingAdapterProtocol(Protocol):
    dim: Optional[int]  # optional known output dimension
    def embed_batch(self, ids: List[str], texts: List[str]) -> List[List[float]]: ...
    def embed_one(self, id: str, text: str) -> List[float]: ...
    def set_seed(self, seed: int) -> None: ...  # optional

# --- Minimal cache expected interface (duck-typed) ---
# Preferred methods:
#   cache.get_many(ids: List[str]) -> Dict[str, List[float]]
#   cache.set_many(mapping: Dict[str, List[float]]) -> None
# Fallback methods:
#   cache.get(id) -> Optional[List[float]]
#   cache.set(id, vector) -> None

# --- small helpers ---
def normalize_text_field(s: Optional[str]) -> str:
    if s is None:
        return ""
    # Basic normalisation: strip and collapse whitespace. Adjust if you have a shared helper.
    return " ".join(str(s).split())

def _is_finite_number(x: float) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False

def is_valid_embedding(vec: List[float], expected_dim: int) -> bool:
    if not isinstance(vec, (list, tuple)):
        return False
    if len(vec) != expected_dim:
        return False
    # numeric and finite
    for x in vec:
        if not isinstance(x, numbers.Real):
            return False
        if not _is_finite_number(x):
            return False
    return True

def _coerce_to_list_of_float(raw: Any) -> List[float]:
    # Accept numpy arrays, tensors (with .tolist()), lists/tuples
    if raw is None:
        raise ValueError("raw embedding is None")
    if hasattr(raw, "tolist"):
        raw = raw.tolist()
    if not isinstance(raw, (list, tuple)):
        raise TypeError(f"cannot coerce raw embedding of type {type(raw)} to list")
    return [float(x) for x in raw]


# -------------------------
# embed_records (strict)
# -------------------------
def embed_records(
    models: List[Any],  # CanonicalChunk-like objects: must have .chunk_id and .text
    *,
    adapter: Optional[EmbeddingAdapterProtocol] = None,
    cache: Optional[Any] = None,
    expected_dim: Optional[int] = None,
    seed: Optional[int] = None,
    batch_size: int = 512,
) -> List[List[float]]:
    """
    Embed a list of CanonicalChunk-like models.

    Contract / behavior:
      - models: list of objects with .chunk_id and .text (or dict-like with those keys)
      - adapter: must implement embed_batch(ids, texts) and/or embed_one(id, text). If None, _build_default_adapter() is used.
      - cache: optional EmbeddingCache instance (opened by caller). embed_records will NOT open/close caches.
      - expected_dim: explicit expected embedding dimension (falls back to adapter.dim or EMBED_DIM).
      - seed: optional determinism seed; if adapter has set_seed, it will be called.
      - batch_size: chunk size for batch calls.

    Returns:
      - list of embeddings in the same order as models (each is a List[float]).

    Raises:
      - RuntimeError with descriptive message if any embedding is missing or has wrong dim / invalid numbers.
    """
    if not models:
        return []

    # resolve adapter
    if adapter is None:
        adapter = _build_default_adapter()  # assumes this exists in your codebase

    adapter_dim = getattr(adapter, "dim", None)
    expected_dim = int(expected_dim or adapter_dim or EMBED_DIM)

    # build ids/texts
    ids = []
    texts = []
    for idx, m in enumerate(models):
        # support object with attributes or dict-like
        cid = getattr(m, "chunk_id", None) or (m.get("chunk_id") if isinstance(m, dict) else None) or f"c{idx}"
        txt = getattr(m, "text", None) or (m.get("text") if isinstance(m, dict) else None) or ""
        ids.append(str(cid))
        texts.append(normalize_text_field(txt))

    # determinism hook
    if seed is not None and hasattr(adapter, "set_seed"):
        try:
            adapter.set_seed(int(seed))
        except Exception:
            logger.debug("adapter.set_seed failed; continuing without seed")

    # try cache bulk-get if cache provided
    out: List[Optional[List[float]]] = [None] * len(models)
    to_compute_idx: List[int] = []
    if cache is not None:
        try:
            if hasattr(cache, "get_many"):
                cached_map: Dict[str, Any] = cache.get_many(ids) or {}
                for i, cid in enumerate(ids):
                    raw = cached_map.get(cid)
                    if raw is None:
                        to_compute_idx.append(i)
                        continue
                    try:
                        vec = _coerce_to_list_of_float(raw)
                    except Exception:
                        logger.debug("invalid cached vector for %s; will recompute", cid)
                        to_compute_idx.append(i)
                        continue
                    if not is_valid_embedding(vec, expected_dim):
                        logger.debug("cached vector invalid dimension for %s; will recompute", cid)
                        to_compute_idx.append(i)
                        continue
                    out[i] = vec
            else:
                # fallback to per-id cache.get (still only read; cache open/close is caller's responsibility)
                for i, cid in enumerate(ids):
                    try:
                        raw = cache.get(cid)
                    except Exception:
                        raw = None
                    if raw is None:
                        to_compute_idx.append(i)
                        continue
                    try:
                        vec = _coerce_to_list_of_float(raw)
                    except Exception:
                        to_compute_idx.append(i)
                        continue
                    if not is_valid_embedding(vec, expected_dim):
                        to_compute_idx.append(i)
                        continue
                    out[i] = vec
        except Exception as e:
            # cache read should not be fatal — log and compute everything
            logger.warning("embedding cache lookup failed, computing all: %s", e)
            to_compute_idx = list(range(len(models)))
    else:
        to_compute_idx = list(range(len(models)))

    # if nothing to compute, return validated cached list
    if not to_compute_idx:
        # final validation (should be valid by earlier checks)
        return [list(map(float, v)) for v in out]  # type: ignore

    # compute in batches using adapter.embed_batch preferred
    batch_fn = getattr(adapter, "embed_batch", None)
    if batch_fn is None:
        # some legacy adapters call it 'batch' or 'batch_encode'
        batch_fn = getattr(adapter, "batch", None) or getattr(adapter, "batch_encode", None)

    computed_map: Dict[str, List[float]] = {}

    # chunk indices to batches
    for start in range(0, len(to_compute_idx), batch_size):
        slice_idx = to_compute_idx[start : start + batch_size]
        slice_ids = [ids[i] for i in slice_idx]
        slice_texts = [texts[i] for i in slice_idx]

        if batch_fn is not None:
            try:
                vecs = batch_fn(slice_ids, slice_texts)
                if not isinstance(vecs, (list, tuple)) or len(vecs) != len(slice_idx):
                    raise RuntimeError(f"adapter.embed_batch returned {len(vecs) if hasattr(vecs,'__len__') else type(vecs)} for {len(slice_idx)} inputs")
                # coerce and validate each
                for j, i_model in enumerate(slice_idx):
                    raw = vecs[j]
                    vec = _coerce_to_list_of_float(raw)
                    if not is_valid_embedding(vec, expected_dim):
                        raise RuntimeError(f"adapter.batch produced invalid embedding for id={ids[i_model]}: len={len(vec)} expected={expected_dim}")
                    computed_map[ids[i_model]] = vec
                continue  # next batch
            except Exception as e:
                # batch failed: fall back to per-item for this slice
                logger.warning("adapter.batch failed for ids[%d:%d]: %s; falling back to per-item", start, start + len(slice_idx), e)
                # fall through to per-item below for this slice

        # per-item fallback
        for i_model in slice_idx:
            cid = ids[i_model]
            txt = texts[i_model]
            try:
                if hasattr(adapter, "embed_one"):
                    raw = adapter.embed_one(cid, txt)
                else:
                    # allow adapter to be a callable for single-item (legacy)
                    raw = adapter(cid, txt)  # type: ignore
            except Exception as e:
                tb = traceback.format_exc()
                logger.exception("adapter single embed failed for %s: %s", cid, e)
                raise RuntimeError(f"adapter.embed_one failed for id={cid}: {e}\n{tb}")
            vec = _coerce_to_list_of_float(raw)
            if not is_valid_embedding(vec, expected_dim):
                raise RuntimeError(f"adapter.single produced invalid embedding for id={cid}: len={len(vec)} expected={expected_dim}")
            computed_map[cid] = vec

    # merge computed_map into out list
    for i in range(len(out)):
        if out[i] is None:
            cid = ids[i]
            if cid in computed_map:
                out[i] = computed_map[cid]
            else:
                # Should not happen
                raise RuntimeError(f"internal error: missing computed embedding for id={cid}")

    # write computed vectors back to cache in batch (if cache supports set_many)
    if cache is not None:
        to_set = {cid: computed_map[cid] for cid in computed_map.keys()}
        try:
            if hasattr(cache, "set_many"):
                cache.set_many(to_set)
            else:
                for k, v in to_set.items():
                    try:
                        cache.set(k, v)
                    except Exception:
                        logger.debug("cache.set failed for %s", k)
        except Exception as e:
            logger.debug("embedding cache set_many failed: %s", e)

    # final validation and return (coerce to float)
    final: List[List[float]] = []
    for i, v in enumerate(out):
        if v is None:
            raise RuntimeError(f"embedding failed for id={ids[i]} (null vector).")
        if len(v) != expected_dim:
            raise RuntimeError(f"embedding dim mismatch for id={ids[i]}: {len(v)} != {expected_dim}")
        final.append([float(x) for x in v])

    return final
