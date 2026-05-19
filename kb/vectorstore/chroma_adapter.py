# shared/chroma_adapter.py
from typing import Any, List, Dict, Optional, Callable
import logging

logger = logging.getLogger(__name__)


def _default_collection_resolver(client: Any, collection_name: str):
    """
    Best-effort resolver for client + collection_name.
    Tries common helper (get_or_create_collection) if available, else tries client's get_collection / get.
    Caller may pass a custom resolver to adapt to their environment.
    """
    # try common project helper if present
    try:
        # local import to avoid circular imports in the repo
        from .chroma_helpers import get_or_create_collection  # type: ignore
        return get_or_create_collection(client, collection_name)
    except Exception:
        pass

    # try common client methods
    for attr in ("get_collection", "get", "create_collection", "create"):
        fn = getattr(client, attr, None)
        if callable(fn):
            try:
                return fn(collection_name)
            except TypeError:
                # try positional variant
                try:
                    return fn(collection_name)
                except Exception:
                    continue
            except Exception:
                continue
    raise RuntimeError("could not resolve collection from client; provide a custom collection_resolver")


class CollectionAdapter:
    """
    Minimal adapter that normalizes a Chroma 'collection' interface across versions.

    Usage (examples):
        # pass a collection object directly
        adapter = CollectionAdapter(coll)
        adapter.add(ids=[...], documents=[...], embeddings=[...])

        # pass a client + collection_name
        adapter = CollectionAdapter(client, collection_name="chunks")
        adapter.upsert(...)

    Notes:
    - This adapter does *not* sanitize inputs or perform batching. It validates list lengths and raises on misuse.
    - Prefer explicit keyword args when calling adapter methods.
    - If your repo has a custom resolver, pass collection_resolver=your_fn(client, collection_name).
    """

    def __init__(
        self,
        collection_or_client: Any,
        collection_name: Optional[str] = None,
        collection_resolver: Optional[Callable[[Any, str], Any]] = None,
    ):
        if collection_or_client is None:
            raise ValueError("collection_or_client must not be None")

        self._client = None
        self._coll = None
        self._resolver = collection_resolver or _default_collection_resolver

        # heuristics: collection object likely exposes 'add' and 'get'
        if hasattr(collection_or_client, "add") and hasattr(collection_or_client, "get"):
            # treat as collection
            self._coll = collection_or_client
            # try to discover client (optional)
            self._client = getattr(collection_or_client, "_client", None)
        else:
            # treat as client; collection_name required
            self._client = collection_or_client
            if not collection_name:
                raise ValueError("collection_name is required when providing a client")
            self._coll = self._resolver(self._client, collection_name)

    # low-level caller that tries a list of method names on the resolved collection
    def _call(self, candidates: List[str], /, *args, **kwargs):
        last_exc = None
        for name in candidates:
            fn = getattr(self._coll, name, None)
            if not callable(fn):
                continue
            try:
                # try preferred keyword invocation
                return fn(**kwargs)
            except TypeError as te:
                # maybe this version uses positional args — try positional fallback (best-effort)
                try:
                    return fn(*args)
                except Exception as e2:
                    last_exc = e2
                    continue
            except Exception as e:
                last_exc = e
                continue
        # nothing worked
        if last_exc:
            raise last_exc
        raise RuntimeError(f"No supported methods found among: {', '.join(candidates)}")

    @staticmethod
    def _validate_lengths(ids: List[str], documents: Optional[List[str]], embeddings: Optional[List[List[float]]], metadatas: Optional[List[Dict[str, Any]]]):
        n = len(ids)
        for name, lst in (("documents", documents), ("embeddings", embeddings), ("metadatas", metadatas)):
            if lst is not None and len(lst) != n:
                raise ValueError(f"length mismatch: len(ids)={n} but len({name})={len(lst)}")

    # Public API (kept intentionally small)
    def add(self, ids: List[str], documents: Optional[List[str]] = None,
            embeddings: Optional[List[List[float]]] = None, metadatas: Optional[List[Dict[str, Any]]] = None):
        """
        Add new entries. Will raise if lengths mismatch.
        Tries collection methods in this order: 'add', 'insert', 'add_documents', 'add_embeddings'
        """
        if not ids:
            raise ValueError("ids must be non-empty for add()")
        self._validate_lengths(ids, documents, embeddings, metadatas)
        return self._call(["add", "insert", "add_documents", "add_embeddings"], ids, documents, embeddings, metadatas,
                          ids=ids, documents=documents, embeddings=embeddings, metadatas=metadatas)

    def upsert(self, ids: List[str], documents: Optional[List[str]] = None,
               embeddings: Optional[List[List[float]]] = None, metadatas: Optional[List[Dict[str, Any]]] = None):
        """
        Idempotent upsert if supported. Tries: 'upsert' -> 'update' -> 'add'.
        """
        if not ids:
            raise ValueError("ids must be non-empty for upsert()")
        self._validate_lengths(ids, documents, embeddings, metadatas)
        try:
            return self._call(["upsert", "update", "add"], ids, documents, embeddings, metadatas,
                              ids=ids, documents=documents, embeddings=embeddings, metadatas=metadatas)
        except Exception:
            # let the caller see exception if all attempts fail
            raise

    def update(self, *args, **kwargs):
        """Explicit update wrapper; will raise if underlying collection doesn't support 'update'."""
        return self._call(["update", "upsert"], *args, **kwargs)

    def get(self, *args, **kwargs):
        """Unified get/query accessor. Accepts whatever your Chroma version expects (ids=..., where=..., etc)."""
        return self._call(["get", "query", "fetch"], *args, **kwargs)

    def query(self, *args, **kwargs):
        """Alias for get — kept for readability in caller codebases."""
        return self.get(*args, **kwargs)

    def persist(self):
        """
        If a client or collection exposes a persistence hook, call it.
        - Prefer explicit client.persist()
        - Else try collection.maybe_persist()
        - Else try project-level maybe_persist(client) helper if available
        This method is best-effort and will not raise on missing hooks.
        """
        if self._client is not None and hasattr(self._client, "persist") and callable(getattr(self._client, "persist")):
            try:
                self._client.persist()
                return
            except Exception as e:
                logger.debug("client.persist() failed: %s", e)

        if hasattr(self._coll, "persist") and callable(getattr(self._coll, "persist")):
            try:
                self._coll.maybe_persist()
                return
            except Exception as e:
                logger.debug("collection.maybe_persist() failed: %s", e)

        # try project helper maybe_persist
        try:
            from .chroma_helpers import maybe_persist  # type: ignore
            try:
                maybe_persist(self._client)
            except Exception as e:
                logger.debug("maybe_persist(client) failed: %s", e)
        except Exception:
            # no persist helper available; nothing else we can do
            logger.debug("no persist hook available on client/collection; skipping persist()")
            return



# deprec


# shared/chroma_adapters_resolver.py

# from typing import Callable


# def _resolve_upsert_fn(provided: Optional[Callable] = None):
#     if provided:
#         return provided
#     return _default_upsert



# def _resolve_embed_fn(provided: Optional[Callable]):
#     if provided:
#         return provided
#     # preferred embed adapter
#     try:
#         from pipeline.embedding.engine import embed_records as _embed_adapter  # type: ignore
#         return lambda models, embed_fn=None: _embed_adapter(models, embed_fn=embed_fn)
#     except Exception:
#         pass
#     if "cached_embed" in globals():
#         return globals()["cached_embed"]
#     return None


