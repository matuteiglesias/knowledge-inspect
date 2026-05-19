
# from pipeline.parsers.canonicalize import canonical_meta_from_chunk

from typing import Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# # storage (backend.app.chunks_fs, backend.app.papers_fs)

# Break shared/chroma_helpers.py into these modules (or keep file but split logically with clear regions):

# shared/chroma_client.py

# def make_client(persist_dir: Optional[Path]) -> Any

# def maybe_persist(client) -> bool


# -----------------------
# Client creation & lifecycle
# -----------------------

def make_chroma_client(persist_directory: Optional[Path] = None, create_dir: bool = True):
    """
    Create a chromadb client, preferring the new PersistentClient API when available.
    If new API not available, try Settings(...) -> chromadb.Client(settings=...).
    Otherwise fall back to chromadb.Client().

    Returns the client object and sets attributes on it:
      - _chroma_persist_mode: "persistent" | "ephemeral" | "legacy"
      - _persist_directory: str or None
    """
        
    import chromadb
    from chromadb.config import Settings

    # try PersistentClient first

    if chromadb is None:
        raise RuntimeError("chromadb package not importable in this environment")

    pd = Path(persist_directory) if persist_directory else None
    if pd and create_dir:
        pd.mkdir(parents=True, exist_ok=True)

    # 1) New API: PersistentClient / EphemeralClient / HttpClient
    try:
        if hasattr(chromadb, "PersistentClient"):
            # Use PersistentClient(path=...)
            if pd:
                client = chromadb.PersistentClient(path=str(pd))
                setattr(client, "_chroma_persist_mode", "persistent")
                setattr(client, "_persist_directory", str(pd))
                logger.debug("Created chromadb.PersistentClient(path=%s)", pd)
                return client
            else:
                # If no persist dir requested, use EphemeralClient
                client = chromadb.EphemeralClient()
                setattr(client, "_chroma_persist_mode", "ephemeral")
                setattr(client, "_persist_directory", None)
                logger.debug("Created chromadb.EphemeralClient() (no persist directory requested)")
                return client
    except Exception as e:
        logger.debug("no (pd). persist directory requested?")
        logger.debug("New PersistentClient API available but creation failed: %s", e)



    # # 2) Mid-era API: Settings(...) -> chromadb.Client(settings=Settings(...))
    # try:
    #     if Settings is not None:
    #         try:
    #             kwargs = {
    #                 "chroma_db_impl": "duckdb+parquet",
    #                 "anonymized_telemetry": False,
    #             }
    #             if pd:
    #                 kwargs["persist_directory"] = str(pd)
    #             settings = Settings(**kwargs)
    #             client = chromadb.Client(settings=settings)
    #             # mark legacy-style but persistent
    #             setattr(client, "_chroma_persist_mode", "legacy")
    #             setattr(client, "_persist_directory", str(pd) if pd else None)
    #             logger.debug("Created chromadb.Client(settings=Settings(...)) with persist_directory=%s", kwargs.get("persist_directory"))
    #             return client
    #         except ValueError as ve:
    #             # library explicitly complains about legacy settings/migration -> fall through
    #             logger.info("Chroma Settings(...) rejected: %s", ve)
    #         except Exception as e:
    #             logger.debug("chromadb.Client(Settings(...)) attempt failed: %s", e)
    # except Exception:
    #     pass

    # # 3) Fallback: plain chromadb.Client() (older versions)
    # try:
    #     client = chromadb.Client()
    #     # assume ephemeral if Client() has no persist
    #     # but set marker so caller can check
    #     setattr(client, "_chroma_persist_mode", "fallback")
    #     # try to discover a persist path on some versions
    #     pd_attr = getattr(client, "_persist_directory", None)
    #     setattr(client, "_persist_directory", pd_attr)
    #     logger.debug("Created fallback chromadb.Client() (mode=fallback)")
    #     return client
    
    # except Exception as e:
    #     logger.exception("Failed to create any chroma client: %s", e)
    #     raise


# shared/chroma_helpers.py (or shared/chroma_client.py)
from pathlib import Path
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

_GLOBAL_CHROMA_CLIENTS: Dict[str, Any] = {}

def _canonical_dir_key(persist_directory: Optional[Path]) -> str:
    if persist_directory:
        return str(Path(persist_directory).expanduser().resolve())
    return "__ephemeral__"

def get_client(persist_directory: Optional[Path] = None, create_if_missing: bool = True) -> Any:
    """
    Return a durable singleton chroma client per persist_directory (or an in-memory client for None).

    - Tries to construct a client in a few compatible ways to handle different chromadb versions.
    - Stores a module-level client per directory key so callers can safely call get_client() repeatedly.
    - Does NOT call persist() — use maybe_persist(client) when you want to persist.
    """
    key = _canonical_dir_key(persist_directory)
    if key in _GLOBAL_CHROMA_CLIENTS:
        return _GLOBAL_CHROMA_CLIENTS[key]

    # lazy import to not fail module import if chromadb missing in some contexts
    try:
        import chromadb
    except Exception as e:
        logger.exception("chromadb import failed: %s", e)
        raise

    client = None
    # prefer explicit Settings-based constructor if present
    try:
        # new-style API: chromadb.config.Settings exists in newer releases
        from chromadb.config import Settings  # type: ignore
        settings_kwargs = {}
        if persist_directory:
            settings_kwargs["persist_directory"] = str(Path(persist_directory).expanduser().resolve())
        client = chromadb.Client(Settings(**settings_kwargs))
        logger.debug("constructed chroma.Client(Settings(...)) for %s", key)
    except Exception:
        # fallback attempts: PersistentClient / EphemeralClient / Client()
        try:
            # some builds expose PersistentClient/EphemeralClient constructors
            # if persist_directory and hasattr(chromadb, "PersistentClient"):
            client = chromadb.PersistentClient(path=str(Path(persist_directory).expanduser().resolve()))
            logger.debug("constructed chroma.PersistentClient for %s", key)

            # elif not persist_directory and hasattr(chromadb, "EphemeralClient"):
            #     client = chromadb.EphemeralClient()
            #     logger.debug("constructed chroma.EphemeralClient (in-memory)")
            # else:
            #     # last resort: default Client()
            #     client = chromadb.Client()
            #     logger.debug("constructed chroma.Client() fallback for %s", key)
        except Exception as e2:
            logger.exception("failed to instantiate a chroma client: %s", e2)
            raise

    # optionally create the persist dir if the caller asked and the client uses a file path
    if persist_directory and create_if_missing:
        try:
            Path(persist_directory).expanduser().mkdir(parents=True, exist_ok=True)
        except Exception:
            # non-fatal: client creation might still succeed
            logger.debug("could not ensure persist_directory exists: %s", persist_directory, exc_info=True)

    _GLOBAL_CHROMA_CLIENTS[key] = client
    return client



def maybe_persist(client) -> bool:
    """
    Try to call client.persist() if available. Returns True if persisted.
    """
    if client is None:
        return False
    try:
        # new/normal API
        if hasattr(client, "persist") and callable(getattr(client, "persist")):
            client.persist()
            return True
        # older builds might have a different name - try common alternatives
        for attr in ("maybe_persist", "save", "flush"):
            if hasattr(client, attr) and callable(getattr(client, attr)):
                getattr(client, attr)()
                return True
    except Exception:
        logger.debug("maybe_persist: persist call failed", exc_info=True)
    return False

def close_client():
    """Persist + close the module global client and reset it."""
    global _GLOBAL_CHROMA_CLIENT
    try:
        if _GLOBAL_CHROMA_CLIENT is not None:
            try:
                maybe_persist(_GLOBAL_CHROMA_CLIENT)
            except Exception:
                logger.debug("persist failed during close_client")
            if hasattr(_GLOBAL_CHROMA_CLIENT, "close"):
                try:
                    _GLOBAL_CHROMA_CLIENT.close()
                except Exception:
                    logger.debug("close() failed on chroma client")
    finally:
        _GLOBAL_CHROMA_CLIENT = None

