# kb/vectorstore/chroma_client.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import chromadb
from chromadb.config import Settings


@dataclass(frozen=True)
class ChromaConfig:
    chroma_dir: Path
    collection_name: str
    allow_reset: bool = False
    mode: str = "persistent"  # "persistent" or "ephemeral"


def _make_settings(cfg: ChromaConfig) -> Settings:
    # Do NOT pass None. Some chromadb versions assume settings is an object.
    # Keep it minimal and explicit.
    return Settings(
        allow_reset=bool(cfg.allow_reset),
        anonymized_telemetry=False,
    )


def _make_client(cfg: ChromaConfig):
    settings = _make_settings(cfg)

    mode = (cfg.mode or "persistent").strip().lower()
    if mode == "ephemeral":
        return chromadb.EphemeralClient(settings=settings)

    # persistent (default)
    path = str(Path(cfg.chroma_dir).expanduser())
    return chromadb.PersistentClient(path=path, settings=settings)


def _collection_names(client: Any) -> set[str]:
    names: set[str] = set()
    for item in client.list_collections():
        name = getattr(item, "name", None)
        if name is None and isinstance(item, str):
            name = item
        if name:
            names.add(str(name))
    return names


def get_collection(
    cfg: ChromaConfig,
    *,
    embedding_function: Optional[Any] = None,
    metadata: Optional[Dict[str, Any]] = None,
    reset: bool = False,
) -> Tuple[Any, Any]:
    """Return ``(client, collection)`` for one private derivative collection.

    ``reset=True`` is intentionally collection-scoped.  The old implementation
    called ``client.reset()``, which cleared every collection in the Chroma
    client even though the CLI contract says "reset collection".  W3 keeps
    unrelated derivative representations intact.
    """
    client = _make_client(cfg)

    if reset:
        if not cfg.allow_reset:
            raise ValueError("reset=True requested but allow_reset=False in ChromaConfig")
        if cfg.collection_name in _collection_names(client):
            client.delete_collection(name=cfg.collection_name)

    coll = client.get_or_create_collection(
        name=cfg.collection_name,
        embedding_function=embedding_function,
        metadata=metadata,
    )
    return client, coll
