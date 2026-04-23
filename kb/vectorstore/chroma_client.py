# kb/vectorstore/chroma_client.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple

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


def get_collection(
    cfg: ChromaConfig,
    *,
    embedding_function: Optional[Any] = None,
    reset: bool = False,
) -> Tuple[Any, Any]:
    """
    Returns (client, collection).
    """
    client = _make_client(cfg)

    if reset:
        if not cfg.allow_reset:
            raise ValueError("reset=True requested but allow_reset=False in ChromaConfig")
        # Public API: reset clears all collections in this client
        client.reset()

    coll = client.get_or_create_collection(
        name=cfg.collection_name,
        embedding_function=embedding_function,
    )
    return client, coll
