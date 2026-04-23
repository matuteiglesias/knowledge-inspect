"""
kb_config.py

Single source of truth for paths + embedding/vectorstore defaults.

Design goals:
- No global state leakage. Everything is explicit via `KBConfig`.
- Paths are resolved relative to KB_ROOT unless absolute.
- Secrets stay in env vars (do NOT hardcode API keys).

This file is a cleaned-up consolidation of repeated config fragments in dev notebooks/scripts.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    return v


def _as_path(p: str | None) -> Path | None:
    if p is None:
        return None
    return Path(p).expanduser()


@dataclass(frozen=True)
class KBConfig:
    # Root
    kb_root: Path

    # Chat exports / test data
    chat_jsonl_dir: Path

    # Embedding cache
    cache_db: Path

    # Vector store (Chroma)
    chroma_dir: Path
    collection_name: str

    # Embeddings
    embed_provider: str  # "jina" | "openai" | "other"
    embed_model: str
    embed_task: str | None
    embed_dim: int | None

    # Optional keys (kept in env, read on demand)
    jina_api_key_env: str = "JINAAI_API_KEY"
    openai_api_key_env: str = "OPENAI_API_KEY"

    @property
    def artifacts_dir(self) -> Path:
        return self.kb_root / "artifacts"

    @property
    def run_records_dir(self) -> Path:
        return self.artifacts_dir / "run_records"

    @property
    def exports_dir(self) -> Path:
        return self.artifacts_dir / "exports"

    def ensure_dirs(self) -> None:
        self.kb_root.mkdir(parents=True, exist_ok=True)
        self.chat_jsonl_dir.mkdir(parents=True, exist_ok=True)
        self.chroma_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.run_records_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.cache_db.parent.mkdir(parents=True, exist_ok=True)


def load_config() -> KBConfig:
    """
    Load configuration from environment variables.
    Reasonable defaults are chosen to match your current repo layout conventions.

    Env vars supported:
      - KB_ROOT (default ".")
      - KB_CHAT_JSONL_DIR (default "<KB_ROOT>/test_data")
      - KB_CACHE_DB (default "<KB_ROOT>/embedding_cache.sqlite")
      - KB_CHROMA_DIR (default "<KB_ROOT>/store/chroma_jina_v3")
      - KB_COLLECTION (default "gpt_logs_jina_v3")
      - KB_EMBED_PROVIDER (default "jina")
      - KB_EMBED_MODEL (default "jina-embeddings-v3")
      - KB_EMBED_TASK (default "retrieval.passage")
      - KB_EMBED_DIM (default "" -> None)
    """
    kb_root = _as_path(_env("KB_ROOT", ".")) or Path(".")
    kb_root = kb_root.resolve()

    chat_jsonl_dir = _as_path(_env("KB_CHAT_JSONL_DIR")) or (kb_root / "test_data")
    cache_db = _as_path(_env("KB_CACHE_DB")) or (kb_root / "embedding_cache.sqlite")
    chroma_dir = _as_path(_env("KB_CHROMA_DIR")) or (kb_root / "store" / "chroma_jina_v3")
    collection_name = _env("KB_COLLECTION", "gpt_logs_jina_v3") or "gpt_logs_jina_v3"

    embed_provider = _env("KB_EMBED_PROVIDER", "jina") or "jina"
    embed_model = _env("KB_EMBED_MODEL", "jina-embeddings-v3") or "jina-embeddings-v3"
    embed_task = _env("KB_EMBED_TASK", "retrieval.passage")
    dim_raw = _env("KB_EMBED_DIM", None)
    embed_dim = int(dim_raw) if (dim_raw and dim_raw.isdigit()) else None

    # Resolve relative paths under kb_root
    def _resolve_under_root(p: Path) -> Path:
        if p.is_absolute():
            return p
        return (kb_root / p).resolve()

    chat_jsonl_dir = _resolve_under_root(chat_jsonl_dir)
    cache_db = _resolve_under_root(cache_db)
    chroma_dir = _resolve_under_root(chroma_dir)

    return KBConfig(
        kb_root=kb_root,
        chat_jsonl_dir=chat_jsonl_dir,
        cache_db=cache_db,
        chroma_dir=chroma_dir,
        collection_name=collection_name,
        embed_provider=embed_provider,
        embed_model=embed_model,
        embed_task=embed_task,
        embed_dim=embed_dim,
    )
