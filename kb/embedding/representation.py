"""Repository-local identity for embedding-derived runtime state.

Logical knowledge identity stays producer-owned.  This module identifies only a
representation of that knowledge so caches, processed-state markers, and vector
collections cannot be silently reused across materially different embedding
configurations.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, Dict


@dataclass(frozen=True)
class EmbeddingRepresentation:
    provider: str
    model: str
    task: str | None = None
    configured_dim: int | None = None

    @classmethod
    def from_config(cls, cfg: Any) -> "EmbeddingRepresentation":
        return cls(
            provider=str(getattr(cfg, "embed_provider", "") or "").strip().lower(),
            model=str(getattr(cfg, "embed_model", "") or "").strip(),
            task=(str(getattr(cfg, "embed_task", "") or "").strip() or None),
            configured_dim=(
                int(getattr(cfg, "embed_dim"))
                if getattr(cfg, "embed_dim", None) is not None
                else None
            ),
        )

    def canonical_payload(self) -> Dict[str, Any]:
        return {
            "schema": "embedding-representation.v1",
            "provider": self.provider,
            "model": self.model,
            "task": self.task,
            "configured_dim": self.configured_dim,
        }

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(
            self.canonical_payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @property
    def representation_id(self) -> str:
        return f"embedrep-v1-{self.fingerprint[:24]}"

    def state_key(self, logical_id: str) -> str:
        """Namespace private derivative state without changing logical identity."""
        return f"{self.representation_id}:{logical_id}"

    def collection_name(self, base_name: str) -> str:
        """Resolve a private Chroma collection for this representation.

        The configured collection remains the operator-facing logical base name;
        the physical derivative collection receives a stable representation
        suffix so two same-dimensional models cannot share vector rows.
        """
        clean = re.sub(r"[^A-Za-z0-9._-]+", "-", str(base_name or "kb")).strip("._-")
        if not clean:
            clean = "kb"
        # Keep the generated name conservative for Chroma versions with stricter
        # collection-name limits while preserving a recognizable operator prefix.
        clean = clean[:48].rstrip("._-") or "kb"
        return f"{clean}-r-{self.fingerprint[:12]}"

    def evidence(self) -> Dict[str, Any]:
        return {
            **self.canonical_payload(),
            "representation_id": self.representation_id,
        }
