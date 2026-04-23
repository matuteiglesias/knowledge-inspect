"""
sqlite_cache.py

Embedding cache: (id -> vector) stored in SQLite.
Separated from processed-files state (see processed_files.py).

Inspired by the simple cached_embed pattern used in your dev scripts:
- "vecs" table with (id, dim, blob) fileciteturn2file10L36-L44
- frequent commits so Ctrl+C doesn't lose work fileciteturn2file10L41-L44

Notes:
- We store float32 bytes in little-endian (NumPy default).
- Dimension is stored per row; you can optionally assert expected dim.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
import numpy as np
from typing import Callable, Optional


VEC_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS vecs (
  id  TEXT PRIMARY KEY,
  dim INTEGER NOT NULL,
  blob BLOB NOT NULL
);
"""


@dataclass
class SQLiteVecCache:
    con: sqlite3.Connection

    @classmethod
    def open(cls, path: Path) -> "SQLiteVecCache":
        path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(str(path))
        con.execute(VEC_TABLE_DDL)
        con.commit()
        return cls(con=con)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass

    def get(self, vec_id: str) -> Optional[np.ndarray]:
        row = self.con.execute("SELECT blob, dim FROM vecs WHERE id = ?", (vec_id,)).fetchone()
        if not row:
            return None
        blob, dim = row
        return np.frombuffer(blob, dtype=np.float32).reshape((dim,))

    def put(self, vec_id: str, vec: np.ndarray) -> None:
        vec = np.asarray(vec, dtype=np.float32).reshape((-1,))
        self.con.execute(
            "INSERT OR REPLACE INTO vecs (id, dim, blob) VALUES (?,?,?)",
            (vec_id, int(vec.size), vec.tobytes()),
        )
        # commit immediately to preserve work under interruptions (Ctrl+C)
        self.con.commit()

    def cached_embedder(
        self,
        embed_fn: Callable[[str], np.ndarray],
        *,
        expected_dim: int | None = None,
    ) -> Callable[[str, str], np.ndarray]:
        """
        Wrap a pure embedding function (text -> vec) into a cached_embed(text_id, text) callable.
        """
        def cached_embed(text_id: str, text: str) -> np.ndarray:
            cached = self.get(text_id)
            if cached is not None:
                if expected_dim is not None and int(cached.size) != int(expected_dim):
                    raise ValueError(f"Cached vec dim mismatch for id={text_id}: got={cached.size} expected={expected_dim}")
                return cached

            vec = np.asarray(embed_fn(text), dtype=np.float32).reshape((-1,))
            if expected_dim is not None and int(vec.size) != int(expected_dim):
                raise ValueError(f"Embed vec dim mismatch for id={text_id}: got={vec.size} expected={expected_dim}")

            self.put(text_id, vec)
            return vec

        return cached_embed
