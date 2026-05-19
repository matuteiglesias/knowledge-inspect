

# pipeline/embedding/cache.py
import sqlite3, time
from pathlib import Path
from dataclasses import dataclass

import numpy as np

@dataclass
class EmbeddingCache:
    db_path: Path
    conn: sqlite3.Connection

    @classmethod
    def open(cls, db_path: Path):
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vecs (
                id TEXT PRIMARY KEY,
                dim INTEGER,
                blob BLOB,
                created_at REAL
            )
        """)
        conn.commit()
        return cls(db_path=db_path, conn=conn)

    def get(self, uid: str):
        cur = self.conn.execute("SELECT blob, dim FROM vecs WHERE id=?", (uid,))
        row = cur.fetchone()
        if not row:
            return None
        blob, dim = row
        if np is None:
            return blob
        arr = np.frombuffer(blob, dtype=np.float32)
        if dim and arr.size != dim:
            return arr
        return arr

    def set(self, uid: str, vec):
        if np is None:
            raise RuntimeError("numpy required for cache.set")
        arr = np.array(vec, dtype=np.float32).reshape(-1)
        self.conn.execute("INSERT OR REPLACE INTO vecs (id, dim, blob, created_at) VALUES (?,?,?,?)",
                          (uid, int(arr.size), arr.tobytes(), time.time()))
        self.conn.commit()

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:
            pass


