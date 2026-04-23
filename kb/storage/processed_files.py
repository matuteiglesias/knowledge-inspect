"""
processed_files.py

Tiny "have we already processed this input?" state manager.
In your drafts, this lived inside the same SQLite file as the embedding cache fileciteturn2file6L2-L4.

We keep it separate as a module, but it's still intended to use the same SQLite DB file.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3


PROCESSED_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS processed_files (
  fname TEXT PRIMARY KEY
);
"""


@dataclass
class ProcessedFiles:
    con: sqlite3.Connection

    @classmethod
    def open(cls, path: Path) -> "ProcessedFiles":
        path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(str(path))
        con.execute(PROCESSED_TABLE_DDL)
        con.commit()
        return cls(con=con)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass

    def is_processed(self, fname: str) -> bool:
        row = self.con.execute("SELECT 1 FROM processed_files WHERE fname = ? LIMIT 1", (fname,)).fetchone()
        return row is not None

    def mark_processed(self, fname: str) -> None:
        self.con.execute("INSERT OR REPLACE INTO processed_files (fname) VALUES (?)", (fname,))
        self.con.commit()

    def all_processed(self) -> list[str]:
        rows = self.con.execute("SELECT fname FROM processed_files ORDER BY fname").fetchall()
        return [r[0] for r in rows]
