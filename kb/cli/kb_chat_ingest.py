"""
cli/kb_chat_ingest.py

CLI wrapper around kb.pipelines.chat_ingest.ingest_paths

Example:
  python -m kb.cli.kb_chat_ingest --glob "~/Documents/GPT_n/test_data/2025*.jsonl"

Notes:
- Config is read from env via KBConfig (see kb/config/kb_config.py).
- Secrets are read from env (e.g., JINAAI_API_KEY).
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from kb.pipelines.chat_ingest import ingest_paths
import glob

def _parse_args(argv):
    ap = argparse.ArgumentParser(prog="kb_chat_ingest")
    ap.add_argument("--paths", nargs="*", default=None, help="Explicit JSONL file paths")
    ap.add_argument("--glob", default=None, help="Glob pattern to expand into paths")
    ap.add_argument("--reset-collection", action="store_true", help="Reset Chroma collection (destructive). Requires allow_reset.")
    ap.add_argument("--dry-run", action="store_true", help="Parse+embed but do not write to Chroma nor mark processed_files.")
    ap.add_argument("--batch-size", type=int, default=128, help="Chroma add batch size")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    paths = []

    if args.paths:
        paths.extend([Path(p).expanduser() for p in args.paths])

    if args.glob:
        pat = str(Path(args.glob).expanduser())
        paths.extend([Path(p) for p in sorted(glob.glob(pat, recursive=True))])


    if not paths:
        print("No input paths. Use --paths or --glob.", file=sys.stderr)
        return 2

    res = ingest_paths(
        paths,
        reset_collection=bool(args.reset_collection),
        dry_run=bool(args.dry_run),
        batch_size=int(args.batch_size),
    )
    print(f"run_record: {res.run_record_path}")
    print(f"status: {res.run_record.get('status')}")
    print(f"stats: {res.run_record.get('stats')}")
    if res.run_record.get("status") != "ok":
        print("errors:", res.run_record.get("errors"), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
