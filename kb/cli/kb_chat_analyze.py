"""
cli/kb_chat_analyze.py

CLI wrapper around kb.pipelines.chat_analyze.analyze

Example:
  python -m kb.cli.kb_chat_analyze --export-name combined_notes.md
"""
from __future__ import annotations

import argparse
import sys

from kb.pipelines.chat_analyze import analyze


def _parse_args(argv):
    ap = argparse.ArgumentParser(prog="kb_chat_analyze")
    ap.add_argument("--export-name", default="combined_notes.md", help="Filename in artifacts/exports/")
    ap.add_argument("--batch-size", type=int, default=500, help="Chroma get batch size")
    ap.add_argument("--max-nodes", type=int, default=None, help="Optional cap for analysis")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    res = analyze(export_name=args.export_name, batch_size=int(args.batch_size), max_nodes=args.max_nodes)
    print(f"run_record: {res.run_record_path}")
    print(f"export: {res.export_path}")
    print(f"status: {res.run_record.get('status')}")
    if res.run_record.get("status") not in {"success", "empty_success", "partial_success"}:
        print("errors:", res.run_record.get("errors"), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
