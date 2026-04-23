"""
cli/kb_papers_grobid.py

CLI wrapper around kb.pipelines.papers_grobid.run_pdf

This expects `grobid_ingest.py` to be importable (PYTHONPATH) or vendored.
Example:
  python -m kb.cli.kb_papers_grobid ~/papers/foo.pdf --save-tei ./artifacts/exports/foo.tei.xml
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from kb.pipelines.papers_grobid import run_pdf


def _parse_args(argv):
    ap = argparse.ArgumentParser(prog="kb_papers_grobid")
    ap.add_argument("pdf", help="PDF file path")
    ap.add_argument("--no-post", action="store_true", help="Do NOT POST to GROBID service (assumes TEI already exists or other mode)")
    ap.add_argument("--save-tei", default=None, help="Path to save TEI XML")
    ap.add_argument("--chroma-dir", default=None, help="Optional Chroma dir for upsert (legacy behavior)")
    ap.add_argument("--langchain", action="store_true", help="Emit LangChain Documents in legacy runner")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    pdf = Path(args.pdf).expanduser()
    save_tei = Path(args.save_tei).expanduser() if args.save_tei else None
    chroma_dir = Path(args.chroma_dir).expanduser() if args.chroma_dir else None

    res = run_pdf(
        pdf,
        do_post_grobid=not bool(args.no_post),
        save_tei=save_tei,
        chroma_dir=chroma_dir,
        emit_langchain=bool(args.langchain),
    )
    print(f"run_record: {res.run_record_path}")
    print(f"status: {res.run_record.get('status')}")
    if res.run_record.get("status") != "ok":
        print("errors:", res.run_record.get("errors"), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
