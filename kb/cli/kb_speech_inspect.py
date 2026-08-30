from __future__ import annotations

import argparse
import json
from pathlib import Path

from kb.speech_inspect import build_index, query_index


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Bounded inspection of a politics-wiki speech chunk projection"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    index_parser = sub.add_parser("index", help="Build a deterministic local lexical index")
    index_parser.add_argument("--chunk-set", type=Path, required=True)
    index_parser.add_argument("--index-root", type=Path, required=True)

    query_parser = sub.add_parser("query", help="Query one previously built speech lexical index")
    query_parser.add_argument("--index-dir", type=Path, required=True)
    query_parser.add_argument("--query", required=True)
    query_parser.add_argument("--top-k", type=int, default=5)

    args = parser.parse_args(argv)
    if args.command == "index":
        manifest = build_index(args.chunk_set, args.index_root)
        public = {key: value for key, value in manifest.items() if key != "index_dir"}
        print(json.dumps(public, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    result = query_index(args.index_dir, args.query, args.top_k)
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
