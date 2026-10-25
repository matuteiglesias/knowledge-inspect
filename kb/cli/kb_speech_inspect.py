from __future__ import annotations

import argparse
import json
from pathlib import Path

from kb.speech_inspect import build_index, query_index
from kb.speech_run_evidence import record_speech_run, utc_now


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Bounded inspection of a politics-wiki speech chunk projection"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    index_parser = sub.add_parser("index", help="Build a deterministic local lexical index")
    index_parser.add_argument("--chunk-set", type=Path, required=True)
    index_parser.add_argument("--index-root", type=Path, required=True)
    index_parser.add_argument("--evidence-root", type=Path)

    query_parser = sub.add_parser("query", help="Query one previously built speech lexical index")
    query_parser.add_argument("--index-dir", type=Path, required=True)
    query_parser.add_argument("--query", required=True)
    query_parser.add_argument("--top-k", type=int, default=5)
    query_parser.add_argument("--evidence-root", type=Path)

    args = parser.parse_args(argv)
    started = utc_now()
    if args.command == "index":
        manifest = build_index(args.chunk_set, args.index_root)
        finished = utc_now()
        if args.evidence_root is not None:
            record_speech_run(
                root=args.evidence_root, mode="index",
                started_at=started, finished_at=finished,
                source_file=args.chunk_set, output_file=manifest["index_dir"] / "index.json",
            )
        public = {key: value for key, value in manifest.items() if key != "index_dir"}
        print(json.dumps(public, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    result = query_index(args.index_dir, args.query, args.top_k)
    finished = utc_now()
    if args.evidence_root is not None:
        # Full query results remain local. GCP receives only allowed hashes and
        # categorical status from the source manifest translator.
        from kb.speech_run_evidence import dump
        import hashlib
        encoded = json.dumps(result, ensure_ascii=False, sort_keys=True).encode("utf-8")
        output_name = hashlib.sha256(encoded).hexdigest() + ".json"
        output_dir = args.evidence_root / "local-query-outputs"
        dump(output_dir / output_name, result)
        record_speech_run(
            root=args.evidence_root, mode="query",
            started_at=started, finished_at=finished,
            source_file=args.index_dir / "index.json",
            output_file=output_dir / output_name,
        )
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
