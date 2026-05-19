from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from kb.contracts.chunk_set import ChunkSetValidationError, validate_chunk_set_file


def _parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="kb_validate_chunk_set")
    ap.add_argument("paths", nargs="+", help="One or more chunk_set artifact JSON files")
    ap.add_argument(
        "--format",
        choices=["json", "text"],
        default="json",
        help="Output format for diagnostics",
    )
    return ap.parse_args(argv)


def _emit_json(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False))


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    all_valid = True

    for raw_path in args.paths:
        path = Path(raw_path).expanduser()
        try:
            payload = validate_chunk_set_file(path)
            chunk_count = payload.get("chunk_count")
            if not isinstance(chunk_count, int):
                chunk_count = len(payload.get("chunks", []))

            if args.format == "json":
                _emit_json({
                    "status": "valid",
                    "path": str(path),
                    "chunk_count": chunk_count,
                })
            else:
                print(f"VALID {path} chunk_count={chunk_count}")
        except (ChunkSetValidationError, OSError, json.JSONDecodeError) as exc:
            all_valid = False
            if args.format == "json":
                _emit_json({
                    "status": "invalid",
                    "path": str(path),
                    "error": str(exc),
                })
            else:
                print(f"INVALID {path}: {exc}", file=sys.stderr)

    return 0 if all_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
