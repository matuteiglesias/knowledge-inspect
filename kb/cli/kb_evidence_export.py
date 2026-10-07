"""CLI for producer-owned summary_bus -> generic evidence JSONL projection."""

from __future__ import annotations

import argparse
import json
import sys

from kb.evidence_export import EvidenceExportError, export_summary_bus


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Project governed Knowledge Inspect summary artifacts to generic JSONL evidence."
    )
    parser.add_argument("summary", nargs="+", help="summary_bus JSON artifact(s)")
    parser.add_argument("--output", required=True, help="new JSONL output path")
    args = parser.parse_args()

    try:
        receipt = export_summary_bus(args.summary, output=args.output)
    except EvidenceExportError as exc:
        sys.stderr.write(
            json.dumps(
                {
                    "error_code": "evidence_export_failed",
                    "message": str(exc),
                },
                sort_keys=True,
            )
            + "\n"
        )
        return 2

    sys.stdout.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
