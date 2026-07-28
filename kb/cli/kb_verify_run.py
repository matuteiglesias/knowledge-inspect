from __future__ import annotations

import argparse
import json
import sys

from kb.config.kb_config import load_config
from kb.contracts.run_evidence import verify_run


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="kb_verify_run")
    parser.add_argument("run_id", help="Producer run identifier (not a filesystem path)")
    parser.add_argument("--operator", help="Expected dotted operator identity")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    report = verify_run(load_config(), args.run_id, operator=args.operator)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(f"{report['status'].upper()} run_id={report['run_id']}")
        for detail in report["details"]:
            print(f"- {detail}")
    return int(report["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
