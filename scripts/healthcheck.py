#!/usr/bin/env python3
"""Print a machine-readable health report and return non-zero when unhealthy."""

from __future__ import annotations

import argparse
from pathlib import Path

from vn_climate_risk_monitor.health import collect_health


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope", choices=("forecast", "archive", "all"), default="all"
    )
    parser.add_argument(
        "--require-gold",
        action="store_true",
        help="Kiểm tra freshness/grain của Gold sau transform",
    )
    parser.add_argument("--output", type=Path, help="Ghi thêm JSON atomically vào file")
    args = parser.parse_args()

    report = collect_health(scope=args.scope, require_gold=args.require_gold)
    payload = report.to_json()
    print(payload)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        temporary.replace(args.output)
    raise SystemExit(report.exit_code)


if __name__ == "__main__":
    main()
