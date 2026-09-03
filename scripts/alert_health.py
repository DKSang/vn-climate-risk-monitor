#!/usr/bin/env python3
"""Send an unhealthy/degraded report to an optional HTTP webhook."""

from __future__ import annotations

import argparse
import json
import os
from urllib.request import Request, urlopen

from vn_climate_risk_monitor.health import collect_health


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope", choices=("forecast", "archive", "all"), default="all"
    )
    parser.add_argument("--require-gold", action="store_true")
    args = parser.parse_args()

    report = collect_health(scope=args.scope, require_gold=args.require_gold)
    print(report.to_json())
    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if report.status != "HEALTHY" and webhook:
        body = json.dumps(
            {
                "project": "vn-climate-risk-monitor",
                "status": report.status,
                "scope": report.scope,
                "checked_at_utc": report.checked_at_utc,
                "failed_checks": [
                    check.name for check in report.checks if check.status != "PASS"
                ],
            }
        ).encode()
        request = Request(
            webhook,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise RuntimeError(f"Webhook trả HTTP {response.status}")
    raise SystemExit(report.exit_code)


if __name__ == "__main__":
    main()
