#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

import codex_account_monitor as monitor


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Show live Codex account counts grouped by active plan type and deactive state. "
            "The pro count includes both pro and prolite accounts."
        )
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to config.yaml. Default: {DEFAULT_CONFIG_PATH}",
    )
    parser.add_argument(
        "--settings-file",
        help="Optional settings file. Defaults to the monitor settings search path.",
    )
    parser.add_argument(
        "--base-url",
        help="Management API base URL. Defaults to http://127.0.0.1:<port>/v0/management.",
    )
    parser.add_argument(
        "--key",
        help="Management key. If omitted, read from settings/env or prompt interactively.",
    )
    parser.add_argument(
        "--auth-dir",
        help="Authentication directory. Defaults to auth-dir from config or ~/.cli-proxy-api.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel workers for live lookups. Default: 4.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=monitor.REQUEST_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds. Default: {monitor.REQUEST_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output normalized JSON instead of plain text.",
    )
    return parser.parse_args()


def build_monitor_config(args: argparse.Namespace) -> monitor.Config:
    config = monitor.resolve_config(
        argparse.Namespace(
            config=args.config,
            settings_file=args.settings_file,
            base_url=args.base_url,
            key=args.key,
            auth_dir=args.auth_dir,
            state_dir=None,
            log_dir=None,
            include_disabled=True,
            workers=args.workers,
            timeout=args.timeout,
            dry_run=True,
            json=args.json,
            once=True,
            loop=False,
            no_prime=True,
        )
    )
    config.include_disabled = True
    config.dry_run = True
    config.no_prime = True
    config.once = True
    config.loop = False
    return config


def is_active_row(row: dict[str, object]) -> bool:
    if bool(row.get("disabled")):
        return False
    runtime_status = str(row.get("runtime_status") or "").lower()
    live_state = str(row.get("live_state") or "").lower()
    if runtime_status not in {"active", "unknown"}:
        return False
    return live_state not in {"deactive", "invalidated"}


def summarize_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    counts = {
        "pro": 0,
        "plus": 0,
        "team": 0,
        "free": 0,
        "deactive": 0,
        "prolite_merged_into_pro": 0,
    }
    quota_exhausted = {
        "pro20": 0,
        "pro5": 0,
        "plus": 0,
        "team": 0,
        "free": 0,
    }

    for row in rows:
        live_state = str(row.get("live_state") or "")
        live_plan = str(row.get("live_plan") or "").lower()
        health_state = str(row.get("health_state") or "").lower()

        if live_state == "deactive":
            counts["deactive"] += 1

        if health_state == "quota_exhausted":
            if live_plan == "pro":
                quota_exhausted["pro20"] += 1
            elif live_plan == "prolite":
                quota_exhausted["pro5"] += 1
            elif live_plan == "plus":
                quota_exhausted["plus"] += 1
            elif live_plan == "team":
                quota_exhausted["team"] += 1
            elif live_plan == "free":
                quota_exhausted["free"] += 1

        if not is_active_row(row):
            continue

        if live_plan in {"pro", "prolite"}:
            counts["pro"] += 1
            if live_plan == "prolite":
                counts["prolite_merged_into_pro"] += 1
        elif live_plan == "plus":
            counts["plus"] += 1
        elif live_plan == "team":
            counts["team"] += 1
        elif live_plan == "free":
                counts["free"] += 1

    counts["active_total"] = counts["pro"] + counts["plus"] + counts["team"] + counts["free"]
    return {
        "counts": counts,
        "quota_exhausted": quota_exhausted,
    }


def prune_zero_values(values: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in values.items()
        if not (isinstance(value, int) and value == 0)
    }


def main() -> int:
    args = parse_args()
    config = build_monitor_config(args)
    history = monitor.load_json_file(config.state_dir / monitor.HISTORY_FILENAME, default={})
    rows, _, service_state = monitor.collect_rows(config, history)

    if service_state != "ok":
        raise SystemExit(f"monitor query failed: service_state={service_state}")

    summary = summarize_rows(rows)
    visible_counts = prune_zero_values(summary["counts"])
    visible_quota_exhausted = prune_zero_values(summary["quota_exhausted"])
    payload = {
        "generated_at": monitor.utc_now_iso(),
        "service_state": service_state,
        "summary": {
            "counts": visible_counts,
            "quota_exhausted": visible_quota_exhausted,
        },
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(f"time: {payload['generated_at']}")
    print("active counts")
    for key in ("pro", "plus", "team", "free", "deactive", "active_total"):
        if key in visible_counts:
            print(f"{key}: {visible_counts[key]}")
    if "prolite_merged_into_pro" in visible_counts:
        print(f"note: prolite merged into pro = {visible_counts['prolite_merged_into_pro']}")
    if visible_quota_exhausted:
        print("quota_exhausted")
        for key in ("pro20", "pro5", "plus", "team", "free"):
            if key in visible_quota_exhausted:
                print(f"{key}: {visible_quota_exhausted[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
