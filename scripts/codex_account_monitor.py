#!/usr/bin/env python3

import argparse
import base64
import concurrent.futures
import fcntl
import getpass
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
DEFAULT_AUTH_DIR = "~/.cli-proxy-api"
DEFAULT_SETTINGS_FILENAMES = (
    "/etc/cliproxyapi-codex-monitor.conf",
    os.path.expanduser("~/.config/cliproxyapi-codex-monitor.conf"),
)
DEFAULT_WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
DEFAULT_PRIME_URL = "https://chatgpt.com/backend-api/codex"
DEFAULT_PRIME_MODEL = "gpt-5.4-mini"
DEFAULT_USER_AGENT = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
DEFAULT_STATE_DIR = Path(__file__).resolve().parents[1] / "state"
DEFAULT_LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
DEFAULT_SCAN_INTERVAL_SECONDS = 300
DEFAULT_PLUS_THRESHOLD = 4
DEFAULT_PRO20_THRESHOLD = 0
DEFAULT_PRO5_THRESHOLD = 0
DEFAULT_PRIME_FAILURE_COOLDOWN_SECONDS = 43_200
DEFAULT_TIMEZONE = "Asia/Shanghai"
LOCK_FILENAME = "codex-account-monitor.lock"
SNAPSHOT_FILENAME = "codex-monitor-snapshot.json"
HISTORY_FILENAME = "codex-monitor-history.json"
LOG_FILENAME = "codex-monitor.log"
ROTATED_LOG_PREFIX = "codex-monitor-"
LOG_RETENTION_DAYS = 7

PRO_20X_PLANS = {"pro"}
PRO_5X_PLANS = {"prolite"}
PLUS_PLANS = {"plus"}
TEAM_PLANS = {"team"}
PAID_PLANS = PRO_20X_PLANS | PRO_5X_PLANS | PLUS_PLANS | TEAM_PLANS
NON_ROUTABLE_STATES = {
    "disabled",
    "deactive",
    "invalidated",
    "quota_exhausted",
    "service_down",
    "live_error",
    "runtime_unavailable",
}
ALERTABLE_STATES = {"disabled", "deactive", "invalidated"}
DEACTIVE_PATTERN = re.compile(r"\b(?:deactive|deactivated)\b", re.IGNORECASE)
INVALIDATED_PATTERN = re.compile(
    r"\b(?:invalidated|unauthorized|authentication token has been invalidated)\b",
    re.IGNORECASE,
)
REQUEST_TIMEOUT_SECONDS = 30.0


@dataclass
class Config:
    config_path: str
    settings_path: str | None
    base_url: str
    management_key: str
    auth_dir: Path
    state_dir: Path
    log_dir: Path
    timezone: str
    scan_interval_seconds: int
    plus_threshold: int
    pro20_threshold: int
    pro5_threshold: int
    prime_model: str
    prime_url: str
    prime_enabled: bool
    low_plus_hook: str | None
    low_plus_hook_enabled: bool
    tg_bot_token: str | None
    tg_chat_id: str | None
    include_disabled: bool
    workers: int
    dry_run: bool
    loop: bool
    no_prime: bool
    once: bool
    timeout: float
    prime_failure_cooldown_seconds: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Standalone Codex account monitor for CLIProxyAPI. It collects runtime status, "
            "tracks live plan/quota data, sends Telegram alerts, primes paid accounts whose "
            "weekly window has not started, and recalculates auth-file priority."
        )
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to config.yaml. Default: {DEFAULT_CONFIG_PATH}",
    )
    parser.add_argument(
        "--settings-file",
        help=(
            "Optional monitor settings file. Defaults to "
            "/etc/cliproxyapi-codex-monitor.conf or ~/.config/cliproxyapi-codex-monitor.conf."
        ),
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
        help=(
            "Authentication directory. Defaults to auth-dir from config or "
            f"{DEFAULT_AUTH_DIR} if config is unavailable."
        ),
    )
    parser.add_argument(
        "--state-dir",
        help=f"Directory for state snapshots. Default: {DEFAULT_STATE_DIR}",
    )
    parser.add_argument(
        "--log-dir",
        help=f"Directory for monitor logs. Default: {DEFAULT_LOG_DIR}",
    )
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Include disabled auth files in the JSON output and change detection.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel workers for usage lookups. Default: 4.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=REQUEST_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds. Default: {REQUEST_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect, compare, and print planned actions without editing auth files.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the current run result as JSON to stdout.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one collection cycle and exit.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously and sleep according to SCAN_INTERVAL_SECONDS.",
    )
    parser.add_argument(
        "--no-prime",
        action="store_true",
        help="Skip paid-account week priming for this run.",
    )
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(message: str, *, error: bool = False) -> None:
    stream = sys.stderr if error else sys.stdout
    print(f"[{utc_now_iso()}] {message}", file=stream)


def safe_zoneinfo(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("UTC")


def parse_settings_text(raw: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if key:
            values[key] = value
    return values


def load_settings_file(settings_file: str | None) -> tuple[str | None, dict[str, str]]:
    candidates = [settings_file] if settings_file else list(DEFAULT_SETTINGS_FILENAMES)
    for path in candidates:
        if not path:
            continue
        expanded = os.path.expanduser(path)
        if not os.path.isfile(expanded):
            continue
        try:
            with open(expanded, "r", encoding="utf-8") as handle:
                return expanded, parse_settings_text(handle.read())
        except OSError:
            continue
    return None, {}


def normalize_string(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return str(value)
    return None


def normalize_number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        numeric = float(value)
        if math.isfinite(numeric):
            return numeric
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            return None
        if math.isfinite(numeric):
            return numeric
    return None


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def env_or_setting(settings: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = normalize_string(settings.get(key) or os.getenv(key))
        if value:
            return value
    return None


def read_config_port(config_path: str) -> int:
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            for line in handle:
                match = re.match(r"^\s*port:\s*(\d+)\s*$", line)
                if match:
                    return int(match.group(1))
    except OSError:
        pass
    return 8317


def read_auth_dir_from_config(config_path: str) -> str | None:
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            for line in handle:
                match = re.match(r'^\s*auth-dir:\s*"?([^"#]+?)"?\s*(?:#.*)?$', line)
                if match:
                    return match.group(1).strip()
    except OSError:
        return None
    return None


def resolve_auth_dir(args: argparse.Namespace, settings: dict[str, str]) -> Path:
    raw_path = (
        args.auth_dir
        or env_or_setting(settings, "CPA_AUTH_DIR", "CLIPROXYAPI_AUTH_DIR")
        or read_auth_dir_from_config(args.config)
        or DEFAULT_AUTH_DIR
    )
    return Path(os.path.expanduser(raw_path)).resolve()


def normalize_base_url(base_url: str | None, config_path: str) -> str:
    if not base_url:
        port = read_config_port(config_path)
        return f"http://127.0.0.1:{port}/v0/management"

    normalized = base_url.rstrip("/")
    if normalized.endswith("/v0/management"):
        return normalized
    if normalized.endswith("/management"):
        return normalized
    return normalized + "/v0/management"


def resolve_management_key(cli_key: str | None, settings: dict[str, str]) -> str:
    candidates = [
        cli_key,
        settings.get("CPA_MANAGEMENT_KEY"),
        settings.get("CLIPROXYAPI_MANAGEMENT_KEY"),
        settings.get("MANAGEMENT_PASSWORD"),
        os.getenv("CPA_MANAGEMENT_KEY"),
        os.getenv("CLIPROXYAPI_MANAGEMENT_KEY"),
        os.getenv("MANAGEMENT_PASSWORD"),
    ]
    for candidate in candidates:
        value = normalize_string(candidate)
        if value:
            return value

    if sys.stdin.isatty():
        value = getpass.getpass("Management key: ").strip()
        if value:
            return value

    raise SystemExit(
        "Missing management key. Use --key, set CPA_MANAGEMENT_KEY, or define it in the monitor settings file."
    )


def parse_positive_int(value: str | None, default: int) -> int:
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def resolve_config(args: argparse.Namespace) -> Config:
    settings_path, settings = load_settings_file(args.settings_file)
    management_key = resolve_management_key(args.key, settings)
    base_url = normalize_base_url(
        args.base_url or env_or_setting(settings, "CPA_BASE_URL", "CLIPROXYAPI_BASE_URL"),
        args.config,
    )
    auth_dir = resolve_auth_dir(args, settings)
    state_dir = Path(
        os.path.expanduser(
            args.state_dir
            or env_or_setting(settings, "CPA_MONITOR_STATE_DIR", "CLIPROXYAPI_CODEX_MONITOR_STATE_DIR")
            or str(DEFAULT_STATE_DIR)
        )
    ).resolve()
    log_dir = Path(
        os.path.expanduser(
            args.log_dir
            or env_or_setting(settings, "CPA_MONITOR_LOG_DIR", "CLIPROXYAPI_CODEX_MONITOR_LOG_DIR")
            or str(DEFAULT_LOG_DIR)
        )
    ).resolve()
    timezone = env_or_setting(settings, "TIMEZONE", "CPA_MONITOR_TIMEZONE") or DEFAULT_TIMEZONE
    scan_interval_seconds = parse_positive_int(
        env_or_setting(settings, "SCAN_INTERVAL_SECONDS", "CPA_MONITOR_SCAN_INTERVAL_SECONDS"),
        DEFAULT_SCAN_INTERVAL_SECONDS,
    )
    plus_threshold = parse_positive_int(
        env_or_setting(settings, "PLUS_THRESHOLD", "CPA_MONITOR_PLUS_THRESHOLD"),
        DEFAULT_PLUS_THRESHOLD,
    )
    pro20_threshold = parse_positive_int(
        env_or_setting(settings, "PRO20_THRESHOLD", "CPA_MONITOR_PRO20_THRESHOLD"),
        DEFAULT_PRO20_THRESHOLD,
    )
    pro5_threshold = parse_positive_int(
        env_or_setting(settings, "PRO5_THRESHOLD", "CPA_MONITOR_PRO5_THRESHOLD"),
        DEFAULT_PRO5_THRESHOLD,
    )
    prime_failure_cooldown_seconds = parse_positive_int(
        env_or_setting(
            settings,
            "PRIME_FAILURE_COOLDOWN_SECONDS",
            "CPA_MONITOR_PRIME_FAILURE_COOLDOWN_SECONDS",
        ),
        DEFAULT_PRIME_FAILURE_COOLDOWN_SECONDS,
    )
    prime_model = (
        env_or_setting(settings, "CODEX_PRIME_MODEL", "CPA_MONITOR_CODEX_PRIME_MODEL")
        or DEFAULT_PRIME_MODEL
    )
    prime_url = (
        env_or_setting(settings, "CODEX_PRIME_URL", "CPA_MONITOR_CODEX_PRIME_URL")
        or DEFAULT_PRIME_URL
    )
    low_plus_hook = env_or_setting(settings, "LOW_PLUS_HOOK", "CPA_MONITOR_LOW_PLUS_HOOK")
    low_plus_hook_enabled = boolish(
        env_or_setting(
            settings,
            "LOW_PLUS_HOOK_ENABLED",
            "CPA_MONITOR_LOW_PLUS_HOOK_ENABLED",
        )
    )
    tg_bot_token = env_or_setting(settings, "TG_BOT_TOKEN", "CPA_MONITOR_TG_BOT_TOKEN")
    tg_chat_id = env_or_setting(settings, "TG_CHAT_ID", "CPA_MONITOR_TG_CHAT_ID")
    prime_enabled = not boolish(
        env_or_setting(settings, "PRIME_DISABLED", "CPA_MONITOR_PRIME_DISABLED")
    )
    return Config(
        config_path=args.config,
        settings_path=settings_path,
        base_url=base_url,
        management_key=management_key,
        auth_dir=auth_dir,
        state_dir=state_dir,
        log_dir=log_dir,
        timezone=timezone,
        scan_interval_seconds=scan_interval_seconds,
        plus_threshold=plus_threshold,
        pro20_threshold=pro20_threshold,
        pro5_threshold=pro5_threshold,
        prime_model=prime_model,
        prime_url=prime_url,
        prime_enabled=prime_enabled,
        low_plus_hook=low_plus_hook,
        low_plus_hook_enabled=low_plus_hook_enabled,
        tg_bot_token=tg_bot_token,
        tg_chat_id=tg_chat_id,
        include_disabled=args.include_disabled,
        workers=max(1, args.workers),
        dry_run=args.dry_run,
        loop=args.loop,
        no_prime=args.no_prime,
        once=args.once or not args.loop,
        timeout=max(1.0, args.timeout),
        prime_failure_cooldown_seconds=prime_failure_cooldown_seconds,
    )


def parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_from_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def decode_base64url(segment: str) -> str | None:
    text = segment.strip()
    if not text:
        return None
    padded = text + ("=" * (-len(text) % 4))
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8"))
    except (ValueError, OSError):
        return None
    try:
        return decoded.decode("utf-8")
    except UnicodeDecodeError:
        return None


def parse_json_like(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def parse_id_token_payload(value: Any) -> dict[str, Any] | None:
    record = parse_json_like(value)
    if record is not None:
        return record
    if not isinstance(value, str):
        return None
    parts = value.strip().split(".")
    if len(parts) < 2:
        return None
    decoded = decode_base64url(parts[1])
    if not decoded:
        return None
    return parse_json_like(decoded)


def extract_error_message(raw: str) -> str:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip()

    if isinstance(parsed, dict):
        error_value = parsed.get("error")
        if isinstance(error_value, dict):
            message = normalize_string(error_value.get("message"))
            if message:
                return message
        if isinstance(error_value, str) and error_value.strip():
            return error_value.strip()
        message = normalize_string(parsed.get("message"))
        if message:
            return message
    return raw.strip()


def walk_strings(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, str):
        found.append(value)
    elif isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str):
                found.append(key)
            found.extend(walk_strings(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(walk_strings(item))
    return found


def classify_remote_error(payload: Any) -> tuple[str, str]:
    candidates = walk_strings(payload)
    if isinstance(payload, str):
        candidates.append(payload)
    for candidate in candidates:
        text = " ".join(candidate.strip().split())
        if not text:
            continue
        if DEACTIVE_PATTERN.search(text):
            return "deactive", text[:240]
        if INVALIDATED_PATTERN.search(text):
            return "invalidated", text[:240]
    if isinstance(payload, dict):
        message = normalize_string(payload.get("message"))
        if message:
            return "live_error", message[:240]
    return "live_error", ""


def request_json(
    base_url: str,
    key: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = REQUEST_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    body = None
    headers = {
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        message = extract_error_message(raw) or raw.strip() or str(exc.reason)
        if exc.code == 401:
            message = (
                f"{message}. Expected CLIProxyAPI management key/local password, not provider API key"
            )
        raise RuntimeError(f"{exc.code} {message}".strip()) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"request failed: {exc.reason}") from exc

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON response from {url}: {raw[:200]}") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError(f"unexpected JSON response from {url}")
    return parsed


def request_plain_json(
    url: str,
    payload: dict[str, Any],
    *,
    timeout: float = REQUEST_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{exc.code} {extract_error_message(raw)}".strip()) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"request failed: {exc.reason}") from exc

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON response from {url}: {raw[:200]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"unexpected JSON response from {url}")
    return parsed


def fetch_auth_files(config: Config) -> list[dict[str, Any]]:
    payload = request_json(config.base_url, config.management_key, "/auth-files", timeout=config.timeout)
    files = payload.get("files")
    if not isinstance(files, list):
        raise RuntimeError("management /auth-files returned no files array")
    return [item for item in files if isinstance(item, dict)]


def load_local_auth_records(auth_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if not auth_dir.is_dir():
        return records
    for path in sorted(auth_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            records[path.name] = payload
    return records


def resolve_account_id(file_entry: dict[str, Any], local_record: dict[str, Any] | None) -> str | None:
    candidates = [
        file_entry.get("account_id"),
        (file_entry.get("metadata") or {}).get("account_id") if isinstance(file_entry.get("metadata"), dict) else None,
        (file_entry.get("attributes") or {}).get("account_id") if isinstance(file_entry.get("attributes"), dict) else None,
    ]
    if local_record:
        candidates.append(local_record.get("account_id"))
    for candidate in candidates:
        value = normalize_string(candidate)
        if value:
            return value

    token_candidates = [
        file_entry.get("id_token"),
        (file_entry.get("metadata") or {}).get("id_token") if isinstance(file_entry.get("metadata"), dict) else None,
        (file_entry.get("attributes") or {}).get("id_token") if isinstance(file_entry.get("attributes"), dict) else None,
    ]
    if local_record:
        token_candidates.extend(
            [
                local_record.get("id_token"),
                (local_record.get("metadata") or {}).get("id_token")
                if isinstance(local_record.get("metadata"), dict)
                else None,
                (local_record.get("attributes") or {}).get("id_token")
                if isinstance(local_record.get("attributes"), dict)
                else None,
            ]
        )
    for candidate in token_candidates:
        payload = parse_id_token_payload(candidate)
        if not payload:
            continue
        value = normalize_string(payload.get("chatgpt_account_id") or payload.get("chatgptAccountId"))
        if value:
            return value
    return None


def build_usage_api_call_payload(auth_index: str, account_id: str) -> dict[str, Any]:
    return {
        "authIndex": auth_index,
        "method": "GET",
        "url": DEFAULT_WHAM_USAGE_URL,
        "header": {
            "Authorization": "Bearer $TOKEN$",
            "Content-Type": "application/json",
            "User-Agent": DEFAULT_USER_AGENT,
            "Chatgpt-Account-Id": account_id,
            "Accept": "application/json",
        },
    }


def fetch_usage_payload(config: Config, auth_index: str, account_id: str) -> dict[str, Any]:
    result = request_json(
        config.base_url,
        config.management_key,
        "/api-call",
        method="POST",
        payload=build_usage_api_call_payload(auth_index, account_id),
        timeout=config.timeout,
    )
    status_code = int(result.get("status_code") or result.get("statusCode") or 0)
    body = result.get("body")
    if isinstance(body, str):
        text = body.strip()
        if text:
            try:
                body = json.loads(text)
            except json.JSONDecodeError:
                body = {"raw_body": text}
        else:
            body = {}
    if status_code < 200 or status_code >= 300:
        state, detail = classify_remote_error(body if isinstance(body, dict) else result.get("body"))
        raise RuntimeError(f"{state}:{detail or f'upstream {status_code}'}")
    if not isinstance(body, dict):
        raise RuntimeError("usage response is not a JSON object")
    return body


def build_prime_api_call_payload(
    auth_index: str,
    account_id: str,
    *,
    prime_url: str,
    model: str,
) -> dict[str, Any]:
    body = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Reply with OK.",
                    }
                ],
            }
        ],
        "max_output_tokens": 1,
        "stream": False,
    }
    return {
        "authIndex": auth_index,
        "method": "POST",
        "url": prime_url,
        "header": {
            "Authorization": "Bearer $TOKEN$",
            "Content-Type": "application/json",
            "User-Agent": DEFAULT_USER_AGENT,
            "Chatgpt-Account-Id": account_id,
            "Accept": "application/json",
        },
        "body": json.dumps(body, ensure_ascii=True),
    }


def run_prime_request(config: Config, auth_index: str, account_id: str) -> dict[str, Any]:
    payload = build_prime_api_call_payload(
        auth_index,
        account_id,
        prime_url=config.prime_url,
        model=config.prime_model,
    )
    result = request_json(
        config.base_url,
        config.management_key,
        "/api-call",
        method="POST",
        payload=payload,
        timeout=config.timeout,
    )
    status_code = int(result.get("status_code") or result.get("statusCode") or 0)
    body = result.get("body")
    if isinstance(body, str):
        text = body.strip()
        if text:
            try:
                body = json.loads(text)
            except json.JSONDecodeError:
                body = {"raw_body": text}
        else:
            body = {}
    if status_code < 200 or status_code >= 300:
        detail = extract_error_message(json.dumps(body, ensure_ascii=False)) if isinstance(body, dict) else normalize_string(body)
        raise RuntimeError(f"prime status {status_code}: {detail or 'request failed'}")
    return body if isinstance(body, dict) else {}


def load_json_file(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.is_file():
        return default or {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return default or {}
    return data if isinstance(data, dict) else (default or {})


def write_json_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(serialized)
    os.replace(tmp_path, path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def ensure_directories(config: Config) -> None:
    config.state_dir.mkdir(parents=True, exist_ok=True)
    config.log_dir.mkdir(parents=True, exist_ok=True)


def monitor_log_path(config: Config, when: datetime | None = None) -> Path:
    moment = (when or utc_now()).astimezone(safe_zoneinfo(config.timezone))
    return config.log_dir / f"{ROTATED_LOG_PREFIX}{moment.date().isoformat()}.log"


def prune_monitor_logs(config: Config, when: datetime | None = None) -> None:
    moment = (when or utc_now()).astimezone(safe_zoneinfo(config.timezone))
    cutoff_date = moment.date() - timedelta(days=max(0, LOG_RETENTION_DAYS - 1))
    for path in config.log_dir.glob(f"{ROTATED_LOG_PREFIX}*.log"):
        suffix = path.name.removeprefix(ROTATED_LOG_PREFIX).removesuffix(".log")
        try:
            log_date = datetime.fromisoformat(suffix).date()
        except ValueError:
            continue
        if log_date < cutoff_date:
            try:
                path.unlink()
            except OSError:
                continue


def plan_rank(plan: str | None) -> int:
    normalized = (plan or "").lower()
    if normalized in PRO_20X_PLANS:
        return 5
    if normalized in PRO_5X_PLANS:
        return 4
    if normalized in PLUS_PLANS:
        return 3
    if normalized in TEAM_PLANS:
        return 2
    if normalized == "free":
        return 1
    return 0


def is_paid_plan(plan: str | None) -> bool:
    return (plan or "").lower() in PAID_PLANS


def get_window_seconds(window: dict[str, Any] | None) -> int | None:
    if not isinstance(window, dict):
        return None
    value = normalize_number(window.get("limit_window_seconds"))
    if value is None:
        value = normalize_number(window.get("limitWindowSeconds"))
    return int(value) if value is not None else None


def classify_windows(limit_info: dict[str, Any] | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not isinstance(limit_info, dict):
        return None, None
    primary = limit_info.get("primary_window") or limit_info.get("primaryWindow")
    secondary = limit_info.get("secondary_window") or limit_info.get("secondaryWindow")
    windows = []
    if isinstance(primary, dict):
        windows.append(primary)
    if isinstance(secondary, dict):
        windows.append(secondary)
    five_hour = None
    weekly = None
    for window in windows:
        seconds = get_window_seconds(window)
        if seconds == 18_000 and five_hour is None:
            five_hour = window
        elif seconds == 604_800 and weekly is None:
            weekly = window
    if five_hour is None and isinstance(primary, dict) and primary is not weekly:
        five_hour = primary
    if weekly is None and isinstance(secondary, dict) and secondary is not five_hour:
        weekly = secondary
    return five_hour, weekly


def resolve_reset_at(window: dict[str, Any] | None) -> datetime | None:
    if not isinstance(window, dict):
        return None
    reset_at = normalize_number(window.get("reset_at"))
    if reset_at is None:
        reset_at = normalize_number(window.get("resetAt"))
    if reset_at is None:
        reset_after = normalize_number(window.get("reset_after_seconds"))
        if reset_after is None:
            reset_after = normalize_number(window.get("resetAfterSeconds"))
        if reset_after is None:
            return None
        return utc_now() + timedelta(seconds=max(0.0, reset_after))
    if reset_at > 1_000_000_000_000:
        reset_at /= 1000.0
    return datetime.fromtimestamp(reset_at, tz=UTC)


def extract_remaining_percent(window: dict[str, Any] | None, limit_info: dict[str, Any] | None) -> float | None:
    if not isinstance(window, dict):
        return None
    used = normalize_number(window.get("used_percent"))
    if used is None:
        used = normalize_number(window.get("usedPercent"))
    limit_reached = False
    allowed = True
    if isinstance(limit_info, dict):
        limit_reached = boolish(limit_info.get("limit_reached")) or boolish(limit_info.get("limitReached"))
        allowed_value = limit_info.get("allowed")
        if allowed_value is not None:
            allowed = boolish(allowed_value)
    if used is None and (limit_reached or not allowed):
        used = 100.0
    if used is None:
        return None
    return max(0.0, min(100.0, 100.0 - used))


def tier_name(plan: str | None) -> str:
    normalized = (plan or "").lower()
    if normalized in PRO_20X_PLANS:
        return "pro20x"
    if normalized in PRO_5X_PLANS:
        return "pro5x"
    if normalized in PLUS_PLANS:
        return "plus"
    if normalized in TEAM_PLANS:
        return "team"
    if normalized == "free":
        return "free"
    return "unknown"


def is_week_started(plan: str | None, weekly_window: dict[str, Any] | None) -> bool:
    if not is_paid_plan(plan):
        return False
    return isinstance(weekly_window, dict)


def is_week_not_started(plan: str | None, weekly_window: dict[str, Any] | None) -> bool:
    if not is_paid_plan(plan):
        return False
    return not is_week_started(plan, weekly_window)


def is_quota_exhausted(plan: str | None, five_hour_remaining: float | None, weekly_remaining: float | None) -> bool:
    normalized = (plan or "").lower()
    if normalized in PAID_PLANS:
        if five_hour_remaining is not None and five_hour_remaining <= 0:
            return True
        if weekly_remaining is not None and weekly_remaining <= 0:
            return True
        return False
    if normalized == "free":
        return weekly_remaining is not None and weekly_remaining <= 0
    return False


def derive_health_state(
    *,
    runtime_status: str,
    disabled: bool,
    unavailable: bool,
    live_state: str,
    live_plan: str | None,
    five_hour_remaining: float | None,
    weekly_remaining: float | None,
    weekly_window: dict[str, Any] | None,
) -> str:
    if disabled or runtime_status == "disabled":
        return "disabled"
    if unavailable:
        return "runtime_unavailable"
    if live_state in {"invalidated", "deactive", "live_error"}:
        return live_state
    if is_quota_exhausted(live_plan, five_hour_remaining, weekly_remaining):
        return "quota_exhausted"
    if is_week_not_started(live_plan, weekly_window):
        return "week_not_started"
    return "healthy_active"


def routing_eligible(state: str) -> bool:
    return state not in NON_ROUTABLE_STATES


def calculate_priority(
    *,
    state: str,
    live_plan: str | None,
    weekly_reset_at: datetime | None,
    config: Config,
) -> int:
    if state in NON_ROUTABLE_STATES:
        return -100
    if state == "week_not_started" and is_paid_plan(live_plan):
        return 10_000
    rank = plan_rank(live_plan)
    if weekly_reset_at is None:
        return rank * 100
    timezone = safe_zoneinfo(config.timezone)
    local_now = utc_now().astimezone(timezone)
    local_reset = weekly_reset_at.astimezone(timezone)
    days_until_reset = max(0, (local_reset.date() - local_now.date()).days)
    return (rank * 100) + max(0, 90 - min(days_until_reset, 90))


def make_prime_cycle_marker(config: Config, when: datetime) -> str:
    local = when.astimezone(safe_zoneinfo(config.timezone))
    iso_year, iso_week, _ = local.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def current_priority(raw_priority: Any) -> int | None:
    if isinstance(raw_priority, bool):
        return int(raw_priority)
    if isinstance(raw_priority, int):
        return raw_priority
    if isinstance(raw_priority, float) and raw_priority.is_integer():
        return int(raw_priority)
    if isinstance(raw_priority, str) and re.fullmatch(r"[+-]?\d+", raw_priority.strip()):
        return int(raw_priority.strip())
    return None


def local_auth_path(config: Config, filename: str) -> Path:
    return config.auth_dir / filename


def prime_state_entry(history: dict[str, Any], filename: str) -> dict[str, Any]:
    entries = history.setdefault("prime", {})
    if not isinstance(entries, dict):
        history["prime"] = {}
        entries = history["prime"]
    entry = entries.get(filename)
    if not isinstance(entry, dict):
        entry = {}
        entries[filename] = entry
    return entry


def status_state_entry(history: dict[str, Any], filename: str) -> dict[str, Any]:
    entries = history.setdefault("status", {})
    if not isinstance(entries, dict):
        history["status"] = {}
        entries = history["status"]
    entry = entries.get(filename)
    if not isinstance(entry, dict):
        entry = {}
        entries[filename] = entry
    return entry


def record_event(
    events: list[dict[str, Any]],
    event_type: str,
    *,
    account: str | None = None,
    detail: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    event = {
        "type": event_type,
        "account": account,
        "detail": detail,
    }
    if payload:
        event.update(payload)
    events.append(event)


def build_account_row(
    item: dict[str, Any],
    local_record: dict[str, Any] | None,
) -> dict[str, Any]:
    filename = normalize_string(item.get("name") or item.get("file_name") or item.get("fileName")) or "-"
    provider = normalize_string(item.get("provider") or item.get("type")) or ""
    runtime_status = (normalize_string(item.get("status")) or "unknown").lower()
    auth_index = normalize_string(item.get("auth_index") or item.get("authIndex"))
    email = normalize_string(item.get("email"))
    if not email and local_record:
        email = normalize_string(local_record.get("email"))
    account_id = resolve_account_id(item, local_record)
    return {
        "name": filename,
        "provider": provider.lower(),
        "email": email or "-",
        "runtime_status": runtime_status,
        "disabled": boolish(item.get("disabled")) or boolish((local_record or {}).get("disabled")),
        "disabled_before": boolish(item.get("disabled")) or boolish((local_record or {}).get("disabled")),
        "unavailable": boolish(item.get("unavailable")),
        "auth_index": auth_index,
        "account_id": account_id,
        "priority_before": current_priority(item.get("priority")),
        "priority_after": None,
        "live_plan": None,
        "plan_tier": "unknown",
        "live_state": "service_down",
        "live_error": None,
        "five_hour_remaining": None,
        "weekly_remaining": None,
        "five_hour_reset_at": None,
        "weekly_reset_at": None,
        "week_started": False,
        "week_not_started": False,
        "health_state": "service_down",
        "invalidated_count": 0,
        "prime_attempted": False,
        "prime_success": False,
        "prime_error": None,
        "primed_at": None,
        "priority_changed": False,
    }


def enrich_usage_row(config: Config, row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    auth_index = normalize_string(updated.get("auth_index"))
    account_id = normalize_string(updated.get("account_id"))
    if not auth_index:
        updated["live_state"] = "live_error"
        updated["live_error"] = "missing auth_index"
        return updated
    if not account_id:
        updated["live_state"] = "live_error"
        updated["live_error"] = "missing chatgpt_account_id"
        return updated

    try:
        usage = fetch_usage_payload(config, auth_index, account_id)
    except Exception as exc:
        message = str(exc)
        if ":" in message:
            state, detail = message.split(":", 1)
        else:
            state, detail = "live_error", message
        updated["live_state"] = state
        updated["live_error"] = detail or message
        return updated

    plan = normalize_string(usage.get("plan_type") or usage.get("planType"))
    rate_limit = usage.get("rate_limit") or usage.get("rateLimit")
    main_five, main_week = classify_windows(rate_limit if isinstance(rate_limit, dict) else None)
    updated["live_plan"] = (plan or "").lower() or None
    updated["plan_tier"] = tier_name(updated["live_plan"])
    updated["five_hour_remaining"] = extract_remaining_percent(
        main_five,
        rate_limit if isinstance(rate_limit, dict) else None,
    )
    updated["weekly_remaining"] = extract_remaining_percent(
        main_week,
        rate_limit if isinstance(rate_limit, dict) else None,
    )
    updated["five_hour_reset_at"] = iso_from_datetime(resolve_reset_at(main_five))
    updated["weekly_reset_at"] = iso_from_datetime(resolve_reset_at(main_week))
    updated["week_started"] = is_week_started(updated["live_plan"], main_week)
    updated["week_not_started"] = is_week_not_started(updated["live_plan"], main_week)
    updated["live_state"] = "ok"
    updated["health_state"] = derive_health_state(
        runtime_status=str(updated.get("runtime_status") or ""),
        disabled=bool(updated.get("disabled")),
        unavailable=bool(updated.get("unavailable")),
        live_state="ok",
        live_plan=updated.get("live_plan"),
        five_hour_remaining=updated.get("five_hour_remaining"),
        weekly_remaining=updated.get("weekly_remaining"),
        weekly_window=main_week,
    )
    return updated


def maybe_prime_row(config: Config, row: dict[str, Any], history: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    updated = dict(row)
    if config.dry_run or config.no_prime or not config.prime_enabled:
        return updated
    if updated.get("health_state") != "week_not_started":
        return updated
    if not is_paid_plan(updated.get("live_plan")):
        return updated

    auth_index = normalize_string(updated.get("auth_index"))
    account_id = normalize_string(updated.get("account_id"))
    if not auth_index or not account_id:
        return updated

    entry = prime_state_entry(history, updated["name"])
    cycle_marker = make_prime_cycle_marker(config, utc_now())
    if normalize_string(entry.get("success_cycle")) == cycle_marker:
        return updated
    failure_until = parse_iso_datetime(entry.get("failure_until"))
    if failure_until and failure_until > utc_now():
        return updated

    updated["prime_attempted"] = True
    try:
        run_prime_request(config, auth_index, account_id)
        updated["prime_success"] = True
        updated["primed_at"] = utc_now_iso()
        entry["success_cycle"] = cycle_marker
        entry["failure_until"] = None
        entry["last_error"] = None
        refreshed = enrich_usage_row(config, updated)
        refreshed["prime_attempted"] = True
        refreshed["prime_success"] = True
        refreshed["primed_at"] = updated["primed_at"]
        record_event(
            events,
            "prime_success",
            account=updated["name"],
            detail=f"primed {updated['email']} via authIndex {auth_index}",
        )
        return refreshed
    except Exception as exc:
        updated["prime_error"] = str(exc)
        entry["last_error"] = str(exc)
        entry["failure_until"] = iso_from_datetime(
            utc_now() + timedelta(seconds=config.prime_failure_cooldown_seconds)
        )
        record_event(
            events,
            "prime_failed",
            account=updated["name"],
            detail=str(exc),
        )
        return updated


def rewrite_priority_if_needed(config: Config, row: dict[str, Any], local_record: dict[str, Any] | None) -> bool:
    if config.dry_run or local_record is None:
        return False
    filename = normalize_string(row.get("name"))
    if not filename:
        return False
    path = local_auth_path(config, filename)
    if not path.is_file():
        return False
    target_priority = row.get("priority_after")
    target_disabled = bool(row.get("disabled"))

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(data, dict):
        return False

    changed = False
    if current_priority(data.get("priority")) != target_priority:
        data["priority"] = target_priority
        changed = True
    if boolish(data.get("disabled")) != target_disabled:
        data["disabled"] = target_disabled
        changed = True
    if not changed:
        return False

    serialized = json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n"
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(serialized)
    os.replace(tmp_path, path)
    return True


def low_plus_hook_notice(config: Config) -> str | None:
    if not config.low_plus_hook:
        return None
    if not config.low_plus_hook_enabled:
        return f"hook configured but disabled: {config.low_plus_hook}"
    return f"hook configured but not executed by design: {config.low_plus_hook}"


def available_plan_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "plus": 0,
        "pro20": 0,
        "pro5": 0,
    }
    for row in rows:
        if not routing_eligible(str(row.get("health_state") or "")):
            continue
        live_plan = normalize_string(row.get("live_plan")) or ""
        if live_plan in PRO_20X_PLANS:
            counts["pro20"] += 1
        elif live_plan in PRO_5X_PLANS:
            counts["pro5"] += 1
        elif live_plan in PLUS_PLANS:
            counts["plus"] += 1
    return counts


def update_row_status_history(
    history: dict[str, Any],
    row: dict[str, Any],
    *,
    previously_disabled: bool,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    updated = dict(row)
    name = normalize_string(updated.get("name")) or "-"
    entry = status_state_entry(history, name)
    live_state = normalize_string(updated.get("live_state")) or "unknown"
    prior_invalidated = parse_positive_int(normalize_string(entry.get("invalidated_count")), 0)

    if live_state == "invalidated":
        invalidated_count = prior_invalidated + 1
        entry["invalidated_count"] = invalidated_count
        updated["invalidated_count"] = invalidated_count
        updated["health_state"] = "invalidated"
        if invalidated_count >= 2:
            updated["disabled"] = True
            if not previously_disabled:
                record_event(
                    events,
                    "account_disabled",
                    account=name,
                    detail=f"disabled after {invalidated_count} consecutive invalidated checks",
                )
    else:
        entry["invalidated_count"] = 0
        updated["invalidated_count"] = 0
        if live_state == "deactive":
            updated["disabled"] = True
            updated["health_state"] = "deactive"
            if not previously_disabled:
                record_event(
                    events,
                    "account_disabled",
                    account=name,
                    detail="disabled immediately because upstream returned deactive",
                )

    entry["last_live_state"] = live_state
    entry["last_health_state"] = normalize_string(updated.get("health_state")) or "unknown"
    entry["last_seen_at"] = utc_now_iso()
    entry["disabled"] = bool(updated.get("disabled"))
    return updated


def send_telegram(config: Config, text: str) -> None:
    if not config.tg_bot_token or not config.tg_chat_id:
        raise RuntimeError("Telegram is not configured")
    url = f"https://api.telegram.org/bot{config.tg_bot_token}/sendMessage"
    payload = {
        "chat_id": config.tg_chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    request_plain_json(url, payload, timeout=config.timeout)


def snapshot_from_rows(
    *,
    config: Config,
    rows: list[dict[str, Any]],
    events: list[dict[str, Any]],
    service_state: str,
) -> dict[str, Any]:
    counts = available_plan_counts(rows)
    return {
        "generated_at": utc_now_iso(),
        "timezone": config.timezone,
        "service_state": service_state,
        "plus_threshold": config.plus_threshold,
        "pro20_threshold": config.pro20_threshold,
        "pro5_threshold": config.pro5_threshold,
        "plus_count": counts["plus"],
        "pro20_count": counts["pro20"],
        "pro5_count": counts["pro5"],
        "prime_enabled": config.prime_enabled and not config.no_prime,
        "rows": rows,
        "events": events,
    }


def compare_snapshots(previous: dict[str, Any], current: dict[str, Any], config: Config) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    previous_state = normalize_string(previous.get("service_state")) or "unknown"
    current_state = normalize_string(current.get("service_state")) or "unknown"
    if previous_state != current_state:
        record_event(events, "service_state_changed", detail=f"{previous_state} -> {current_state}")

    threshold_checks = (
        ("plus", "plus_count", config.plus_threshold, low_plus_hook_notice(config)),
        ("pro20", "pro20_count", config.pro20_threshold, None),
        ("pro5", "pro5_count", config.pro5_threshold, None),
    )
    for label, count_key, threshold, notice in threshold_checks:
        if threshold <= 0:
            continue
        previous_count = int(previous.get(count_key) or 0)
        current_count = int(current.get(count_key) or 0)
        if previous_count >= threshold > current_count:
            detail = f"live {label} count dropped below threshold: {current_count} < {threshold}"
            if notice:
                detail = f"{detail}; {notice}"
            record_event(events, f"low_{label}_threshold", detail=detail)
        elif previous_count < threshold <= current_count:
            record_event(
                events,
                f"low_{label}_recovered",
                detail=f"live {label} count recovered: {current_count} >= {threshold}",
            )

    previous_rows = {
        normalize_string(item.get("name")) or "": item
        for item in previous.get("rows", [])
        if isinstance(item, dict)
    }
    for row in current.get("rows", []):
        if not isinstance(row, dict):
            continue
        name = normalize_string(row.get("name")) or ""
        if not name:
            continue
        old = previous_rows.get(name, {})
        new_state = normalize_string(row.get("health_state")) or "unknown"
        old_state = normalize_string(old.get("health_state")) or "unknown"
        if old_state != new_state and new_state in ALERTABLE_STATES:
            record_event(
                events,
                "account_state_changed",
                account=name,
                detail=f"{old_state} -> {new_state}",
            )
        old_disabled = boolish(old.get("disabled"))
        new_disabled = boolish(row.get("disabled"))
        if not old_disabled and new_disabled:
            disable_reason = normalize_string(row.get("live_state")) or new_state
            record_event(
                events,
                "account_disabled",
                account=name,
                detail=f"disabled due to {disable_reason}",
            )
        new_plan = normalize_string(row.get("live_plan")) or "-"
        old_plan = normalize_string(old.get("live_plan")) or "-"
        if old_plan != new_plan:
            record_event(
                events,
                "live_plan_changed",
                account=name,
                detail=f"{old_plan} -> {new_plan}",
            )
        if row.get("prime_attempted") and not row.get("prime_success") and row.get("prime_error"):
            record_event(
                events,
                "prime_failed",
                account=name,
                detail=normalize_string(row.get("prime_error")) or "prime failed",
            )
    return events


def format_alert_text(snapshot: dict[str, Any], change_events: list[dict[str, Any]]) -> str:
    lines = [
        "CLIProxyAPI Codex monitor",
        f"time: {snapshot.get('generated_at')}",
        f"service: {snapshot.get('service_state')}",
        (
            "counts: "
            f"plus {snapshot.get('plus_count')} / {snapshot.get('plus_threshold')}, "
            f"pro20 {snapshot.get('pro20_count')} / {snapshot.get('pro20_threshold')}, "
            f"pro5 {snapshot.get('pro5_count')} / {snapshot.get('pro5_threshold')}"
        ),
    ]
    for event in change_events:
        label = normalize_string(event.get("type")) or "event"
        account = normalize_string(event.get("account"))
        detail = normalize_string(event.get("detail")) or "-"
        if account:
            lines.append(f"- {label}: {account} | {detail}")
        else:
            lines.append(f"- {label}: {detail}")
    return "\n".join(lines[:40])


def collect_rows(config: Config, history: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    local_records = load_local_auth_records(config.auth_dir)
    events: list[dict[str, Any]] = []
    try:
        files = fetch_auth_files(config)
    except Exception as exc:
        return [], [{"type": "service_state_changed", "detail": str(exc)}], "service_down"

    rows = []
    candidate_rows = []
    for item in files:
        provider = (normalize_string(item.get("provider") or item.get("type")) or "").lower()
        if provider != "codex":
            continue
        filename = normalize_string(item.get("name") or item.get("file_name") or item.get("fileName"))
        local_record = local_records.get(filename or "")
        row = build_account_row(item, local_record)
        candidate_rows.append((row, local_record))

    workers = max(1, min(config.workers, len(candidate_rows) or 1))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(enrich_usage_row, config, row): (row, local_record)
            for row, local_record in candidate_rows
        }
        for future in concurrent.futures.as_completed(future_map):
            original_row, local_record = future_map[future]
            try:
                row = future.result()
            except Exception as exc:
                row = dict(original_row)
                row["live_state"] = "live_error"
                row["live_error"] = str(exc)
                row["health_state"] = "live_error"
            rows.append((row, local_record))

    rows.sort(key=lambda item: (normalize_string(item[0].get("email")) or "", normalize_string(item[0].get("name")) or ""))

    updated_rows: list[dict[str, Any]] = []
    for row, local_record in rows:
        if row.get("health_state") == "week_not_started":
            row = maybe_prime_row(config, row, history, events)

        if row.get("live_state") != "ok":
            row["health_state"] = derive_health_state(
                runtime_status=str(row.get("runtime_status") or ""),
                disabled=bool(row.get("disabled")),
                unavailable=bool(row.get("unavailable")),
                live_state=str(row.get("live_state") or "live_error"),
                live_plan=normalize_string(row.get("live_plan")),
                five_hour_remaining=row.get("five_hour_remaining"),
                weekly_remaining=row.get("weekly_remaining"),
                weekly_window={"dummy": True} if bool(row.get("week_started")) else None,
            )

        row = update_row_status_history(
            history,
            row,
            previously_disabled=bool((local_record or {}).get("disabled")) or bool(row.get("disabled")),
            events=events,
        )
        weekly_reset = parse_iso_datetime(row.get("weekly_reset_at"))
        row["priority_after"] = calculate_priority(
            state=str(row.get("health_state") or ""),
            live_plan=normalize_string(row.get("live_plan")),
            weekly_reset_at=weekly_reset,
            config=config,
        )
        row["priority_changed"] = row.get("priority_before") != row.get("priority_after")

        rewrite_priority_if_needed(config, row, local_record)
        updated_rows.append(row)

    visible_rows = updated_rows
    if not config.include_disabled:
        visible_rows = [
            row
            for row in updated_rows
            if not (bool(row.get("disabled_before")) and bool(row.get("disabled")))
        ]

    cleaned_rows = []
    for row in visible_rows:
        cleaned = dict(row)
        cleaned.pop("disabled_before", None)
        cleaned_rows.append(cleaned)

    return cleaned_rows, events, "ok"


def with_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError(f"monitor already running; lock busy at {lock_path}")
    return handle


def single_run(config: Config, *, print_json: bool) -> int:
    ensure_directories(config)
    snapshot_path = config.state_dir / SNAPSHOT_FILENAME
    history_path = config.state_dir / HISTORY_FILENAME
    log_path = monitor_log_path(config)
    previous_snapshot = load_json_file(snapshot_path, default={})
    history = load_json_file(history_path, default={})

    rows, run_events, service_state = collect_rows(config, history)
    snapshot = snapshot_from_rows(config=config, rows=rows, events=run_events, service_state=service_state)
    change_events = compare_snapshots(previous_snapshot, snapshot, config)
    if service_state == "ok":
        snapshot["events"] = run_events + change_events
    else:
        snapshot["events"] = change_events or run_events

    append_jsonl(
        log_path,
        {
            "timestamp": snapshot["generated_at"],
            "service_state": service_state,
            "plus_count": snapshot["plus_count"],
            "plus_threshold": snapshot["plus_threshold"],
            "events": snapshot["events"],
        },
    )
    prune_monitor_logs(config)

    if not config.dry_run:
        write_json_file(snapshot_path, snapshot)
        write_json_file(history_path, history)

    if not config.dry_run and change_events and config.tg_bot_token and config.tg_chat_id:
        try:
            send_telegram(config, format_alert_text(snapshot, change_events))
        except Exception as exc:
            append_jsonl(
                log_path,
                {
                    "timestamp": utc_now_iso(),
                    "service_state": service_state,
                    "events": [{"type": "telegram_failed", "detail": str(exc)}],
                },
            )
            log(f"Telegram alert failed: {exc}", error=True)

    if print_json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    else:
        log(
            "monitor run complete: "
            f"service={service_state} plus={snapshot['plus_count']} "
            f"threshold={snapshot['plus_threshold']} events={len(snapshot['events'])}"
        )
    return 0 if service_state == "ok" else 1


def main() -> int:
    args = parse_args()
    config = resolve_config(args)
    ensure_directories(config)
    lock_handle = None
    try:
        lock_handle = with_lock(config.state_dir / LOCK_FILENAME)
    except Exception as exc:
        log(str(exc), error=True)
        return 1

    try:
        if config.loop:
            while True:
                config = resolve_config(args)
                ensure_directories(config)
                exit_code = single_run(config, print_json=args.json)
                if args.once:
                    return exit_code
                sleep_seconds = max(5, config.scan_interval_seconds)
                time.sleep(sleep_seconds)
        return single_run(config, print_json=args.json)
    finally:
        if lock_handle is not None:
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
