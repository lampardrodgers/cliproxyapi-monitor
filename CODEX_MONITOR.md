# CLIProxyAPI Codex Monitor

`scripts/codex_account_monitor.py` is a standalone sidecar monitor for Codex auth files. It runs outside the main request path, can be started or stopped independently, and does not add latency to normal proxy traffic.

## What It Does

- Polls the local management API on a fixed interval. Default is `300` seconds.
- Tracks Codex account runtime state, live plan type, 5-hour quota, weekly quota, and weekly reset status.
- Sends alerts when service state changes, live `plus` / `pro20x` / `pro5x` count drops below the configured threshold, or an account becomes `deactive` / `invalidated`.
- Rewrites local auth-file `priority` and `disabled` fields based on live state.
- Stores snapshots and monitor history under `state/`, and daily JSONL run logs under `logs/`.

## State Rules

- `deactive`: immediately marked non-routable and written as `disabled: true`.
- `invalidated`: immediately marked non-routable; only after 2 consecutive monitor cycles will the auth file be written as `disabled: true`.
- `quota_exhausted`, `service_down`, `runtime_unavailable`, `live_error`: non-routable, but not auto-disabled.

## Priority Rules

- Paid accounts with `week_not_started` get the highest priority bucket.
- `week_not_started` only applies to `pro`, `prolite`, `plus`, and `team`.
- `free` accounts are never primed and never treated as `week_not_started`.
- Other routable accounts are ordered by plan tier plus weekly reset day urgency.
- Same-day weekly resets stay in the same day bucket.

## Weekly Priming

Priming is not `codex` CLI execution. The monitor sends one tiny management `/api-call` directly to the upstream Codex endpoint for a target `authIndex`, with a payload equivalent to:

```json
{
  "model": "gpt-5.4-mini",
  "input": [
    {
      "role": "user",
      "content": [
        {"type": "input_text", "text": "Reply with OK."}
      ]
    }
  ],
  "max_output_tokens": 1,
  "stream": false
}
```

Rules:

- Only for `pro`, `prolite`, `plus`, `team`.
- Only when the weekly window has not started yet.
- At most once per account per weekly cycle.
- After a successful prime, the monitor refreshes usage and recalculates priority immediately.

## Config

Example config file: `scripts/cliproxyapi-codex-monitor.conf.example`

Recommended active file:

```bash
/etc/cliproxyapi-codex-monitor.conf
```

Important keys:

- `CPA_MANAGEMENT_KEY`
- `SCAN_INTERVAL_SECONDS`
- `PLUS_THRESHOLD`
- `PRO20_THRESHOLD`
- `PRO5_THRESHOLD`
- `TIMEZONE`
- `TG_BOT_TOKEN`
- `TG_CHAT_ID`
- `PRIME_DISABLED`
- `CODEX_PRIME_MODEL`
- `CODEX_PRIME_URL`

`LOW_PLUS_HOOK` is reserved for later. In the current version it is only reported in alerts and is not executed.

## Manual Run

```bash
python3 scripts/codex_account_monitor.py --dry-run --json
python3 scripts/codex_account_monitor.py --once
python3 scripts/codex_account_monitor.py --loop
```

## Install As Service

```bash
bash scripts/install_codex_monitor_service.sh
```

Useful commands:

```bash
systemctl status cliproxyapi-codex-monitor
systemctl restart cliproxyapi-codex-monitor
systemctl stop cliproxyapi-codex-monitor
journalctl -u cliproxyapi-codex-monitor -f
```

## Files

- Snapshot: `/root/cliproxyapi/state/codex-monitor-snapshot.json`
- History: `/root/cliproxyapi/state/codex-monitor-history.json`
- Log files: `/root/cliproxyapi/logs/codex-monitor-YYYY-MM-DD.log`
