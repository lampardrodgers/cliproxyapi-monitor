# CLIProxyAPI Monitor

A small standalone monitor for Codex accounts behind CLIProxyAPI.

It runs as a sidecar service, not in the main request path. Its job is to check account health, count available account types, alert when thresholds drop, and adjust local auth-file priority based on live usage data.

## What It Does

- Checks account status on a fixed interval
- Detects `active`, `deactive`, `invalidated`, and quota exhaustion
- Counts available `plus`, `pro20x`, `pro5x`, `team`, and `free` accounts
- Sends Telegram alerts when thresholds are crossed
- Gives highest priority to paid accounts whose weekly window has not started
- Optionally sends one tiny priming request to start the weekly window for paid accounts
- Writes snapshot, history, and daily rotated logs

## Main Files

- `scripts/codex_account_monitor.py`
  The main monitor process
- `scripts/show_codex_account_counts.py`
  Real-time account count summary
- `cliproxyapi-codex-monitor.service`
  Example systemd service
- `scripts/install_codex_monitor_service.sh`
  Installs the service and config template
- `scripts/cliproxyapi-codex-monitor.conf.example`
  Example config file

## Requirements

- Python 3
- A running local CLIProxyAPI instance
- Access to CLIProxyAPI management API
- Codex auth files already present on the host

## Install

1. Copy the repo to your server
2. Turn the example config into a real config file
3. Fill in at least `CPA_MANAGEMENT_KEY`
4. Optionally fill in Telegram settings
5. Install the service

```bash
bash scripts/install_codex_monitor_service.sh
```

## Create The Real Config File

The example file is:

```bash
scripts/cliproxyapi-codex-monitor.conf.example
```

Turn it into the real runtime config with:

```bash
cp scripts/cliproxyapi-codex-monitor.conf.example /etc/cliproxyapi-codex-monitor.conf
```

Then edit it:

```bash
vim /etc/cliproxyapi-codex-monitor.conf
```

After changing the config, restart the monitor:

```bash
systemctl restart cliproxyapi-codex-monitor
```

## Config

Example config:

```bash
CPA_MANAGEMENT_KEY=
SCAN_INTERVAL_SECONDS=300
PLUS_THRESHOLD=4
PRO20_THRESHOLD=0
PRO5_THRESHOLD=0
TIMEZONE=Asia/Shanghai
TG_BOT_TOKEN=
TG_CHAT_ID=
```

Threshold meaning:

- `PLUS_THRESHOLD`: alert when available plus accounts drop below this number
- `PRO20_THRESHOLD`: alert when available `pro` accounts drop below this number
- `PRO5_THRESHOLD`: alert when available `prolite` accounts drop below this number
- `0` means disabled

## Common Commands

Run one manual check:

```bash
python3 scripts/codex_account_monitor.py --once --json
```

Run count summary:

```bash
python3 scripts/show_codex_account_counts.py --settings-file /etc/cliproxyapi-codex-monitor.conf
```

Service control:

```bash
systemctl start cliproxyapi-codex-monitor
systemctl stop cliproxyapi-codex-monitor
systemctl restart cliproxyapi-codex-monitor
systemctl status cliproxyapi-codex-monitor
```

View logs:

```bash
journalctl -u cliproxyapi-codex-monitor -f
ls -l logs/codex-monitor-*.log
```

## Priority Rules

- Non-routable accounts get `-100`
- Paid accounts with `week_not_started` get the highest priority bucket
- Other accounts are ranked by plan tier plus weekly reset proximity

## Safety

This repository is intended to contain code and examples only.

Do not commit:

- real `config.yaml`
- live auth JSON files
- Telegram tokens
- management keys
- API keys
- snapshots, logs, or backups

## More Detail

See `CODEX_MONITOR.md` for the full monitor behavior and deployment notes.
