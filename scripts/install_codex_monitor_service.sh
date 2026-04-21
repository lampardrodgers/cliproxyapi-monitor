#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="cliproxyapi-codex-monitor"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_PATH="$REPO_DIR/scripts/codex_account_monitor.py"
SERVICE_SOURCE="$REPO_DIR/cliproxyapi-codex-monitor.service"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
CONFIG_EXAMPLE="$REPO_DIR/scripts/cliproxyapi-codex-monitor.conf.example"
CONFIG_PATH="/etc/cliproxyapi-codex-monitor.conf"
LEGACY_SETTINGS="/etc/show-codex-quotas.conf"

if [[ ! -f "$SCRIPT_PATH" ]]; then
  echo "monitor script not found: $SCRIPT_PATH" >&2
  exit 1
fi

if [[ ! -f "$SERVICE_SOURCE" ]]; then
  echo "service template not found: $SERVICE_SOURCE" >&2
  exit 1
fi

install -m 0644 "$SERVICE_SOURCE" "$SERVICE_PATH"

if [[ ! -f "$CONFIG_PATH" ]]; then
  install -m 0600 "$CONFIG_EXAMPLE" "$CONFIG_PATH"
  if [[ -f "$LEGACY_SETTINGS" ]]; then
    legacy_key="$(awk -F= '/^(CPA_MANAGEMENT_KEY|CLIPROXYAPI_MANAGEMENT_KEY|MANAGEMENT_PASSWORD)=/ {print substr($0, index($0, "=") + 1); exit}' "$LEGACY_SETTINGS")"
    if [[ -n "${legacy_key:-}" ]]; then
      sed -i "s|^CPA_MANAGEMENT_KEY=$|CPA_MANAGEMENT_KEY=${legacy_key}|" "$CONFIG_PATH"
      echo "Copied management key from $LEGACY_SETTINGS"
    fi
  fi
  echo "Created $CONFIG_PATH from example. Fill in Telegram settings if needed."
else
  echo "Keeping existing config: $CONFIG_PATH"
fi

systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}.service"

echo "Installed and started ${SERVICE_NAME}.service"
systemctl status "${SERVICE_NAME}.service" --no-pager -l | sed -n '1,80p'
