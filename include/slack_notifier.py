"""
include/slack_notifier.py
--------------------------
Modular Slack notifier for the Data Pipeline to ML Model project.
Sends formatted alert messages to a Slack channel via Incoming Webhooks.

Setup:
  1. Go to https://api.slack.com/apps -> Create App -> Incoming Webhooks -> Activate
  2. Add webhook to workspace and copy Webhook URL.
  3. Configure in Airflow:
     - Admin -> Variables -> Add Key: SLACK_WEBHOOK_URL
     - OR set environment variable: SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
     - OR pass directly in airflow_settings.yaml.

Falls back gracefully to console logging if no webhook URL is configured.
"""

import os
import json
import urllib.request
import logging

logger = logging.getLogger("airflow.task")


def send_slack_alert(message: str, webhook_url: str = None) -> bool:
    """
    Sends message to Slack via Incoming Webhook.
    Returns True if successfully sent, False otherwise.
    Falls back to console output if webhook is unconfigured (safe for offline evaluation).
    """
    # 1. Resolve URL from argument, Airflow Variable, or environment variable
    url = webhook_url
    if not url:
        try:
            from airflow.models import Variable
            url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
        except Exception:
            url = None

    if not url:
        url = os.getenv("SLACK_WEBHOOK_URL")

    # 2. Console fallback if no webhook configured
    if not url:
        divider = "=" * 65
        print("\n" + divider)
        print("[Slack Notifier] No SLACK_WEBHOOK_URL configured.")
        print("[Slack Notifier] Console fallback alert output:")
        print(divider)
        print(message)
        print(divider + "\n")
        return False

    # 3. HTTP POST to Slack Webhook
    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        url.strip(),
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            print(f"[Slack Notifier] Alert successfully delivered to Slack. HTTP {status}")
            return True
    except Exception as e:
        print(f"[Slack Notifier] Failed to deliver alert to Slack: {e}")
        return False
