# Databricks notebook source
import requests
import json

SLACK_WEBHOOK_URL = "<your_slack_webhook_url>"

def send_alert(message, level="INFO"):

    if level == "ERROR":
        emoji = "🚨"
    elif level == "WARNING":
        emoji = "⚠️"
    else:
        emoji = "ℹ️"

    payload = {
        "text": f"{emoji} {message}"
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200:
            print(f"Slack failed: {response.text}")

    except Exception as e:
        print(f"Slack error: {e}")