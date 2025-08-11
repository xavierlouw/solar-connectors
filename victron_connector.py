import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

INGEST_URL_BASE = os.getenv("INGEST_URL_BASE")
INGEST_KEY = os.getenv("INGEST_KEY")
VICTRON_TOKEN = os.getenv("VICTRON_TOKEN")
VICTRON_INSTALLATION_ID = os.getenv("VICTRON_INSTALLATION_ID")

def fetch_victron_data(site_id, since_minutes):
    # Placeholder: Replace with real Victron API calls
    now = datetime.utcnow()
    since_time = now - timedelta(minutes=since_minutes)
    data = [
        {
            "ts": now.isoformat(),
            "site_id": site_id,
            "metric": "ac_power",
            "value": 5000,
            "unit": "W",
            "src": "victron",
            "device_id": "victron-vebus-001"
        }
    ]
    return data

def send_to_ingest(data):
    url = f"{INGEST_URL_BASE}/ingest/telemetry"
    headers = {"x-api-key": INGEST_KEY}
    r = requests.post(url, json=data, headers=headers)
    print("Ingest response:", r.status_code, r.text)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--site-id", required=True)
    parser.add_argument("--since-minutes", type=int, default=5)
    args = parser.parse_args()
    data = fetch_victron_data(args.site_id, args.since_minutes)
    send_to_ingest(data)
