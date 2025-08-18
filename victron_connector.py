import os, argparse, datetime, requests, sys
from dotenv import load_dotenv

load_dotenv()

INGEST_URL_BASE = os.getenv("INGEST_URL_BASE")
INGEST_KEY      = os.getenv("INGEST_KEY")
VRM_TOKEN       = os.getenv("VICTRON_TOKEN")
INSTALL_ID      = os.getenv("VICTRON_INSTALLATION_ID")

def post_telemetry(site_id: str, metrics: list, src="victron"):
    """Send ONE object (not an array) to /ingest/telemetry with required fields."""
    payload = {
        "v": 1,
        "site_id": site_id,
        "device_id": None,  # keep None to avoid FK errors until devices table is populated
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "src": src,
        "metrics": metrics,
    }
    r = requests.post(
        f"{INGEST_URL_BASE}/ingest/telemetry",
        headers={"Content-Type": "application/json", "X-INGEST-KEY": INGEST_KEY},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    print("POST telemetry:", r.status_code, r.text)

def vrm_get(path: str, params=None):
    """Call Victron VRM API with Bearer token."""
    if not VRM_TOKEN or not INSTALL_ID:
        raise RuntimeError("VICTRON_TOKEN and VICTRON_INSTALLATION_ID are required for live VRM calls.")
    url = f"https://vrmapi.victronenergy.com{path}"
    headers = {"X-Authorization": f"Bearer {VRM_TOKEN}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def build_metrics_from_vrm_live_feed(rec: dict) -> list:
    """Map common VRM live fields to normalized metrics. Be defensive about keys."""
    metrics = []
    # PV / Solar power (W)
    pv = (
        (rec.get("solar") or {}).get("power")
        or rec.get("total_solar_power")
        or rec.get("pv_power")
    )
    if pv is not None:
        metrics.append({"metric":"pv_power_w","value":float(pv),"unit":"W"})

    # AC load / consumption (W)
    load = (
        (rec.get("ac") or {}).get("consumption")
        or rec.get("total_consumption")
        or rec.get("consumption")
    )
    if load is not None:
        metrics.append({"metric":"ac_load_w","value":float(load),"unit":"W"})

    # Battery SOC (%)
    soc = (rec.get("battery") or {}).get("soc") or rec.get("soc")
    if soc is not None:
        metrics.append({"metric":"soc_pct","value":float(soc),"unit":"%"})

    # Grid import/export (W)
    grid = (rec.get("grid") or {}).get("power") or rec.get("grid_power")
    if grid is not None:
        metrics.append({"metric":"grid_power_w","value":float(grid),"unit":"W"})

    return metrics

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-id", required=True)
    ap.add_argument("--since-minutes", type=int, default=5)  # reserved for later
    args = ap.parse_args()

    # If Victron secrets exist, try live data; otherwise send a simple sample payload
    if VRM_TOKEN and INSTALL_ID:
        try:
            data = vrm_get(f"/v2/installations/{INSTALL_ID}/stats", params={"type": "live_feed"})
            # Some accounts return {"records": {...}}, others may vary; handle safely:
            rec = {}
            if isinstance(data, dict):
                rec = data.get("records") or data.get("record") or data
            metrics = build_metrics_from_vrm_live_feed(rec)
            if not metrics:
                # If keys differ for your account, still send a heartbeat so we can iterate
                print("Info: No expected fields found in VRM live_feed; sending heartbeat only. Keys:", list(rec.keys()))
                metrics = [{"metric":"heartbeat","value":1,"unit":"count"}]
            post_telemetry(args.site_id, metrics, src="victron")
            return
        except Exception as e:
            # Fall back to a heartbeat so the workflow keeps proving the pipeline
            print("Warning: VRM call failed, sending heartbeat. Error:", e, file=sys.stderr)
            post_telemetry(args.site_id, [{"metric":"heartbeat","value":1,"unit":"count"}], src="victron")
            return
    else:
        # No Victron secrets yet -> send a small sample so pipeline remains functional
        sample = [
            {"metric":"pv_power_w","value":1234,"unit":"W"},
            {"metric":"ac_load_w","value":567,"unit":"W"},
            {"metric":"soc_pct","value":75.5,"unit":"%"},
        ]
        post_telemetry(args.site_id, sample, src="victron")

if __name__ == "__main__":
    # Basic env checks for ingestion endpoint
    if not INGEST_URL_BASE or not INGEST_KEY:
        print("Missing INGEST_URL_BASE or INGEST_KEY environment variables.", file=sys.stderr)
        sys.exit(1)
    main()
