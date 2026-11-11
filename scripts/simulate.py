"""Rehearse the story on one machine: start a run, then add simulated devices
one at a time so the dashboard shows each jump in throughput.

    python scripts/simulate.py --server http://localhost:8000 --token XXXX

Default sequence (every --step seconds):
  1 phone -> +2 phones -> old PC -> laptop -> gaming PC
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

import requests

AGENT = Path(__file__).resolve().parent.parent / "client" / "agent.py"
DEFAULT_PLAN = "phone,phone+phone,oldpc,laptop,gaming"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="http://localhost:8000")
    ap.add_argument("--token", required=True)
    ap.add_argument("--preset", default="demo")
    ap.add_argument("--step", type=float, default=30, help="seconds between steps")
    ap.add_argument("--plan", default=DEFAULT_PLAN,
                    help="comma-separated steps; use + to add several devices at once")
    ap.add_argument("--wait-first", type=float, default=8, help="seconds with 0 devices at the start")
    a = ap.parse_args()

    h = {"X-Grid-Token": a.token}
    requests.post(f"{a.server}/api/reset", headers=h).raise_for_status()
    requests.post(f"{a.server}/api/runs", json={"preset": a.preset}, headers=h).raise_for_status()
    print(f"Run started. {a.wait_first:.0f}s with no device…")
    time.sleep(a.wait_first)

    procs, counts = [], {}
    try:
        for step in a.plan.split(","):
            for profile in step.split("+"):
                counts[profile] = counts.get(profile, 0) + 1
                name = f"{profile}-{counts[profile]}"
                procs.append(subprocess.Popen(
                    [sys.executable, str(AGENT), "--server", a.server, "--token", a.token,
                     "--simulate", profile, "--name", name],
                    stdout=subprocess.DEVNULL))
                print("+", name)
            time.sleep(a.step)
        print("All devices added. Waiting for the run to finish (Ctrl+C to stop)…")
        while not requests.get(f"{a.server}/api/stats").json()["run"].get("finished"):
            time.sleep(2)
        print("Run finished.")
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
