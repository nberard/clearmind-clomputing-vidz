"""grid_benchmark: run one benchmark on the grid, wait for it to finish, then
load the results and rebuild the dbt models.

Trigger it from the Airflow UI with params:
  preset    demo | cpu_only | full
  simulate  true  -> Airflow launches simulated devices with `plan`
            false -> you add real devices yourself (phones via QR, PCs via agent)
  plan      e.g. "phone,phone+phone,oldpc,laptop,gaming" (simulated only)
  step      seconds between simulated devices joining

Running it several times with different plans builds the scaling curve in
BigQuery (mart_scaling_curve).
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param

try:  # Airflow 3
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:  # Airflow 2
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT_ROOT = os.environ.get("VIBEGEN_ROOT", "/opt/vibegen")


def cfg(name: str, default: str | None = None) -> str | None:
    return Variable.get(name, default_var=None) or os.environ.get(name, default)


def api(method: str, path: str, **kw):
    r = requests.request(method, cfg("GRID_SERVER").rstrip("/") + path,
                         headers={"X-Grid-Token": cfg("GRID_TOKEN")}, timeout=30, **kw)
    r.raise_for_status()
    return r.json()


@dag(
    dag_id="grid_benchmark",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vibegen", "benchmark"],
    doc_md=__doc__,
    params={
        "preset": Param("cpu_only", enum=["demo", "cpu_only", "full"]),
        "simulate": Param(True, type="boolean"),
        "plan": Param("phone,phone+phone,oldpc,laptop,gaming", type="string"),
        "step": Param(30, type="integer", minimum=5),
        "timeout_minutes": Param(60, type="integer", minimum=1),
    },
)
def grid_benchmark():
    @task
    def run_benchmark(params=None) -> dict:
        p = params
        if p["simulate"]:
            # simulate.py resets metrics, starts the run, adds devices and waits for the end
            cmd = [sys.executable, f"{PROJECT_ROOT}/scripts/simulate.py",
                   "--server", cfg("GRID_SERVER"), "--token", cfg("GRID_TOKEN"),
                   "--preset", p["preset"], "--plan", p["plan"], "--step", str(p["step"])]
            subprocess.run(cmd, check=True, timeout=p["timeout_minutes"] * 60)
        else:
            api("POST", "/api/runs", json={"preset": p["preset"]})
            deadline = time.time() + p["timeout_minutes"] * 60
            while time.time() < deadline:
                if (api("GET", "/api/stats").get("run") or {}).get("finished"):
                    break
                time.sleep(15)
            else:
                raise TimeoutError("run did not finish in time")
        stats = api("GET", "/api/stats")
        return {"run": stats["run"]["id"], "seconds": round(stats["elapsed"]), "stages": len(stats["stages"])}

    load_and_model = TriggerDagRunOperator(
        task_id="trigger_grid_elt",
        trigger_dag_id="grid_elt",
        wait_for_completion=True,
        poke_interval=20,
    )

    run_benchmark() >> load_and_model


grid_benchmark()
