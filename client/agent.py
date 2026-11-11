"""vibegen-grid client agent.

Install on any PC or Android phone (Termux), point it at the server, and it
contributes to the pipeline:

    python agent.py --server https://grid.example.com --token XXXX

It benchmarks the device, registers, then loops: pull a job it can run,
run it, report the result.

--simulate <profile> fakes a device (phone, oldpc, laptop, gaming...) so you
can rehearse the whole "0 -> phone -> phones -> PC" story on one machine.
"""

import argparse
import os
import platform
import random
import signal
import socket
import sys
import threading
import time

import requests

# Work calibration: 1 unit = about 1 second on a reference device (score 100).
REF_CPU_MATMULS = 5000   # 256x256 float32 matmuls per CPU unit
REF_GPU_MATMULS = 2000   # 1024x1024 float32 matmuls per GPU unit

# Simulated devices. Scores are relative speeds (100 = reference).
PROFILES = {
    "phone":   {"kind": "phone",   "score": 20,  "native": False, "gpu": False},
    "oldpc":   {"kind": "oldpc",   "score": 12,  "native": True,  "gpu": False},
    "laptop":  {"kind": "laptop",  "score": 70,  "native": True,  "gpu": False},
    "desktop": {"kind": "desktop", "score": 110, "native": True,  "gpu": False},
    "gaming":  {"kind": "gaming",  "score": 160, "native": True,  "gpu": True,
                "gpu_name": "RTX 3070 (sim)", "vram_gb": 8, "gpu_score": 100},
    "gpu_server": {"kind": "server", "score": 200, "native": True, "gpu": True,
                   "gpu_name": "L4 (sim)", "vram_gb": 24, "gpu_score": 140},
}


# ---------- capability detection ----------

def cpu_benchmark(seconds: float = 2.0) -> float:
    import numpy as np
    a = np.random.rand(256, 256).astype(np.float32)
    n, t0 = 0, time.perf_counter()
    while time.perf_counter() - t0 < seconds:
        a @ a
        n += 1
    return round(n / (time.perf_counter() - t0) / REF_CPU_MATMULS * 100, 1)


def gpu_info():
    try:
        import torch
    except ImportError:
        return {"gpu": False}
    if torch.cuda.is_available():
        dev = "cuda"
        name = torch.cuda.get_device_name(0)
        vram = torch.cuda.get_device_properties(0).total_memory / 2**30
    elif getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        dev, name = "mps", "Apple GPU"
        vram = ram_gb() * 0.6  # unified memory
    else:
        return {"gpu": False}
    a = torch.rand(1024, 1024, device=dev)
    for _ in range(10):
        a @ a
    sync(torch, dev)
    n, t0 = 0, time.perf_counter()
    while time.perf_counter() - t0 < 2:
        for _ in range(20):
            a @ a
        sync(torch, dev)
        n += 20
    score = n / (time.perf_counter() - t0) / REF_GPU_MATMULS * 100
    return {"gpu": True, "gpu_name": name, "vram_gb": round(vram, 1),
            "gpu_score": round(score, 1), "gpu_device": dev}


def sync(torch, dev):
    if dev == "cuda":
        torch.cuda.synchronize()
    elif dev == "mps":
        torch.mps.synchronize()


def ram_gb() -> float:
    try:
        import psutil
        return round(psutil.virtual_memory().total / 2**30, 1)
    except ImportError:
        return 0.0


def guess_kind(caps: dict) -> str:
    if "ANDROID_ROOT" in os.environ or "com.termux" in sys.prefix:
        return "phone"
    if caps.get("gpu") and caps.get("vram_gb", 0) >= 6:
        return "gaming"
    if caps.get("score", 0) < 25:
        return "oldpc"
    return "laptop" if hasattr(os, "getloadavg") and platform.system() == "Darwin" else "desktop"


def detect(profile: str | None) -> tuple[str, dict]:
    if profile:
        p = dict(PROFILES[profile])
        kind = p.pop("kind")
        p.update(simulated=True, cores=0, ram_gb=0)
        return kind, p
    print("Benchmarking this device…")
    caps = {"score": cpu_benchmark(), "native": True, "cores": os.cpu_count(),
            "ram_gb": ram_gb(), "os": platform.platform(terse=True)}
    caps.update(gpu_info())
    return guess_kind(caps), caps


# ---------- job execution ----------
# These are synthetic workloads sized like the real pipeline steps, so you can
# measure scaling before installing any AI model. Replace a handler with the
# real step (librosa, CLIP, Flux, Wan, ffmpeg...) and the rest stays the same.

def run_cpu(units: float):
    import numpy as np
    a = np.random.rand(256, 256).astype(np.float32)
    for _ in range(int(units * REF_CPU_MATMULS)):
        a @ a


def run_gpu(units: float, dev: str):
    import torch
    a = torch.rand(1024, 1024, device=dev)
    n = int(units * REF_GPU_MATMULS)
    for i in range(n):
        a @ a
        if i % 200 == 0:
            sync(torch, dev)
    sync(torch, dev)


GPU_TYPES = {"gen_image", "gen_music", "gen_video"}


def execute(job: dict, caps: dict):
    units = job["units"]
    gpu_job = job["type"] in GPU_TYPES
    if caps.get("simulated"):
        speed = caps["gpu_score"] if gpu_job else caps["score"]
        time.sleep(units * 100 / speed * random.uniform(0.9, 1.1))
    elif gpu_job:
        run_gpu(units, caps["gpu_device"])
    else:
        run_cpu(units)
    return {}


# ---------- main loop ----------

class Agent:
    def __init__(self, server, token, name, kind, caps):
        self.server, self.name, self.kind, self.caps = server.rstrip("/"), name, kind, caps
        self.http = requests.Session()
        self.http.headers["X-Grid-Token"] = token
        self.cid = None
        self.stop = threading.Event()

    def post(self, path, **body):
        r = self.http.post(self.server + path, json=body, timeout=30)
        r.raise_for_status()
        return r.json()

    def register(self):
        self.cid = self.post("/api/register", name=self.name, kind=self.kind, caps=self.caps)["client_id"]
        print(f"Joined grid as '{self.name}' ({self.kind}) caps={self.caps}")

    def heartbeat_loop(self):
        while not self.stop.wait(5):
            try:
                self.post("/api/heartbeat", client_id=self.cid)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    self.register()
            except requests.RequestException:
                pass

    def run(self):
        self.register()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        idle = 1.0
        while True:
            try:
                job = self.post("/api/pull", client_id=self.cid)["job"]
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    self.register()
                    continue
                raise
            except requests.RequestException as e:
                print("server unreachable, retrying:", e)
                time.sleep(5)
                continue
            if not job:
                time.sleep(idle)
                idle = min(idle * 1.5, 5)
                continue
            idle = 1.0
            t0 = time.perf_counter()
            try:
                result, ok = execute(job, self.caps), True
            except Exception as e:  # report failure so the job is requeued
                print("job failed:", e)
                result, ok = {"error": str(e)}, False
            secs = time.perf_counter() - t0
            print(f"{job['type']:<14} {job['units']:>6} units  {secs:6.1f}s")
            self.post("/api/done", client_id=self.cid, job_id=job["id"], ok=ok, seconds=secs, result=result)


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=os.environ.get("GRID_SERVER", "http://localhost:8000"))
    ap.add_argument("--token", default=os.environ.get("GRID_TOKEN"), required=not os.environ.get("GRID_TOKEN"))
    ap.add_argument("--name", default=socket.gethostname()[:20])
    ap.add_argument("--kind", choices=["phone", "laptop", "desktop", "oldpc", "gaming", "server"])
    ap.add_argument("--simulate", choices=list(PROFILES))
    a = ap.parse_args()
    kind, caps = detect(a.simulate)
    agent = Agent(a.server, a.token, a.name, a.kind or kind, caps)
    try:
        agent.run()
    finally:
        # tell the server right away so the dashboard shows the device leaving
        if agent.cid:
            try:
                agent.post("/api/leave", client_id=agent.cid)
            except requests.RequestException:
                pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
