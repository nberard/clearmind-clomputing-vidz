"""vibegen-grid coordinator.

Holds the job queue, hands jobs to clients that can run them, and records
metrics so the dashboard can show how throughput changes as devices join.
Everything lives in memory; finished runs are saved to data/history.json.
"""

import asyncio
import json
import os
import random
import secrets
import time
import uuid
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

TOKEN = os.environ.get("GRID_TOKEN") or secrets.token_urlsafe(8)
DATA_DIR = Path(os.environ.get("GRID_DATA", "data"))
STATIC = Path(__file__).parent / "static"
OFFLINE_AFTER = 20  # seconds without heartbeat before a client is dropped
RATE_WINDOW = 30  # seconds used for the rolling throughput

# Each job type mirrors a real pipeline step. "units" is the work size:
# 1 unit is about 1 second on a reference machine (score 100).
JOB_TYPES = {
    "ingest":        {"label": "Ingest",          "units": 2,   "req": {}},
    "analyze_audio": {"label": "Music analysis",  "units": 4,   "req": {}},
    "caption_image": {"label": "Image captions",  "units": 8,   "req": {}},
    "gen_image":     {"label": "Character images", "units": 30, "req": {"gpu": True, "vram_gb": 6}},
    "gen_music":     {"label": "Music generation", "units": 60, "req": {"gpu": True, "vram_gb": 8}},
    "gen_video":     {"label": "Video clips",     "units": 120, "req": {"gpu": True, "vram_gb": 8}},
    "encode_chunk":  {"label": "Video encoding",  "units": 10,  "req": {"native": True}},
}

PRESETS = {
    # counts per job type, plus a scale that shrinks every job (for quick demos)
    "demo": {"scale": 0.25, "counts": {"ingest": 30, "analyze_audio": 30, "caption_image": 40,
                                       "gen_image": 12, "gen_music": 5, "gen_video": 20, "encode_chunk": 20}},
    "cpu_only": {"scale": 0.25, "counts": {"ingest": 40, "analyze_audio": 60, "caption_image": 80,
                                           "encode_chunk": 30}},
    "full": {"scale": 1.0, "counts": {"ingest": 60, "analyze_audio": 60, "caption_image": 200,
                                      "gen_image": 30, "gen_music": 5, "gen_video": 240, "encode_chunk": 40}},
}

KIND_ICON = {"phone": "📱", "browser": "🌐", "laptop": "💻", "desktop": "🖥️",
             "oldpc": "🧓", "gaming": "🎮", "server": "☁️"}

app = FastAPI(title="vibegen-grid")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


class State:
    def __init__(self):
        self.clients: dict[str, dict] = {}
        self.jobs: dict[str, dict] = {}
        self.queue: list[str] = []
        self.run: dict | None = None
        self.events: list[dict] = []
        self.samples: list[dict] = []
        self.completions: list[tuple[float, float]] = []  # (time, units)
        self.lock = asyncio.Lock()


S = State()


def now() -> float:
    return time.time()


def check_token(x_grid_token: str | None = Header(None), token: str | None = Query(None)):
    if (x_grid_token or token) != TOKEN:
        raise HTTPException(401, "bad token")


def online_clients():
    return [c for c in S.clients.values() if c["online"]]


# ---------- raw log for the data warehouse ----------
# Every record is appended to data/<table>.jsonl. The ELT job (elt/extract.py,
# run by Airflow) reads them through /api/export and loads them into BigQuery.
EXPORT_TABLES = ("runs", "clients", "events", "jobs", "samples")


def persist(table: str, rec: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / f"{table}.jsonl", "a") as f:
        f.write(json.dumps(rec, separators=(",", ":")) + "\n")


def add_event(kind: str, client: dict | None = None, text: str = ""):
    S.events.append({
        "t": now(), "type": kind, "text": text,
        "client": client["id"] if client else None,
        "name": client["name"] if client else None,
        "kind": client["kind"] if client else None,
        "icon": KIND_ICON.get(client["kind"], "❔") if client else None,
        "online": len(online_clients()),
    })
    e = S.events[-1]
    persist("events", {"run_id": S.run["id"] if S.run else None, "t": e["t"], "type": kind,
                       "client_id": e["client"], "online": e["online"], "text": text})


def can_run(caps: dict, req: dict) -> bool:
    if req.get("gpu") and not caps.get("gpu"):
        return False
    if caps.get("vram_gb", 0) < req.get("vram_gb", 0):
        return False
    if req.get("native") and not caps.get("native"):
        return False
    return True


def rate_units_per_min(window: float = RATE_WINDOW) -> float:
    cutoff = now() - window
    units = sum(u for t, u in S.completions if t >= cutoff)
    return units * 60 / window


# ---------- client API ----------

class RegisterIn(BaseModel):
    name: str
    kind: str = "desktop"
    caps: dict = {}


@app.post("/api/register", dependencies=[Depends(check_token)])
async def register(body: RegisterIn):
    async with S.lock:
        cid = uuid.uuid4().hex[:8]
        c = {"id": cid, "name": body.name[:32], "kind": body.kind, "caps": body.caps,
             "joined": now(), "left": None, "last_seen": now(), "online": True, "job": None,
             "done": 0, "units": 0.0, "busy_s": 0.0}
        S.clients[cid] = c
        persist("clients", {"client_id": cid, "name": c["name"], "kind": c["kind"],
                            "joined": c["joined"], "caps": c["caps"]})
        add_event("join", c)
        return {"client_id": cid}


class ClientIn(BaseModel):
    client_id: str


def touch(cid: str) -> dict:
    c = S.clients.get(cid)
    if not c:
        raise HTTPException(404, "unknown client, register again")
    c["last_seen"] = now()
    if not c["online"]:
        c["online"] = True
        c["left"] = None
        add_event("join", c, "reconnected")
    return c


@app.post("/api/heartbeat", dependencies=[Depends(check_token)])
async def heartbeat(body: ClientIn):
    async with S.lock:
        c = touch(body.client_id)
        if c["job"] and c["job"] in S.jobs:
            S.jobs[c["job"]]["lease"] = now() + 60
        return {"ok": True}


@app.post("/api/leave", dependencies=[Depends(check_token)])
async def leave(body: ClientIn):
    async with S.lock:
        c = S.clients.get(body.client_id)
        if c and c["online"]:
            drop_client(c)
        return {"ok": True}


def drop_client(c: dict):
    c["online"] = False
    c["left"] = now()
    if c["job"] in S.jobs and S.jobs[c["job"]]["state"] == "running":
        requeue(S.jobs[c["job"]])
    c["job"] = None
    add_event("leave", c)


@app.post("/api/pull", dependencies=[Depends(check_token)])
async def pull(body: ClientIn):
    async with S.lock:
        c = touch(body.client_id)
        caps = c["caps"]
        candidates = [S.jobs[j] for j in S.queue if can_run(caps, S.jobs[j]["req"])]
        if not candidates:
            return {"job": None}
        # GPU clients take GPU work first; everyone else takes the oldest job.
        if caps.get("gpu"):
            gpu_jobs = [j for j in candidates if j["req"].get("gpu")]
            candidates = gpu_jobs or candidates
        job = candidates[0]
        S.queue.remove(job["id"])
        if S.run and not S.queue and not S.run.get("drained"):
            S.run["drained"] = now()  # no work left to hand out: devices start idling
        score = caps.get("gpu_score" if job["req"].get("gpu") else "score", 100) or 100
        expected = job["units"] * 100 / score
        job.update(state="running", client=c["id"], started=now(), lease=now() + max(60, expected * 3))
        c["job"] = job["id"]
        return {"job": {k: job[k] for k in ("id", "type", "units", "payload")}}


class DoneIn(BaseModel):
    client_id: str
    job_id: str
    ok: bool = True
    seconds: float = 0
    result: dict = {}


@app.post("/api/done", dependencies=[Depends(check_token)])
async def done(body: DoneIn):
    async with S.lock:
        c = touch(body.client_id)
        job = S.jobs.get(body.job_id)
        c["job"] = None
        if not job or job["client"] != c["id"] or job["state"] != "running":
            return {"ok": False, "reason": "job was reassigned"}
        if not body.ok:
            requeue(job)
            return {"ok": True}
        t = now()
        job.update(state="done", finished=t, seconds=body.seconds or t - job["started"], result=body.result)
        c["done"] += 1
        c["units"] += job["units"]
        c["busy_s"] += job["seconds"]
        S.completions.append((t, job["units"]))
        persist("jobs", {"run_id": S.run["id"] if S.run else None, "job_id": job["id"], "type": job["type"],
                         "units": job["units"], "gpu_job": bool(job["req"].get("gpu")), "client_id": c["id"],
                         "started": job["started"], "finished": t, "seconds": job["seconds"]})
        maybe_finish_run()
        return {"ok": True}


def requeue(job: dict):
    job.update(state="queued", client=None, started=None)
    if S.run:
        S.run["drained"] = None
    S.queue.insert(0, job["id"])


def maybe_finish_run():
    r = S.run
    if r and not r.get("finished") and all(S.jobs[j]["state"] == "done" for j in r["jobs"]):
        r["finished"] = now()
        add_event("run_done", text=f"Run finished in {r['finished'] - r['started']:.0f}s")
        persist("runs", {"run_id": r["id"], "preset": r["preset"], "started": r["started"],
                         "finished": r["finished"], "drained": r.get("drained") or r["finished"],
                         "total_units": r["total_units"], "jobs": len(r["jobs"])})
        save_history()


# ---------- admin API ----------

class RunIn(BaseModel):
    preset: str = "demo"


@app.post("/api/runs", dependencies=[Depends(check_token)])
async def start_run(body: RunIn):
    if body.preset not in PRESETS:
        raise HTTPException(400, f"presets: {list(PRESETS)}")
    async with S.lock:
        p = PRESETS[body.preset]
        # Drop whatever is left of the previous run.
        for j in list(S.queue):
            S.jobs.pop(j, None)
        S.queue.clear()
        S.completions.clear()
        S.samples.clear()
        S.events.clear()
        S.clients = {k: c for k, c in S.clients.items() if c["online"]}
        for c in S.clients.values():
            c.update(done=0, units=0.0, busy_s=0.0)
            if c["job"] not in S.jobs:
                c["job"] = None
        ids = []
        for jtype, n in p["counts"].items():
            spec = JOB_TYPES[jtype]
            for i in range(n):
                jid = uuid.uuid4().hex[:10]
                S.jobs[jid] = {"id": jid, "type": jtype, "units": round(spec["units"] * p["scale"], 2),
                               "req": spec["req"], "payload": {"index": i}, "state": "queued",
                               "client": None, "started": None}
                ids.append(jid)
        random.shuffle(ids)  # mixed work, like a real pipeline with many inputs
        S.queue.extend(ids)
        S.run = {"id": uuid.uuid4().hex[:6], "preset": body.preset, "started": now(),
                 "finished": None, "jobs": ids,
                 "total_units": sum(S.jobs[j]["units"] for j in ids)}
        add_event("run_start", text=f"Run '{body.preset}' started: {len(ids)} jobs")
        return {"run": S.run["id"], "jobs": len(ids)}


@app.post("/api/reset", dependencies=[Depends(check_token)])
async def reset():
    async with S.lock:
        S.jobs.clear(); S.queue.clear(); S.run = None
        S.events.clear(); S.samples.clear(); S.completions.clear()
        S.clients = {k: c for k, c in S.clients.items() if c["online"]}
        for c in S.clients.values():
            c.update(done=0, units=0.0, busy_s=0.0, job=None)
        return {"ok": True}


# ---------- stats for the dashboard ----------

def compute_stages():
    """Split the run into stages: a new stage starts whenever a device joins or
    leaves. Each stage's throughput skips its first 30% (jobs that started in
    the previous stage are still finishing) and the run's idle tail."""
    r = S.run
    if not r:
        return []
    start = r["started"]
    end = r.get("drained") or r.get("finished") or now()
    marks = [e for e in S.events if e["type"] in ("join", "leave") and start < e["t"] < end]
    bounds = [start] + [e["t"] for e in marks] + [end]
    stages = []
    for i in range(len(bounds) - 1):
        a, b = bounds[i], bounds[i + 1]
        if b - a < 1:
            continue  # several devices joined at once: merge into the next stage
        m = a + (b - a) * 0.3
        units = sum(u for t, u in S.completions if m <= t < b)
        roster = [c for c in S.clients.values()
                  if c["joined"] <= a + 0.5 and (c["left"] is None or c["left"] >= b - 0.5)]
        ev = marks[i - 1] if i > 0 else None
        stages.append({
            "start": round(a - start, 1), "end": round(b - start, 1),
            "label": "start" if not ev else ("+ " if ev["type"] == "join" else "− ") + (ev["name"] or ""),
            "clients": len(roster),
            "icons": "".join(KIND_ICON.get(c["kind"], "❔") for c in roster),
            "units_per_min": round(units * 60 / (b - m), 2),
        })
    return stages


def snapshot():
    r = S.run
    by_type = {}
    if r:
        for jid in r["jobs"]:
            j = S.jobs[jid]
            t = by_type.setdefault(j["type"], {"label": JOB_TYPES[j["type"]]["label"], "total": 0,
                                               "done": 0, "running": 0, "gpu": bool(j["req"].get("gpu")),
                                               "eligible": 0})
            t["total"] += 1
            t["done"] += j["state"] == "done"
            t["running"] += j["state"] == "running"
        for jt, t in by_type.items():
            t["eligible"] = sum(can_run(c["caps"], JOB_TYPES[jt]["req"]) for c in online_clients())
    done_units = sum(u for _, u in S.completions)
    rate = rate_units_per_min()
    remaining = (r["total_units"] - done_units) if r else 0
    blocked = [t["label"] for t in by_type.values() if t["done"] + t["running"] < t["total"] and not t["eligible"]]
    eta = None if blocked or rate <= 0 else remaining / rate * 60
    return {
        "server_time": now(),
        "run": {k: v for k, v in r.items() if k != "jobs"} if r else None,
        "elapsed": ((r.get("finished") or now()) - r["started"]) if r else 0,
        "done_units": round(done_units, 1),
        "rate": round(rate, 2),
        "eta": eta,
        "blocked": blocked,
        "by_type": by_type,
        "clients": [{
            "id": c["id"], "name": c["name"], "kind": c["kind"], "icon": KIND_ICON.get(c["kind"], "❔"),
            "online": c["online"], "busy": c["job"] is not None,
            "job_type": S.jobs[c["job"]]["type"] if c["job"] in S.jobs else None,
            "done": c["done"], "units": round(c["units"], 1),
            "score": c["caps"].get("score"), "gpu": c["caps"].get("gpu_name") if c["caps"].get("gpu") else None,
        } for c in S.clients.values() if c["online"] or c["done"]],
        "events": S.events[-30:],
        "samples": S.samples[-900:],
        "stages": compute_stages(),
    }


@app.get("/api/stats")
async def stats():
    async with S.lock:
        return snapshot()


@app.get("/api/history")
async def history():
    f = DATA_DIR / "history.json"
    return json.loads(f.read_text()) if f.exists() else []


def save_history():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    f = DATA_DIR / "history.json"
    hist = json.loads(f.read_text()) if f.exists() else []
    r = S.run
    hist.append({
        "run": r["id"], "preset": r["preset"], "started": r["started"],
        "seconds": r["finished"] - r["started"], "total_units": r["total_units"],
        "stages": compute_stages(),
        "clients": [{"name": c["name"], "kind": c["kind"], "done": c["done"], "units": c["units"],
                     "busy_s": round(c["busy_s"], 1), "caps": c["caps"]}
                    for c in S.clients.values() if c["done"]],
    })
    f.write_text(json.dumps(hist, indent=1))


@app.get("/api/export/{table}", dependencies=[Depends(check_token)])
async def export(table: str, offset: int = 0, limit: int = 5000):
    """Incremental export: rows from line `offset`. Each row carries `_line`
    so the loader can resume from the highest line already in BigQuery."""
    if table not in EXPORT_TABLES:
        raise HTTPException(404, f"tables: {EXPORT_TABLES}")
    f = DATA_DIR / f"{table}.jsonl"
    rows = []
    if f.exists():
        with open(f) as fh:
            for i, line in enumerate(fh):
                if i < offset:
                    continue
                if len(rows) >= limit:
                    break
                rows.append({"_line": i, **json.loads(line)})
    return {"table": table, "rows": rows, "next_offset": offset + len(rows), "more": len(rows) == limit}


# ---------- pages ----------

@app.get("/")
async def index():
    return RedirectResponse("/static/dashboard.html")


@app.get("/join")
async def join_page():
    return FileResponse(STATIC / "join.html")


# ---------- background loop ----------

async def ticker():
    while True:
        await asyncio.sleep(1)
        async with S.lock:
            t = now()
            for c in S.clients.values():
                if c["online"] and t - c["last_seen"] > OFFLINE_AFTER:
                    drop_client(c)
            for j in S.jobs.values():
                if j["state"] == "running" and t > j["lease"]:
                    requeue(j)
            if S.run and not S.run.get("finished"):
                S.samples.append({"t": round(t - S.run["started"], 1), "rate": round(rate_units_per_min(), 2),
                                  "clients": len(online_clients()),
                                  "busy": sum(1 for c in online_clients() if c["job"])})
                if len(S.samples) % 5 == 0:  # one row every 5 s is plenty for the warehouse
                    persist("samples", {"run_id": S.run["id"], "t": t, **S.samples[-1]})
            # keep memory bounded on long runs
            if len(S.completions) > 50000:
                S.completions = S.completions[-50000:]


@app.on_event("startup")
async def startup():
    print(f"\n  vibegen-grid token: {TOKEN}\n  dashboard: /?token={TOKEN}\n  phone join: /join?token={TOKEN}\n")
    asyncio.create_task(ticker())
