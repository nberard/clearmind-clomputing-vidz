"""Instagram collector: copies the posts saved in your collections to the grid.

Run it at home (gaming PC, Raspberry Pi): a home IP looks like normal use,
a datacenter IP (OVH) gets flagged much faster.

    python collector.py login                 # once: interactive login, saves the session
    python collector.py init                  # once: everything saved up to now (slow, with pauses)
    python collector.py sync                  # only posts saved since the last sync
    python collector.py daily                 # loop: one sync per day at a random time 02:00-06:00

Settings: --server / GRID_SERVER, --token / GRID_TOKEN, IG_USERNAME, IG_PASSWORD.

Safety: read-only (never likes/follows), one saved session (no re-login
loops), random delays, and automatic stops:
  - security check or logged out   -> stop, alert, wait for you to log in again
  - "try again later" / rate limit -> pause 24 h; a second time -> full stop
"""

import argparse
import base64
import getpass
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

STATE_DIR = Path(os.environ.get("COLLECTOR_STATE", Path.home() / ".vibegen-collector"))
SESSION = STATE_DIR / "session.json"
STATE = STATE_DIR / "state.json"
SYNC_CAP = 200  # max posts per collection in a daily sync (safety net)


# ---------- local state ----------

def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {"collections": {}, "seen": [], "paused_until": 0, "strikes": 0, "needs_login": False}


def save_state(st: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(st, indent=1))


# ---------- grid server ----------

class Grid:
    def __init__(self, server: str, token: str):
        self.server, self.h = server.rstrip("/"), {"X-Grid-Token": token}

    def send(self, items: list[dict], process: bool) -> dict:
        out = {"added": 0, "duplicates": 0}
        for i in range(0, len(items), 25):  # small batches: images are inside
            r = requests.post(f"{self.server}/api/sources", headers=self.h, timeout=120,
                              json={"items": items[i:i + 25], "process": process and i + 25 >= len(items)})
            r.raise_for_status()
            res = r.json()
            out["added"] += res["added"]
            out["duplicates"] += res["duplicates"]
            out["run"] = res.get("run")
        return out

    def alert(self, text: str):
        print("ALERT:", text)
        try:
            requests.post(f"{self.server}/api/alert", headers=self.h, json={"text": text}, timeout=15)
        except requests.RequestException:
            pass


# ---------- Instagram ----------

class Stop(Exception):
    """Safety stop: do not touch Instagram again until the condition is cleared."""


def client():
    from instagrapi import Client
    cl = Client()
    cl.delay_range = [3, 8]  # random pause between requests, like a person scrolling
    return cl


def login_interactive():
    cl = client()
    if SESSION.exists():
        cl.load_settings(SESSION)  # keep the same device fingerprint
    user = os.environ.get("IG_USERNAME") or input("Instagram username: ")
    pw = os.environ.get("IG_PASSWORD") or getpass.getpass("Password: ")
    cl.challenge_code_handler = lambda username, choice: input(f"Code sent by {choice} to {username}: ")
    cl.login(user, pw)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    cl.dump_settings(SESSION)
    os.chmod(SESSION, 0o600)
    st = load_state()
    st.update(needs_login=False, strikes=0, paused_until=0)
    save_state(st)
    print("Logged in, session saved to", SESSION)


def open_session():
    """Reuse the saved session. Never logs in again automatically: a re-login
    loop is one of the fastest ways to get an account flagged."""
    if not SESSION.exists():
        raise Stop("no saved session: run `python collector.py login` first")
    from instagrapi.exceptions import LoginRequired
    cl = client()
    cl.load_settings(SESSION)
    try:
        cl.account_info()
    except LoginRequired:
        raise Stop("session expired: run `python collector.py login`")
    return cl


def classify(e: Exception) -> str:
    """'login' = security check / logged out, 'slow' = rate limited, '' = other."""
    name = type(e).__name__
    if name in ("ChallengeRequired", "ChallengeUnknownStep", "LoginRequired", "SelectContactPointRecoveryForm",
                "RecaptchaChallengeForm", "BadPassword", "TwoFactorRequired"):
        return "login"
    if name in ("PleaseWaitFewMinutes", "RateLimitError", "ClientThrottledError", "FeedbackRequired",
                "SentryBlock"):
        return "slow"
    return ""


def thumb_b64(m) -> str | None:
    url = m.thumbnail_url or (m.resources[0].thumbnail_url if getattr(m, "resources", None) else None)
    if not url:
        return None
    r = requests.get(str(url), timeout=30)
    return base64.b64encode(r.content).decode() if r.ok else None


def record(m, coll) -> dict:
    return {
        "media_id": str(m.pk), "code": m.code,
        "collection_id": str(coll.id), "collection_name": coll.name,
        "media_type": m.media_type,
        "taken_at": m.taken_at.timestamp() if m.taken_at else None,
        "author": m.user.username if m.user else None,
        "caption": (m.caption_text or "")[:500],
        "like_count": m.like_count,
        "collected_at": time.time(),
        "image_b64": thumb_b64(m),
    }


def collect(mode: str, only: list[str], max_per_collection: int) -> list[dict]:
    cl = open_session()
    st = load_state()
    seen = set(st["seen"])
    items = []
    collections = [c for c in cl.collections() if not only or c.name in only]
    print(f"{len(collections)} collection(s):", ", ".join(c.name for c in collections))
    for i, coll in enumerate(collections):
        known = st["collections"].get(str(coll.id), {})
        if mode == "init":
            medias = cl.collection_medias(coll.id, amount=max_per_collection)  # 0 = everything
        else:
            # newest first; stops at the newest post already seen
            medias = cl.collection_medias(coll.id, amount=min(max_per_collection or SYNC_CAP, SYNC_CAP),
                                          last_media_pk=int(known.get("newest_pk", 0)))
        new = [m for m in medias if str(m.pk) not in seen]
        print(f"  {coll.name}: {len(medias)} fetched, {len(new)} new")
        for m in new:
            items.append(record(m, coll))
            seen.add(str(m.pk))
        if medias:
            newest = max(int(m.pk) for m in medias)
            st["collections"][str(coll.id)] = {"name": coll.name, "newest_pk": max(newest, int(known.get("newest_pk", 0)))}
        if mode == "init" and i < len(collections) - 1:
            time.sleep(random.uniform(60, 120))  # long pause between collections on the big first pass
    st["seen"] = sorted(seen)
    save_state(st)
    return items


def fake_items(n: int) -> list[dict]:
    """Test the server side without Instagram."""
    px = base64.b64encode(bytes.fromhex(
        "ffd8ffe000104a46494600010100000100010000ffdb004300" + "08" * 64 + "ffd9")).decode()
    return [{"media_id": f"fake{int(time.time())}{i}", "collection_name": random.choice(["Summer", "Reggae vibes"]),
             "media_type": 1, "taken_at": time.time() - i * 3600, "author": "test", "caption": "test post",
             "collected_at": time.time(), "image_b64": px} for i in range(n)]


def run_once(a, mode: str) -> int:
    grid = Grid(a.server, a.token)
    st = load_state()
    if st.get("needs_login"):
        grid.alert("Collector stopped: Instagram asked for a check. Open the app, confirm, then run `collector.py login`.")
        return 2
    if st.get("paused_until", 0) > time.time():
        print("Paused until", datetime.fromtimestamp(st["paused_until"]))
        return 0
    try:
        items = fake_items(a.fake) if a.fake else collect(mode, a.collections, a.max)
    except Stop as e:
        st.update(needs_login=True)
        save_state(st)
        grid.alert(f"Collector stopped: {e}")
        return 2
    except Exception as e:  # noqa: BLE001 - instagrapi raises many exception types
        kind = classify(e)
        st = load_state()
        if kind == "login":
            st["needs_login"] = True
            grid.alert(f"Collector stopped ({type(e).__name__}): confirm in the Instagram app, then run `collector.py login`.")
        elif kind == "slow":
            st["strikes"] = st.get("strikes", 0) + 1
            if st["strikes"] >= 2:
                st["needs_login"] = True
                grid.alert(f"Collector fully stopped: second rate limit ({type(e).__name__}). Restart it manually later.")
            else:
                st["paused_until"] = time.time() + 24 * 3600
                grid.alert(f"Collector paused 24 h: Instagram said slow down ({type(e).__name__}).")
        save_state(st)
        if not kind:
            raise
        return 2
    st = load_state()
    st["strikes"] = 0
    save_state(st)
    if not items:
        print("No new posts.")
        return 0
    res = Grid(a.server, a.token).send(items, process=a.process)
    print(f"Sent {res['added']} new post(s) to the grid ({res['duplicates']} already known).",
          "Processing run started." if res.get("run") else "")
    return 0


def daily(a):
    start_h, end_h = (int(x.split(":")[0]) for x in a.window.split("-"))
    while True:
        now = datetime.now()
        day = now if now.hour < start_h else now + timedelta(days=1)
        at = day.replace(hour=start_h, minute=0, second=0, microsecond=0) + timedelta(
            seconds=random.uniform(0, (end_h - start_h) * 3600))
        print("Next sync at", at.strftime("%Y-%m-%d %H:%M"))
        time.sleep(max(0, (at - datetime.now()).total_seconds()))
        try:
            run_once(a, "sync")
        except Exception as e:  # keep the loop alive, try again tomorrow
            print("sync failed:", e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["login", "init", "sync", "daily"])
    ap.add_argument("--server", default=os.environ.get("GRID_SERVER", "http://localhost:8000"))
    ap.add_argument("--token", default=os.environ.get("GRID_TOKEN"))
    ap.add_argument("--collections", type=lambda s: [x.strip() for x in s.split(",") if x.strip()], default=[],
                    help="only these collections (comma-separated names); default: all")
    ap.add_argument("--max", type=int, default=0, help="max posts per collection (0 = all on init)")
    ap.add_argument("--window", default="02-06", help="daily sync window, hours, e.g. 02-06")
    ap.add_argument("--no-process", dest="process", action="store_false",
                    help="only store new posts, don't start a processing run on the grid")
    ap.add_argument("--fake", type=int, default=0, help=argparse.SUPPRESS)
    a = ap.parse_args()
    if a.command == "login":
        login_interactive()
    elif a.command == "daily":
        daily(a)
    else:
        sys.exit(run_once(a, a.command))


if __name__ == "__main__":
    main()
