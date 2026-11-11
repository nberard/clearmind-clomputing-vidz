# vibegen-grid 🌴

Your own mini distributed-computing grid. One server (OVH) hands out the work
of the vibegen AI video pipeline; phones, laptops, an old PC and a gaming PC
join as clients and share it. A live 9:16 dashboard shows how throughput jumps
every time a device joins, made to be screen-recorded for Instagram stories.

```
              ┌──────────── OVH server ────────────┐
              │ job queue · device matching ·      │
              │ metrics · live dashboard           │
              └──────▲───────▲────────▲───────▲────┘
                     │       │        │       │      pull job → run → report
                  📱 phones  💻 laptop  🧓 old PC  🎮 gaming PC
               (browser link) (agent)  (agent)    (agent + GPU)
```

## How it works

- The pipeline is split into **jobs**: ingest, music analysis, image captions,
  character images, music generation, video clips, encoding.
- Each device **benchmarks itself** when it joins. A job only goes to a device
  that can run it: GPU jobs (⚡) need a GPU with enough VRAM, and encoding
  needs a native client.
- The dashboard splits a run into **stages**. A new stage starts whenever a
  device joins or leaves, and each stage's throughput (work units per minute)
  is compared to the first device alone: ×1.0, ×2.5, ×9.3…
- Finished runs are saved to `data/history.json` for later graphs.

> **Workloads are synthetic for now.** Each job runs a compute loop sized like
> the real step (1 unit ≈ 1 s on a reference device), so you can measure
> scaling before installing any AI model. To plug in the real pipeline, replace
> `execute()` in `client/agent.py` (librosa, CLIP, Flux, Wan, ffmpeg…). The
> server, scheduling and dashboard stay the same.

## 1. Rehearse on your PC (5 minutes)

```bash
pip install -r server/requirements.txt -r client/requirements.txt
cd server && GRID_TOKEN=test uvicorn app:app --port 8000
```

Open <http://localhost:8000/?token=test>, then in a second terminal:

```bash
python scripts/simulate.py --token test --step 30
```

It starts a run with no device, then adds 1 phone → 2 more phones → old PC →
laptop → gaming PC, one every 30 s. These are simulated devices. Change the
order with `--plan "phone,phone+phone+phone,oldpc,gaming"`.

## 2. Deploy on OVH

On a small VPS with Docker, and a domain pointing to it:

```bash
git clone <this repo> && cd clearmind-clomputing-vidz
cp .env.example .env   # set GRID_TOKEN (and GRID_DOMAIN)
docker compose up -d   # starts the grid on 127.0.0.1:8000
```

Then put HTTPS in front of it (phones need HTTPS to keep the screen awake):
- **Server already runs nginx** (ports 80/443 taken): use `docs/nginx.conf` and certbot.
- **Nothing on ports 80/443**: `docker compose --profile caddy up -d` gets a certificate automatically.

Check what uses port 80 with `sudo ss -ltnp | grep -E ':(80|443) '`.

Dashboard: `https://YOUR_DOMAIN/?token=YOUR_TOKEN`

## 3. Add real devices

| Device | How |
|---|---|
| 📱 Any phone (iPhone or Android) | Scan the QR code on the dashboard, tap **Join**, keep the screen on |
| 💻 🧓 🖥️ PC / laptop / old PC | `pip install -r client/requirements.txt` then `python client/agent.py --server https://YOUR_DOMAIN --token YOUR_TOKEN` |
| 🎮 Gaming PC (GPU jobs) | Same, plus `pip install torch --index-url https://download.pytorch.org/whl/cu121`. The agent detects the GPU and takes the ⚡ jobs |
| 📱 Android, native | Install Termux, `pkg install python python-numpy`, `pip install requests psutil`, then run the agent |

Options: `--name "Nico's PC"`, `--kind oldpc` (forces the icon).

## Filming the story

1. **Start a run** on the dashboard (`demo` preset) with no device. The GPU
   steps show *waiting for GPU*.
2. Add the phones one by one. Each one pops in and the yellow line steps up.
3. Add the old PC and the laptop.
4. Connect the gaming PC on camera. The ⚡ GPU jobs start and the line jumps.
5. Tap **Story mode** to hide the controls, and **Story PNG** to export a
   1080×1920 summary image with the speedup bars and the chart.

Presets: `demo` (about 15 min with a gaming PC), `cpu_only` (no GPU needed),
`full` (long, close to the real pipeline's size).

## Data stack: Python → BigQuery → dbt, orchestrated by Airflow

```
grid server ──/api/export──▶ elt/extract.py ──▶ BigQuery grid_raw ──dbt──▶ grid_analytics
 (jsonl log)                 (incremental)        (append-only)            (marts)
              Airflow: grid_elt every 15 min · grid_benchmark on demand
```

**What gets stored:** the server appends every run, device, join/leave event,
finished job and 5-second throughput sample to `data/*.jsonl`.
`elt/extract.py` loads only the new lines into BigQuery, so it's safe to re-run.

**dbt models** (`dbt/`):

| Model | What it answers |
|---|---|
| `fct_stage_throughput` | For every stage of every run: devices online, device mix, units/min, **speedup**, **scaling efficiency** (actual vs sum of each device's speed) |
| `mart_scaling_curve` | **The headline graph:** throughput and speedup by number of devices, GPU count, real vs simulated |
| `mart_device_efficiency` | Speed per device kind × job type (phone vs old PC vs gaming PC) |
| `dim_devices`, `fct_jobs` | Every device with its capabilities, every job with its device |

**Airflow DAGs** (`airflow/dags/`):
- `grid_elt`: every 15 min, extract → BigQuery, then `dbt build` (skipped when there's nothing new).
- `grid_benchmark`: trigger from the UI with a preset and a device plan. It
  runs the benchmark (simulated devices, or your real ones), waits for the end,
  then triggers `grid_elt`. Run it with different plans to fill the scaling curve.

**Setup on the OVH server:**
1. In GCP, create a service account with **BigQuery Data Editor** and
   **BigQuery Job User**, and save its key as `secrets/gcp-key.json`.
2. Set `GCP_PROJECT` in `.env`.
3. Start it: `docker compose --profile data up -d` (add `--profile caddy` only if ports 80/443 are free). Airflow needs about 2–4 GB of RAM.
4. Open the UI through an SSH tunnel: `ssh -L 8080:localhost:8080 your-server`,
   then <http://localhost:8080>. The admin password is in `docker compose logs airflow`.

**Try it without GCP (DuckDB):**
```bash
pip install -r elt/requirements.txt dbt-duckdb
python elt/extract.py --token test --target duckdb --path grid.duckdb
cd dbt && DBT_TARGET=local DUCKDB_PATH=../grid.duckdb dbt build --profiles-dir .
```

Connect Looker Studio (free) to `grid_analytics.mart_scaling_curve` for
graphs you can share.

## API (for your own graphs)

- `GET /api/stats`: live state (devices, stages, samples, pipeline progress)
- `GET /api/history`: all finished runs with per-stage throughput and per-device stats
- `GET /api/export/{runs|clients|events|jobs|samples}?offset=N`: raw rows for the warehouse (token required)
- `POST /api/runs {"preset": "demo"}`, `POST /api/reset`: need the `X-Grid-Token` header
