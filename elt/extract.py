"""Extract grid metrics from the server and load them into the warehouse.

Incremental: for each table it looks up the highest `_line` already loaded,
then asks the server only for newer rows.

    # BigQuery (uses Application Default Credentials or GOOGLE_APPLICATION_CREDENTIALS)
    python elt/extract.py --server https://grid.example.com --token XXX \
        --target bigquery --project my-gcp-project --dataset grid_raw

    # Local DuckDB file (no GCP needed, handy for testing dbt)
    python elt/extract.py --server http://localhost:8000 --token test --target duckdb --path grid.duckdb

Airflow calls `run()` directly (see airflow/dags/grid_elt.py).
"""

import argparse
import os

import requests

TABLES = ("runs", "clients", "events", "jobs", "samples", "sources")

# Explicit schemas keep BigQuery types stable across loads.
# Times are Unix epoch seconds (FLOAT64); dbt converts them to timestamps.
SCHEMAS = {
    "runs": [("_line", "INT64"), ("run_id", "STRING"), ("preset", "STRING"), ("started", "FLOAT64"),
             ("finished", "FLOAT64"), ("drained", "FLOAT64"), ("total_units", "FLOAT64"), ("jobs", "INT64")],
    "clients": [("_line", "INT64"), ("client_id", "STRING"), ("name", "STRING"), ("kind", "STRING"),
                ("joined", "FLOAT64"), ("cpu_score", "FLOAT64"), ("has_gpu", "BOOL"), ("gpu_name", "STRING"),
                ("vram_gb", "FLOAT64"), ("gpu_score", "FLOAT64"), ("native", "BOOL"), ("is_browser", "BOOL"),
                ("is_simulated", "BOOL"), ("cores", "INT64"), ("ram_gb", "FLOAT64"), ("os", "STRING")],
    "events": [("_line", "INT64"), ("run_id", "STRING"), ("t", "FLOAT64"), ("type", "STRING"),
               ("client_id", "STRING"), ("online", "INT64"), ("text", "STRING")],
    "jobs": [("_line", "INT64"), ("run_id", "STRING"), ("job_id", "STRING"), ("type", "STRING"),
             ("units", "FLOAT64"), ("gpu_job", "BOOL"), ("client_id", "STRING"), ("started", "FLOAT64"),
             ("finished", "FLOAT64"), ("seconds", "FLOAT64")],
    "sources": [("_line", "INT64"), ("media_id", "STRING"), ("code", "STRING"), ("collection_id", "STRING"),
                ("collection_name", "STRING"), ("media_type", "INT64"), ("taken_at", "FLOAT64"),
                ("author", "STRING"), ("caption", "STRING"), ("like_count", "INT64"),
                ("collected_at", "FLOAT64"), ("received_at", "FLOAT64"), ("has_image", "BOOL")],
    "samples": [("_line", "INT64"), ("run_id", "STRING"), ("t", "FLOAT64"), ("rate", "FLOAT64"),
                ("clients", "INT64"), ("busy", "INT64")],
}


def flatten(table: str, row: dict) -> dict:
    """Shape a raw row to its schema (device capabilities become columns)."""
    if table == "clients":
        caps = row.pop("caps", {}) or {}
        row.update(cpu_score=caps.get("score"), has_gpu=bool(caps.get("gpu")), gpu_name=caps.get("gpu_name"),
                   vram_gb=caps.get("vram_gb"), gpu_score=caps.get("gpu_score"), native=bool(caps.get("native")),
                   is_browser=bool(caps.get("browser")), is_simulated=bool(caps.get("simulated")),
                   cores=caps.get("cores"), ram_gb=caps.get("ram_gb"), os=caps.get("os"))
    cols = [c for c, _ in SCHEMAS[table]]
    return {c: row.get(c) for c in cols}


def fetch(server: str, token: str, table: str, offset: int):
    while True:
        r = requests.get(f"{server.rstrip('/')}/api/export/{table}", params={"offset": offset},
                         headers={"X-Grid-Token": token}, timeout=60)
        r.raise_for_status()
        page = r.json()
        rows = [flatten(table, x) for x in page["rows"]]
        if rows:
            yield rows
        offset = page["next_offset"]
        if not page["more"]:
            return


# ---------- warehouse targets ----------

class BigQueryTarget:
    def __init__(self, project: str, dataset: str, location: str = "EU"):
        from google.cloud import bigquery
        self.bq = bigquery
        self.client = bigquery.Client(project=project)
        self.dataset = f"{project}.{dataset}"
        ds = bigquery.Dataset(self.dataset)
        ds.location = location
        self.client.create_dataset(ds, exists_ok=True)

    def max_line(self, table: str) -> int:
        from google.api_core.exceptions import NotFound
        try:
            res = self.client.query(f"SELECT MAX(_line) AS m FROM `{self.dataset}.{table}`").result()
        except NotFound:
            return -1
        m = next(iter(res)).m
        return -1 if m is None else m

    def load(self, table: str, rows: list[dict]):
        schema = [self.bq.SchemaField(c, t) for c, t in SCHEMAS[table]]
        cfg = self.bq.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
        self.client.load_table_from_json(rows, f"{self.dataset}.{table}", job_config=cfg).result()


class DuckDBTarget:
    TYPES = {"INT64": "BIGINT", "FLOAT64": "DOUBLE", "STRING": "VARCHAR", "BOOL": "BOOLEAN"}

    def __init__(self, path: str, schema: str = "grid_raw"):
        import duckdb
        self.con = duckdb.connect(path)
        self.schema = schema
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for t, cols in SCHEMAS.items():
            ddl = ", ".join(f'"{c}" {self.TYPES[ty]}' for c, ty in cols)
            self.con.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{t} ({ddl})")

    def max_line(self, table: str) -> int:
        m = self.con.execute(f"SELECT MAX(_line) FROM {self.schema}.{table}").fetchone()[0]
        return -1 if m is None else m

    def load(self, table: str, rows: list[dict]):
        cols = [c for c, _ in SCHEMAS[table]]
        self.con.executemany(
            f"INSERT INTO {self.schema}.{table} VALUES ({', '.join('?' for _ in cols)})",
            [[r[c] for c in cols] for r in rows])


def run(server: str, token: str, target, tables=TABLES) -> dict:
    loaded = {}
    for t in tables:
        start = target.max_line(t) + 1
        n = 0
        for rows in fetch(server, token, t, start):
            target.load(t, rows)
            n += len(rows)
        loaded[t] = n
        print(f"{t:<8} +{n} rows (from line {start})")
    return loaded


def make_target(kind: str, **kw):
    if kind == "bigquery":
        return BigQueryTarget(kw["project"], kw.get("dataset", "grid_raw"), kw.get("location", "EU"))
    return DuckDBTarget(kw.get("path", "grid.duckdb"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=os.environ.get("GRID_SERVER", "http://localhost:8000"))
    ap.add_argument("--token", default=os.environ.get("GRID_TOKEN"))
    ap.add_argument("--target", choices=["bigquery", "duckdb"], default="bigquery")
    ap.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
    ap.add_argument("--dataset", default=os.environ.get("BQ_RAW_DATASET", "grid_raw"))
    ap.add_argument("--location", default=os.environ.get("BQ_LOCATION", "EU"))
    ap.add_argument("--path", default="grid.duckdb")
    a = ap.parse_args()
    run(a.server, a.token, make_target(a.target, project=a.project, dataset=a.dataset,
                                       location=a.location, path=a.path))


if __name__ == "__main__":
    main()
