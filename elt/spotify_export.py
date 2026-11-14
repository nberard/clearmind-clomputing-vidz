"""Load a Spotify "Account data" export into the warehouse.

Reads ONLY the listening files; the personal ones (Payments, Identity,
UserAddress, Identifiers, Inferences, SearchQueries...) are never opened.

  StreamingHistory_music_*.json  -> spotify_streams          (one row per play)
  YourLibrary.json               -> spotify_library_tracks   (liked songs)
  Playlist*.json                 -> spotify_playlist_tracks  (one row per track in a playlist)

An export is a full snapshot, so each load replaces the tables.

    python elt/spotify_export.py "imports/Spotify Account Data" --target bigquery --project my-gcp-project
    python elt/spotify_export.py "imports/Spotify Account Data" --target duckdb --path grid.duckdb
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ALLOWED = re.compile(r"^(StreamingHistory_music_\d+|YourLibrary|Playlist\d+)\.json$")

SCHEMAS = {
    "spotify_streams": [("played_at", "FLOAT64"), ("artist_name", "STRING"), ("track_name", "STRING"),
                        ("ms_played", "INT64"), ("source_file", "STRING")],
    "spotify_library_tracks": [("artist_name", "STRING"), ("album_name", "STRING"), ("track_name", "STRING"),
                               ("track_uri", "STRING")],
    "spotify_playlist_tracks": [("playlist_name", "STRING"), ("playlist_modified", "STRING"),
                                ("position", "INT64"), ("artist_name", "STRING"), ("album_name", "STRING"),
                                ("track_name", "STRING"), ("track_uri", "STRING"), ("added_date", "STRING")],
}


def read(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def epoch(end_time: str) -> float | None:
    """'2025-11-10 21:34' (UTC in the export) -> epoch seconds."""
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(end_time, fmt).replace(tzinfo=timezone.utc).timestamp()
        except (TypeError, ValueError):
            continue
    return None


def parse(folder: Path) -> dict[str, list[dict]]:
    files = sorted(p for p in folder.iterdir() if ALLOWED.match(p.name))
    if not files:
        sys.exit(f"No StreamingHistory_music / YourLibrary / Playlist files in {folder}")
    out = {t: [] for t in SCHEMAS}
    for p in files:
        data = read(p)
        if p.name.startswith("StreamingHistory_music_"):
            for r in data:
                out["spotify_streams"].append({
                    "played_at": epoch(r.get("endTime")), "artist_name": r.get("artistName"),
                    "track_name": r.get("trackName"), "ms_played": r.get("msPlayed"), "source_file": p.name})
        elif p.name == "YourLibrary.json":
            for r in data.get("tracks", []):
                out["spotify_library_tracks"].append({
                    "artist_name": r.get("artist"), "album_name": r.get("album"),
                    "track_name": r.get("track"), "track_uri": r.get("uri")})
        else:  # Playlist<N>.json
            for pl in data.get("playlists", []):
                for i, item in enumerate(pl.get("items", []), start=1):
                    t = item.get("track") or {}
                    if not t:  # podcast episode, local file or audiobook
                        continue
                    out["spotify_playlist_tracks"].append({
                        "playlist_name": pl.get("name"), "playlist_modified": pl.get("lastModifiedDate"),
                        "position": i, "artist_name": t.get("artistName"), "album_name": t.get("albumName"),
                        "track_name": t.get("trackName"), "track_uri": t.get("trackUri"),
                        "added_date": item.get("addedDate")})
        print(f"read {p.name}")
    return out


def load_bigquery(tables: dict, project: str, dataset: str, location: str):
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    ds = bigquery.Dataset(f"{project}.{dataset}")
    ds.location = location
    client.create_dataset(ds, exists_ok=True)
    for name, rows in tables.items():
        cfg = bigquery.LoadJobConfig(schema=[bigquery.SchemaField(c, t) for c, t in SCHEMAS[name]],
                                     write_disposition="WRITE_TRUNCATE")
        client.load_table_from_json(rows, f"{project}.{dataset}.{name}", job_config=cfg).result()
        print(f"{name:<24} {len(rows)} rows")


def load_duckdb(tables: dict, path: str, schema: str):
    import duckdb
    types = {"INT64": "BIGINT", "FLOAT64": "DOUBLE", "STRING": "VARCHAR"}
    con = duckdb.connect(path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for name, rows in tables.items():
        cols = SCHEMAS[name]
        con.execute(f"CREATE OR REPLACE TABLE {schema}.{name} ("
                    + ", ".join(f'"{c}" {types[t]}' for c, t in cols) + ")")
        if rows:
            con.executemany(f"INSERT INTO {schema}.{name} VALUES ({', '.join('?' for _ in cols)})",
                            [[r[c] for c, _ in cols] for r in rows])
        print(f"{name:<24} {len(rows)} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder", type=Path, help='the unzipped "Spotify Account Data" folder')
    ap.add_argument("--target", choices=["bigquery", "duckdb"], default="bigquery")
    ap.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
    ap.add_argument("--dataset", default=os.environ.get("BQ_RAW_DATASET", "grid_raw"))
    ap.add_argument("--location", default=os.environ.get("BQ_LOCATION", "EU"))
    ap.add_argument("--path", default="grid.duckdb")
    a = ap.parse_args()
    tables = parse(a.folder)
    if a.target == "bigquery":
        load_bigquery(tables, a.project, a.dataset, a.location)
    else:
        load_duckdb(tables, a.path, a.dataset)


if __name__ == "__main__":
    main()
