import sqlite3
import json
from datetime import datetime, date


import os
DB_PATH = os.environ.get("DB_PATH", "/tmp/garmin_data.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            date TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            synced_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS activities (
            activity_id INTEGER PRIMARY KEY,
            date TEXT NOT NULL,
            data TEXT NOT NULL,
            synced_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sleep (
            date TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            synced_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hrv (
            date TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            synced_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS body_battery (
            date TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            synced_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS training_readiness (
            date TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            synced_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            synced_at TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT
        );
    """)
    conn.close()


def upsert_daily_summary(dt: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO daily_summary (date, data, synced_at) VALUES (?, ?, ?)",
        (dt, json.dumps(data), datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def upsert_activity(activity_id: int, dt: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO activities (activity_id, date, data, synced_at) VALUES (?, ?, ?, ?)",
        (activity_id, dt, json.dumps(data), datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def upsert_sleep(dt: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO sleep (date, data, synced_at) VALUES (?, ?, ?)",
        (dt, json.dumps(data), datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def upsert_hrv(dt: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO hrv (date, data, synced_at) VALUES (?, ?, ?)",
        (dt, json.dumps(data), datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def upsert_body_battery(dt: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO body_battery (date, data, synced_at) VALUES (?, ?, ?)",
        (dt, json.dumps(data), datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def upsert_training_readiness(dt: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO training_readiness (date, data, synced_at) VALUES (?, ?, ?)",
        (dt, json.dumps(data), datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def log_sync(status: str, message: str = None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO sync_log (synced_at, status, message) VALUES (?, ?, ?)",
        (datetime.utcnow().isoformat(), status, message),
    )
    conn.commit()
    conn.close()


def query_daily_summary(dt: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT data FROM daily_summary WHERE date = ?", (dt,)).fetchone()
    conn.close()
    return json.loads(row["data"]) if row else None


def query_activities(days: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT data FROM activities ORDER BY date DESC LIMIT ?", (days * 10,)
    ).fetchall()
    conn.close()
    return [json.loads(r["data"]) for r in rows]


def query_activities_by_date_range(start: str, end: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT data FROM activities WHERE date BETWEEN ? AND ? ORDER BY date DESC",
        (start, end),
    ).fetchall()
    conn.close()
    return [json.loads(r["data"]) for r in rows]


def query_sleep(dt: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT data FROM sleep WHERE date = ?", (dt,)).fetchone()
    conn.close()
    return json.loads(row["data"]) if row else None


def query_hrv(dt: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT data FROM hrv WHERE date = ?", (dt,)).fetchone()
    conn.close()
    return json.loads(row["data"]) if row else None


def query_body_battery_range(start: str, end: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT date, data FROM body_battery WHERE date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    conn.close()
    return [{"date": r["date"], **json.loads(r["data"])} for r in rows]


def query_training_readiness(dt: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT data FROM training_readiness WHERE date = ?", (dt,)).fetchone()
    conn.close()
    return json.loads(row["data"]) if row else None


def query_daily_summaries_range(start: str, end: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT date, data FROM daily_summary WHERE date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    conn.close()
    return [{"date": r["date"], **json.loads(r["data"])} for r in rows]


def query_sleep_range(start: str, end: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT date, data FROM sleep WHERE date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    conn.close()
    return [{"date": r["date"], **json.loads(r["data"])} for r in rows]


def query_hrv_range(start: str, end: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT date, data FROM hrv WHERE date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    conn.close()
    return [{"date": r["date"], **json.loads(r["data"])} for r in rows]
