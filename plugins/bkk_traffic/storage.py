"""SQLite warehouse (3 ชั้น) + checkpoint + log การรัน.

BRONZE  bronze_incidents  raw JSON ตามที่ดึงมา (เฉพาะ กทม.)          <- ขั้น SCRAPE
SILVER  incidents         ข้อมูลที่ clean + ผ่านกฎรายแถวแล้ว          <- ขั้น CLEAN (clean.py)
        quarantine        แถวที่ไม่ผ่านกฎ พร้อมเหตุผล (ไม่ถูก export)
GOLD    data/parquet, data/processed (นอก DB)                       <- ขั้น PUBLISH (publish.py) หลังผ่าน GATE
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime

from bkk_traffic import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS bronze_incidents (
    eid INTEGER PRIMARY KEY,
    raw_json TEXT,
    fetched_via TEXT,
    first_fetched_at TEXT, last_fetched_at TEXT,
    cleaned_at TEXT                   -- NULL = ยังไม่ผ่านขั้น clean
);
CREATE INDEX IF NOT EXISTS ix_bronze_pending ON bronze_incidents(cleaned_at);
-- สภาพจราจรปกติ: ดัชนีรถติดทุก 5 นาที (ย้อนหลังจากไฟล์รายปีของ Longdo + ค่าล่าสุดจาก realtime)
CREATE TABLE IF NOT EXISTS traffic_index_history (
    ts INTEGER PRIMARY KEY, ts_local TEXT, index_value REAL, source TEXT
);
CREATE TABLE IF NOT EXISTS travel_times (
    route TEXT, fetched_at TEXT,
    distance_m REAL, duration_s REAL, duration_min REAL,
    from_lat REAL, from_lon REAL, to_lat REAL, to_lon REAL,
    raw_json TEXT,
    PRIMARY KEY (route, fetched_at)
);
CREATE TABLE IF NOT EXISTS quarantine (
    eid INTEGER PRIMARY KEY, reasons TEXT, raw_json TEXT, detected_at TEXT
);
CREATE TABLE IF NOT EXISTS incidents (
    eid INTEGER PRIMARY KEY,
    nid INTEGER,
    title TEXT, title_en TEXT,
    comment TEXT, comment_en TEXT,
    latitude REAL, longitude REAL,
    severity INTEGER,                 -- จาก severity_level ของ API (1 หรือ 3)
    event_type_id INTEGER,            -- ฟิลด์ "severity" ของ API = id ประเภท (geteventtype)
    start_time TEXT, stop_time TEXT, createtime TEXT, updatedtime TEXT,
    contributor TEXT, source TEXT, status TEXT,
    event_type TEXT,
    traffic_relevance TEXT,           -- direct / affects / keyword (เก็บเฉพาะที่เกี่ยวกับจราจร)
    is_accident INTEGER,
    is_bangkok INTEGER,
    province_guess TEXT,
    fetched_via TEXT,
    first_seen_at TEXT, last_seen_at TEXT,
    raw_json TEXT,                    -- เลิกใช้ (raw อยู่ใน bronze_incidents)
    dq_flags TEXT,                    -- กฎที่แก้ค่าให้ เช่น R4_stop_before_start
    is_duplicate INTEGER DEFAULT 0    -- รายงานซ้ำ (ไม่ถูก export)
);
CREATE INDEX IF NOT EXISTS ix_incidents_start ON incidents(start_time);
CREATE INDEX IF NOT EXISTS ix_incidents_bkk ON incidents(is_bangkok, is_accident);

-- เก็บแค่ eid ของทุกเหตุการณ์ที่เคยเห็น (ไม่มีเนื้อหา) ไว้ตรวจ eid ขาด และเช็คว่าหน้าไหนเคยดึงแล้ว
CREATE TABLE IF NOT EXISTS seen_eids (
    eid INTEGER PRIMARY KEY, is_bangkok INTEGER, seen_at TEXT
);
CREATE TABLE IF NOT EXISTS traffic_index (
    ts INTEGER PRIMARY KEY,          -- epoch วินาทีจาก API
    ts_local TEXT,
    index_value REAL,
    fetched_at TEXT
);
CREATE TABLE IF NOT EXISTS event_types (
    id INTEGER PRIMARY KEY, name TEXT, name_en TEXT, icon TEXT, severity INTEGER
);
CREATE TABLE IF NOT EXISTS checkpoints (
    job TEXT PRIMARY KEY, state_json TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job TEXT, started_at TEXT, finished_at TEXT,
    requests INTEGER, items INTEGER, new_items INTEGER,
    status TEXT, note TEXT
);
CREATE TABLE IF NOT EXISTS missing_eids (
    eid INTEGER PRIMARY KEY,
    detected_at TEXT, last_try_at TEXT,
    tries INTEGER DEFAULT 0,
    status TEXT                       -- pending / recovered / not_available
);
"""


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


_NEW_COLUMNS = {"incidents": {"dq_flags": "TEXT", "is_duplicate": "INTEGER DEFAULT 0", "event_type_id": "INTEGER",
                              "traffic_relevance": "TEXT"}}


def _add_missing_columns(conn):
    """DB ที่สร้างก่อนมีคอลัมน์ใหม่ -> ALTER TABLE เพิ่มให้."""
    for table, cols in _NEW_COLUMNS.items():
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        for col, typ in cols.items():
            if col not in have:
                try:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
                except sqlite3.OperationalError:  # อีก process เพิ่งเพิ่มไปพร้อมกัน
                    pass
    conn.commit()


@contextmanager
def connect():
    config.WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.WAREHOUSE_DB, timeout=120)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    _add_missing_columns(conn)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ---------------- checkpoints ----------------
def get_checkpoint(conn, job: str) -> dict:
    row = conn.execute("SELECT state_json FROM checkpoints WHERE job=?", (job,)).fetchone()
    return json.loads(row["state_json"]) if row else {}


def set_checkpoint(conn, job: str, state: dict):
    conn.execute(
        "INSERT INTO checkpoints(job, state_json, updated_at) VALUES(?,?,?) "
        "ON CONFLICT(job) DO UPDATE SET state_json=excluded.state_json, updated_at=excluded.updated_at",
        (job, json.dumps(state, ensure_ascii=False), now_str()),
    )
    conn.commit()
    # สำเนาเป็นไฟล์ ให้เปิดดูได้ง่ายโดยไม่ต้องเข้า DB
    config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    (config.CHECKPOINT_DIR / f"{job}.json").write_text(
        json.dumps({**state, "updated_at": now_str()}, ensure_ascii=False, indent=2), encoding="utf-8")


def log_run(conn, job, started_at, requests_, items, new_items, status, note=""):
    conn.execute(
        "INSERT INTO scrape_runs(job, started_at, finished_at, requests, items, new_items, status, note) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (job, started_at, now_str(), requests_, items, new_items, status, note),
    )


# ---------------- SCRAPE -> BRONZE (แล้วส่งต่อให้ CLEAN) ----------------
# ฟังก์ชันแปลงค่าอยู่ที่ clean.py (re-export ไว้ให้โค้ดเดิมเรียกได้)
from bkk_traffic.clean import (clean_text, guess_province, load_type_names, normalize,  # noqa: E402,F401
                               to_float, to_int)


def upsert_incidents(conn, items: list[dict], fetched_via: str) -> tuple[int, list[dict]]:
    """ขั้น scrape: กรองเฉพาะ กทม. + เกี่ยวกับการจราจร -> เก็บ raw ลง bronze_incidents -> สั่ง clean ทันที.

    เหตุการณ์นอกขอบเขต บันทึกแค่ eid ใน seen_eids. คืน (จำนวนเหตุการณ์ใหม่, รายการ raw ที่เก็บ)
    """
    from bkk_traffic import clean

    ts = now_str()
    types = clean.load_types(conn)
    new_count = 0
    bkk_items, bkk_eids = [], []
    for item in items:
        eid = to_int(item.get("eid"))
        if eid is None:
            continue
        is_bkk, relevance = clean.in_scope(item, types)
        conn.execute("INSERT INTO seen_eids(eid, is_bangkok, seen_at) VALUES(?,?,?) "
                     "ON CONFLICT(eid) DO UPDATE SET is_bangkok=excluded.is_bangkok", (eid, is_bkk, ts))
        conn.execute("UPDATE missing_eids SET status='recovered' WHERE eid=? AND status!='recovered'", (eid,))
        if not (is_bkk and relevance):
            # นอก กทม. / ไม่เกี่ยวกับจราจร (เผื่อถูกแก้ทีหลัง ลบของเดิมด้วย)
            for table in ("incidents", "bronze_incidents", "quarantine"):
                conn.execute(f"DELETE FROM {table} WHERE eid=?", (eid,))
            continue
        bkk_items.append(item)
        bkk_eids.append(eid)
        if not conn.execute("SELECT 1 FROM bronze_incidents WHERE eid=?", (eid,)).fetchone():
            new_count += 1
        conn.execute(
            "INSERT INTO bronze_incidents(eid, raw_json, fetched_via, first_fetched_at, last_fetched_at, cleaned_at) "
            "VALUES(?,?,?,?,?,NULL) ON CONFLICT(eid) DO UPDATE SET raw_json=excluded.raw_json, "
            "last_fetched_at=excluded.last_fetched_at, fetched_via=excluded.fetched_via, cleaned_at=NULL",
            (eid, json.dumps(item, ensure_ascii=False), fetched_via, ts, ts),
        )
    conn.commit()
    if bkk_eids:
        clean.clean_eids(conn, bkk_eids)
    return new_count, bkk_items


def known_eids(conn, eids) -> set:
    eids = [e for e in eids if e is not None]
    if not eids:
        return set()
    q = f"SELECT eid FROM seen_eids WHERE eid IN ({','.join('?' * len(eids))})"
    return {r["eid"] for r in conn.execute(q, eids)}
