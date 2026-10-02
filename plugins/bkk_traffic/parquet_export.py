"""ขั้น PUBLISH: export silver (ไม่รวมรายงานซ้ำ) เป็น Parquet แยกไฟล์ละ ROWS_PER_FILE (10,000) แถว.

data/parquet/<dataset>/part-00000.parquet, part-00001.parquet, ... + _manifest.json
เขียนลงโฟลเดอร์ชั่วคราวก่อนแล้วค่อยสลับ เพื่อไม่ให้คนที่เปิดอ่านอยู่เจอไฟล์ครึ่งๆ กลางๆ
"""
import json
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from bkk_traffic import config, storage

ROWS_PER_FILE = 10_000
PARQUET_DIR = config.DATA_DIR / "parquet"

INCIDENT_SCHEMA = pa.schema([
    ("eid", pa.int64()), ("nid", pa.int64()),
    ("start_time", pa.string()), ("stop_time", pa.string()),
    ("createtime", pa.string()), ("updatedtime", pa.string()),
    ("event_type_id", pa.int16()), ("event_type", pa.string()), ("traffic_relevance", pa.string()),
    ("is_accident", pa.int8()), ("is_bangkok", pa.int8()),
    ("severity", pa.int16()),
    ("title", pa.string()), ("title_en", pa.string()),
    ("comment", pa.string()), ("comment_en", pa.string()),
    ("latitude", pa.float64()), ("longitude", pa.float64()),
    ("province_guess", pa.string()), ("contributor", pa.string()), ("source", pa.string()),
    ("status", pa.string()), ("fetched_via", pa.string()),
    ("first_seen_at", pa.string()), ("last_seen_at", pa.string()),
    ("dq_flags", pa.string()),                    # กฎที่แก้ค่าให้ระหว่าง clean (ดู clean.py)
])
_INC_COLS = ",".join(INCIDENT_SCHEMA.names)

DATASETS = {
    "incidents_bangkok": (f"SELECT {_INC_COLS} FROM incidents WHERE is_bangkok=1 AND is_duplicate=0 ORDER BY eid", INCIDENT_SCHEMA),
    "accidents_bangkok": (f"SELECT {_INC_COLS} FROM incidents WHERE is_bangkok=1 AND is_accident=1 AND is_duplicate=0 ORDER BY eid",
                          INCIDENT_SCHEMA),
    # สภาพจราจรปกติ: ดัชนีรถติดทุก 5 นาที ตั้งแต่ 2012 (ไฟล์ย้อนหลังของ Longdo + realtime)
    "traffic_index": ("SELECT ts, ts_local, index_value, source FROM traffic_index_history ORDER BY ts",
                      pa.schema([("ts", pa.int64()), ("ts_local", pa.string()),
                                 ("index_value", pa.float64()), ("source", pa.string())])),
    "travel_times": ("SELECT route, fetched_at, distance_m, duration_s, duration_min, from_lat, from_lon, to_lat, "
                     "to_lon FROM travel_times ORDER BY fetched_at, route",
                     pa.schema([("route", pa.string()), ("fetched_at", pa.string()), ("distance_m", pa.float64()),
                                ("duration_s", pa.float64()), ("duration_min", pa.float64()),
                                ("from_lat", pa.float64()), ("from_lon", pa.float64()),
                                ("to_lat", pa.float64()), ("to_lon", pa.float64())])),
}


def _signature(conn, sql) -> str:
    """ลายเซ็นของข้อมูล (จำนวนแถว + เวลาอัปเดตล่าสุด) ใช้ข้ามการเขียนใหม่ถ้าไม่มีอะไรเปลี่ยน."""
    if "FROM incidents" in sql:
        time_col = "last_seen_at"
    elif "traffic_index_history" in sql:
        time_col = "MAX(ts) || '-' || SUM(index_value)"
    else:
        time_col = "fetched_at"
    inner = sql.rsplit(" ORDER BY", 1)[0]
    agg = time_col if "(" in time_col else f"MAX({time_col})"
    row = conn.execute(f"SELECT COUNT(*), {agg} FROM ({inner})").fetchone()
    return f"{row[0]}|{row[1]}|{ROWS_PER_FILE}"


def _export_dataset(conn, name, sql, schema, validation=None) -> dict:
    final_dir = PARQUET_DIR / name
    tmp_dir = PARQUET_DIR / f".{name}.tmp"
    signature = _signature(conn, sql)
    try:
        old = json.loads((final_dir / "_manifest.json").read_text(encoding="utf-8"))
        if old.get("signature") == signature:
            return {"rows": old["total_rows"], "files": len(old["files"]), "skipped": True}
    except (OSError, ValueError, KeyError):
        pass
    shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True)

    cur = conn.execute(sql)
    files, total, part = [], 0, 0
    while True:
        rows = cur.fetchmany(ROWS_PER_FILE)
        if not rows:
            break
        columns = list(zip(*rows))
        table = pa.Table.from_arrays(
            [pa.array(col, type=field.type) for col, field in zip(columns, schema)], schema=schema)
        fname = f"part-{part:05d}.parquet"
        pq.write_table(table, tmp_dir / fname, compression="zstd")
        key = schema.names[0]
        files.append({"file": fname, "rows": len(rows), f"{key}_min": rows[0][0], f"{key}_max": rows[-1][0]})
        total += len(rows)
        part += 1

    manifest = {"dataset": name, "generated_at": storage.now_str(), "rows_per_file": ROWS_PER_FILE,
                "total_rows": total, "signature": signature, "validation": validation, "files": files}
    (tmp_dir / "_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    shutil.rmtree(final_dir, ignore_errors=True)
    tmp_dir.rename(final_dir)
    return {"rows": total, "files": len(files)}


def export_all(validation: dict | None = None) -> dict:
    """เรียกผ่าน publish.publish_parquet() เท่านั้น (ต้องผ่าน validation gate ก่อน)."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    with storage.connect() as conn:
        return {name: _export_dataset(conn, name, sql, schema, validation)
                for name, (sql, schema) in DATASETS.items()}
