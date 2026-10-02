"""Export ข้อมูลที่ผ่านการ clean เป็น CSV แยกไฟล์รายปี (utf-8-sig เปิดใน Excel แล้วภาษาไทยไม่เพี้ยน).

data/processed/<ชุดข้อมูล>/<ปี>.csv
แยกรายปีเพราะ: (1) ไฟล์รวมใหญ่เกินที่ Excel เปิดได้ (1,048,576 แถว) และ
(2) ไฟล์เดียวเกิน 100 MB ซึ่ง GitHub ไม่รับ — รายปีแต่ละไฟล์ไม่เกิน ~15 MB
"""
import csv
import shutil

from bkk_traffic import config, storage

INCIDENT_COLS = ("eid", "start_time", "stop_time", "event_type_id", "event_type", "traffic_relevance", "is_accident",
                 "severity", "title", "title_en", "comment", "comment_en", "latitude", "longitude", "province_guess",
                 "contributor", "createtime", "updatedtime", "dq_flags")

# ชื่อชุดข้อมูล -> (SQL, คอลัมน์ที่ใช้แยกปี) - SQL ต้องเรียงตามคอลัมน์นั้น
EXPORTS = {
    "incidents_bangkok": (f"SELECT {','.join(INCIDENT_COLS)} FROM incidents "
                          "WHERE is_bangkok=1 AND is_duplicate=0 ORDER BY start_time", "start_time"),
    "accidents_bangkok": (f"SELECT {','.join(INCIDENT_COLS)} FROM incidents "
                          "WHERE is_bangkok=1 AND is_accident=1 AND is_duplicate=0 ORDER BY start_time", "start_time"),
    "traffic_index": ("SELECT ts, ts_local, index_value, source FROM traffic_index_history ORDER BY ts", "ts_local"),
    "travel_times": ("SELECT route, fetched_at, distance_m, duration_s, duration_min, from_lat, from_lon, "
                     "to_lat, to_lon FROM travel_times ORDER BY fetched_at, route", "fetched_at"),
}


def _export(conn, name, sql, year_col) -> dict:
    final_dir = config.PROCESSED_DIR / name
    tmp_dir = config.PROCESSED_DIR / f".{name}.tmp"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True)
    cur = conn.execute(sql)
    header = [d[0] for d in cur.description]
    year_idx = header.index(year_col)
    counts, fh, writer, year = {}, None, None, None
    for row in cur:
        y = str(row[year_idx] or "unknown")[:4]
        if y != year:
            if fh:
                fh.close()
            year = y
            fh = open(tmp_dir / f"{year}.csv", "w", newline="", encoding="utf-8-sig")
            writer = csv.writer(fh)
            writer.writerow(header)
        writer.writerow(row)
        counts[year] = counts.get(year, 0) + 1
    if fh:
        fh.close()
    shutil.rmtree(final_dir, ignore_errors=True)
    tmp_dir.rename(final_dir)
    (config.PROCESSED_DIR / f"{name}.csv").unlink(missing_ok=True)  # ไฟล์รวมแบบเก่า
    return counts


def export_all() -> dict:
    """เรียกผ่าน publish.publish_csv() (ต้องผ่าน validation gate ก่อน)."""
    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    with storage.connect() as conn:
        return {name: _export(conn, name, sql, col) for name, (sql, col) in EXPORTS.items()}
