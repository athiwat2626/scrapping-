"""Export ข้อมูลที่ผ่านการ clean เป็น CSV (utf-8-sig เปิดใน Excel แล้วภาษาไทยไม่เพี้ยน)."""
import csv

from bkk_traffic import config, storage

INCIDENT_COLS = ("eid", "start_time", "stop_time", "event_type_id", "event_type", "traffic_relevance", "is_accident", "severity", "title", "title_en",
                 "comment", "comment_en", "latitude", "longitude", "province_guess", "contributor",
                 "createtime", "updatedtime", "dq_flags")

EXPORTS = {
    "incidents_bangkok.csv": f"SELECT {','.join(INCIDENT_COLS)} FROM incidents WHERE is_bangkok=1 AND is_duplicate=0 ORDER BY start_time",
    "accidents_bangkok.csv": f"SELECT {','.join(INCIDENT_COLS)} FROM incidents "
                             "WHERE is_bangkok=1 AND is_accident=1 AND is_duplicate=0 ORDER BY start_time",
    "traffic_index.csv": "SELECT ts, ts_local, index_value, source FROM traffic_index_history ORDER BY ts",
    "travel_times.csv": "SELECT route, fetched_at, distance_m, duration_s, duration_min, from_lat, from_lon, "
                        "to_lat, to_lon FROM travel_times ORDER BY fetched_at, route",
}


def export_all() -> dict:
    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    counts = {}
    with storage.connect() as conn:
        for name, sql in EXPORTS.items():
            cur = conn.execute(sql)
            tmp = config.PROCESSED_DIR / f".{name}.tmp"
            with open(tmp, "w", newline="", encoding="utf-8-sig") as fh:
                w = csv.writer(fh)
                w.writerow([d[0] for d in cur.description])
                n = 0
                for row in cur:
                    w.writerow(row)
                    n += 1
            tmp.replace(config.PROCESSED_DIR / name)
            counts[name] = n
    return counts
