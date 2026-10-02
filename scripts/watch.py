"""แสดงสถานะการสแคปแบบสดในเทอมินอล (อัปเดตทุก N วินาที, Ctrl+C เพื่อออก).

docker compose exec airflow-scheduler python -u /opt/airflow/scripts/watch.py [วินาที]
"""
import json
import sqlite3
import sys
import time
from datetime import datetime

DB = "/opt/airflow/data/warehouse/bkk_traffic.db"
INTERVAL = int(sys.argv[1]) if len(sys.argv) > 1 else 15
PARQUET_MANIFEST = "/opt/airflow/data/parquet/incidents_bangkok/_manifest.json"


def snapshot():
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=30)
    q = lambda sql: c.execute(sql).fetchone()  # noqa: E731
    cp = {r[0]: (json.loads(r[1]), r[2]) for r in c.execute("SELECT job, state_json, updated_at FROM checkpoints")}
    s = {
        "rows": q("SELECT COUNT(*) FROM incidents")[0],
        "accidents": q("SELECT COUNT(*) FROM incidents WHERE is_accident=1")[0],
        "seen": q("SELECT COUNT(*) FROM seen_eids")[0],
        "index_points": q("SELECT COUNT(*) FROM traffic_index")[0],
        "bf": cp.get("backfill", ({}, None)),
        "rt": cp.get("realtime", ({}, None)),
    }
    c.close()
    try:
        with open(PARQUET_MANIFEST, encoding="utf-8") as fh:
            m = json.load(fh)
        s["pq"] = (len(m["files"]), m["total_rows"], m["generated_at"])
    except (OSError, ValueError, KeyError):
        s["pq"] = None
    return s


def age_sec(ts):
    return (datetime.now() - datetime.fromisoformat(ts)).total_seconds() if ts else 1e9


print("BKK traffic scraper - live status (Ctrl+C เพื่อออก)")
print(f"{'เวลา':8} | {'สถานะ':14} | {'กทม.(แถว)':>10} | {'อุบัติเหตุ':>9} | {'สแคปผ่าน':>9} | "
      f"{'หน้า':>11} | {'ย้อนถึง':16} | {'+แถว/นาที':>9} | {'realtime':10} | parquet")
print("-" * 160)
prev = None
try:
    while True:
        s = snapshot()
        bf, bf_upd = s["bf"]
        rt, _ = s["rt"]
        bf_age = age_sec(bf_upd)
        if bf.get("done"):
            state = "✅ เสร็จแล้ว"
        elif bf_age < 120:
            state = "🟢 กำลังสแคป"
        elif bf_age < 900:
            state = "🟡 รอรอบถัดไป"
        else:
            state = "🔴 หยุดอยู่"
        rate = ""
        if prev:
            mins = (time.time() - prev[0]) / 60
            rate = f"{(s['seen'] - prev[1]) / mins:,.0f}" if mins > 0 else ""
        rate_rows = f"{(s['rows'] - prev[2]) / ((time.time() - prev[0]) / 60):,.0f}" if prev else ""
        rt_age = age_sec(rt.get("last_success"))
        rt_txt = f"{rt_age / 60:.0f} นาทีก่อน" if rt_age < 1e8 else "-"
        page = f"{bf.get('page', 0):,}/{bf.get('source_total_pages') or 0:,}"
        pq_txt = "-"
        if s["pq"]:
            n_files, pq_rows, pq_at = s["pq"]
            pq_txt = f"{n_files} ไฟล์ {pq_rows:,} แถว ({age_sec(pq_at):.0f} วิก่อน)"
        print(f"{datetime.now():%H:%M:%S} | {state:13} | {s['rows']:>10,} | {s['accidents']:>9,} | {s['seen']:>9,} | "
              f"{page:>11} | {str(bf.get('anchor_time', '-'))[:16]:16} | {rate_rows:>9} | {rt_txt:10} | {pq_txt}",
              flush=True)
        prev = (time.time(), s["seen"], s["rows"])
        time.sleep(INTERVAL)
except KeyboardInterrupt:
    print("\nออกจากหน้าแสดงสถานะ (การสแคปยังทำงานต่อใน Airflow)")
