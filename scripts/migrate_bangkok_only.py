"""ล้างข้อมูลเดิมให้เหลือเฉพาะกรุงเทพมหานคร (รันครั้งเดียว ตอนหยุด DAG แล้ว).

docker compose exec -T airflow-scheduler python /opt/airflow/scripts/migrate_bangkok_only.py
"""
import gzip
import json
import shutil
import sys

sys.path.insert(0, "/opt/airflow/plugins")
from bkk_traffic import config, storage  # noqa: E402

with storage.connect() as conn:
    type_names = storage.load_type_names(conn)
    ts = storage.now_str()
    rows = conn.execute("SELECT eid, raw_json FROM incidents").fetchall()
    keep, drop = set(), set()
    for r in rows:
        n = storage.normalize(json.loads(r["raw_json"]), type_names)
        conn.execute("INSERT INTO seen_eids(eid, is_bangkok, seen_at) VALUES(?,?,?) "
                     "ON CONFLICT(eid) DO UPDATE SET is_bangkok=excluded.is_bangkok", (r["eid"], n["is_bangkok"], ts))
        if n["is_bangkok"]:
            keep.add(r["eid"])
            conn.execute("UPDATE incidents SET is_bangkok=1, province_guess=? WHERE eid=?",
                         (n["province_guess"], r["eid"]))
        else:
            drop.add(r["eid"])
    conn.executemany("DELETE FROM incidents WHERE eid=?", [(e,) for e in drop])
    conn.execute("DROP TABLE IF EXISTS probe_count")
    conn.commit()
    print(f"incidents: keep (Bangkok) {len(keep):,}  removed {len(drop):,}")

# raw: เหลือเฉพาะรายการ กทม.
kept_files = removed_files = 0
for src in ("incidents_realtime", "incidents_backfill", "incidents_recover"):
    for path in sorted((config.RAW_DIR / src).glob("dt=*/*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
        items = [i for i in (data.get("item") or []) if storage.to_int(i.get("eid")) in keep]
        if items:
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                json.dump({"item": items}, fh, ensure_ascii=False)
            kept_files += 1
        else:
            path.unlink()
            removed_files += 1
shutil.rmtree(config.RAW_DIR / "probe", ignore_errors=True)
print(f"raw files: rewritten {kept_files}  deleted {removed_files}  (+ raw/probe)")

for f in ("incidents_all.csv", "probe_count.csv"):
    (config.PROCESSED_DIR / f).unlink(missing_ok=True)
for d in ("incidents_all", "probe_count"):
    shutil.rmtree(config.DATA_DIR / "parquet" / d, ignore_errors=True)

with storage.connect() as conn:
    conn.execute("VACUUM")

from bkk_traffic import export, parquet_export, progress  # noqa: E402

print("csv:", export.export_all())
print("parquet:", parquet_export.export_all())
progress.write_progress_file()
