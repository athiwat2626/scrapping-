"""คัดเหตุการณ์ที่ไม่เกี่ยวกับการจราจรออกจากข้อมูลเดิม (รันครั้งเดียว ตอน pause DAG).

ลบออกจาก bronze / silver / quarantine และไฟล์ raw (เก็บแค่ eid ไว้ใน seen_eids) แล้วปล่อย Parquet/CSV ใหม่
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/migrate_v3_traffic_only.py
"""
import gzip
import json
import sys
from collections import Counter

sys.path.insert(0, "/opt/airflow/plugins")
from bkk_traffic import clean, config, publish, storage  # noqa: E402

with storage.connect() as conn:  # connect() เพิ่มคอลัมน์ traffic_relevance / ตาราง travel_times ให้เอง
    types = clean.load_types(conn)
    keep, drop, dropped_types = set(), set(), Counter()
    for eid, raw in conn.execute("SELECT eid, raw_json FROM bronze_incidents"):
        item = json.loads(raw)
        is_bkk, relevance = clean.in_scope(item, types)
        if is_bkk and relevance:
            keep.add(eid)
        else:
            drop.add(eid)
            dropped_types[types[1].get(clean.to_int(item.get("severity")), "Unknown")] += 1
    for table in ("bronze_incidents", "incidents", "quarantine"):
        conn.executemany(f"DELETE FROM {table} WHERE eid=?", [(e,) for e in drop])
    conn.commit()
    print(f"keep {len(keep):,}  drop {len(drop):,}  dropped by type: {dict(dropped_types.most_common())}")
    print("reclean_all:", clean.reclean_all(conn))   # เติม traffic_relevance ให้แถวที่เหลือ
    print("duplicates:", clean.mark_duplicates(conn))

# raw files: เหลือเฉพาะ eid ที่เก็บ
rewritten = deleted = 0
for path in sorted(config.RAW_DIR.glob("incidents_*/dt=*/*.json.gz")):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        data = json.load(fh)
    items = data.get("item") or []
    kept = [i for i in items if clean.to_int(i.get("eid")) in keep]
    if len(kept) == len(items):
        continue
    if kept:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            json.dump({"item": kept}, fh, ensure_ascii=False)
        rewritten += 1
    else:
        path.unlink()
        deleted += 1
print(f"raw files: rewritten {rewritten}  deleted {deleted}")

with storage.connect() as conn:
    conn.execute("VACUUM")

gate = publish.gate_checks(allow_drop=True)  # ตั้งใจลดจำนวนแถว
for c in gate["checks"]:
    print(f"  [{c['status']}] {c['check']}: {c['value']}")
if gate["passed"]:
    print("parquet:", publish.publish_parquet(gate))
    print("csv:", publish.publish_csv())  # Parquet ชุดใหม่เป็นฐานของ G7 แล้ว จึงผ่าน gate ปกติ
