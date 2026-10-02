"""ย้ายข้อมูลเดิมเข้าโครง bronze -> silver -> quarantine แล้ว clean ใหม่ทั้งหมด (รันครั้งเดียว ตอน pause DAG).

docker compose exec -T airflow-scheduler python /opt/airflow/scripts/migrate_v2_layers.py
"""
import sys

sys.path.insert(0, "/opt/airflow/plugins")
from bkk_traffic import clean, publish, storage  # noqa: E402

with storage.connect() as conn:  # connect() เพิ่มตาราง/คอลัมน์ใหม่ให้เอง
    moved = conn.execute(
        "INSERT OR IGNORE INTO bronze_incidents(eid, raw_json, fetched_via, first_fetched_at, last_fetched_at) "
        "SELECT eid, raw_json, fetched_via, first_seen_at, last_seen_at FROM incidents WHERE raw_json IS NOT NULL"
    ).rowcount
    conn.commit()
    print(f"bronze: copied {moved:,} raw rows")
    print("reclean_all:", clean.reclean_all(conn))
    print("duplicates:", clean.mark_duplicates(conn))
    conn.execute("UPDATE incidents SET raw_json=NULL WHERE raw_json IS NOT NULL")  # raw อยู่ใน bronze แล้ว
    conn.commit()
    for t in ("bronze_incidents", "incidents", "quarantine"):
        print(t, conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])

with storage.connect() as conn:
    conn.execute("VACUUM")

gate = publish.gate_checks()
for c in gate["checks"]:
    print(f"  [{c['status']}] {c['check']}: {c['value']}")
print("gate passed:", gate["passed"])
if gate["passed"]:
    print("parquet:", publish.publish_parquet(gate))
    print("csv:", publish.publish_csv())
