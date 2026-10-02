"""ทุก 1 นาที: CLEAN -> VALIDATE (gate) -> PUBLISH Parquet (ไฟล์ละ 10,000 แถว) ที่ data/parquet/.

clean            : clean แถวใน bronze ที่ค้าง + ตั้ง is_duplicate (ตัดรายงานซ้ำ)
validate_gate    : ตรวจ silver (schema, แถวนอก กทม., ค่าว่าง, quarantine, จำนวนแถวไม่ลดฮวบ)
publish_parquet  : เขียน Parquet เฉพาะเมื่อ gate ผ่าน - ไม่ผ่าน = ไม่เขียนทับชุดเดิม + task แดง
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


@dag(
    dag_id="bkk_traffic_parquet",
    schedule="* * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=5)},
    tags=["bkk-traffic", "clean", "validate", "publish"],
)
def bkk_traffic_parquet():
    @task
    def clean():
        from bkk_traffic import publish
        return publish.run_clean_stage()

    @task
    def validate_gate():
        from bkk_traffic import publish
        gate = publish.gate_checks()
        for c in gate["checks"]:
            print(f"[{c['status']}] {c['check']}: {c['value']} {c['detail']}")
        if not gate["passed"]:
            raise AirflowFailException("validation gate FAIL - ไม่ปล่อยไฟล์ใหม่ (ดู data/quality/gate_last.json)")
        return gate

    @task
    def publish_parquet(gate: dict):
        from bkk_traffic import publish
        result = publish.publish_parquet(gate)
        for name, r in result.items():
            print(f"{name}: {r['rows']:,} rows -> {r['files']} files{' (ไม่เปลี่ยน)' if r.get('skipped') else ''}")
        return result

    gate = validate_gate()
    clean() >> gate
    publish_parquet(gate)


bkk_traffic_parquet()
