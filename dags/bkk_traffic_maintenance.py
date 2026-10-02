"""ทุกวัน 03:30: ดูแลระบบให้รันได้ยาวๆ โดยดิสก์ไม่เต็ม.

- ลบ log ของ Airflow ที่เก่ากว่า 7 วัน (DAG parquet รันทุกนาที สร้าง log ~4,000 ไฟล์/วัน)
- ล้างประวัติการรันใน DB ของ Airflow ที่เก่ากว่า 14 วัน (airflow db clean)
- ไม่แตะข้อมูลที่สแคปได้ (data/) ทั้งสิ้น
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

LOG_RETENTION_DAYS = 7
DB_RETENTION_DAYS = 14


@dag(
    dag_id="bkk_traffic_maintenance",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["bkk-traffic", "maintenance"],
)
def bkk_traffic_maintenance():
    @task
    def delete_old_logs():
        import time
        from pathlib import Path
        cutoff = time.time() - LOG_RETENTION_DAYS * 86400
        root = Path("/opt/airflow/logs")
        removed = 0
        for f in root.rglob("*.log"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except OSError:
                pass
        for d in sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
            try:
                d.rmdir()  # ลบเฉพาะโฟลเดอร์ว่าง
            except OSError:
                pass
        print(f"deleted {removed:,} log files older than {LOG_RETENTION_DAYS} days")
        return removed

    clean_airflow_db = BashOperator(
        task_id="clean_airflow_db",
        bash_command=(
            "airflow db clean --clean-before-timestamp "
            f"\"$(date -d '-{DB_RETENTION_DAYS} days' '+%Y-%m-%d')\" --skip-archive --yes"
        ),
    )

    delete_old_logs() >> clean_airflow_db


bkk_traffic_maintenance()
