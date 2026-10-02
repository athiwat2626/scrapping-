"""ทุก 15 นาที: ไล่ดึงข้อมูลย้อนหลังต่อจาก checkpoint ทีละ batch.

หยุดเมื่อถึง bkk_backfill_start_date หรือเมื่อเหตุการณ์ กทม. ครบ bkk_backfill_target_rows
ถ้าหยุดเพราะครบจำนวน จะ export Parquet ทันที (realtime ยังดึงของใหม่ต่อ)
หยุดการสแคปทั้งหมดด้วยมือ: ตั้ง Variable bkk_scrape_stopped=true
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from bkk_traffic import config


@dag(
    dag_id="bkk_traffic_backfill",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2), "pool": "longdo_api", "priority_weight": 1},
    tags=["bkk-traffic", "scrape", "backfill"],
)
def bkk_traffic_backfill():
    @task(execution_timeout=timedelta(minutes=30))
    def backfill_batch():
        from bkk_traffic import scrape
        if Variable.get("bkk_scrape_stopped", "false") == "true":
            raise AirflowSkipException("การสแคปถูกหยุดแล้ว (bkk_scrape_stopped=true)")
        state = scrape.run_backfill_batch(
            pages_per_run=int(Variable.get("bkk_backfill_pages_per_run", config.DEFAULT_BACKFILL_PAGES_PER_RUN)),
            start_date=Variable.get("bkk_backfill_start_date", config.DEFAULT_BACKFILL_START_DATE),
            page_size=int(Variable.get("bkk_backfill_page_size", config.DEFAULT_BACKFILL_PAGE_SIZE)),
            target_rows=int(Variable.get("bkk_backfill_target_rows", config.DEFAULT_BACKFILL_TARGET_ROWS)),
            max_seconds=config.DEFAULT_BACKFILL_MAX_SECONDS,
        )
        if state.get("done"):
            if "target" in (state.get("done_reason") or ""):
                # ครบเป้าแล้วหยุดเฉพาะ backfill (ของเก่า) - realtime ยังดึงของใหม่ต่อไป
                from bkk_traffic import export, parquet_export
                print("final parquet:", parquet_export.export_all())
                print("final csv:", export.export_all())
            raise AirflowSkipException(f"backfill เสร็จแล้ว ({state.get('done_reason')})")
        return state

    @task(pool="default_pool", trigger_rule="all_done")
    def update_progress():
        from bkk_traffic import progress
        print(progress.write_progress_file().get("backfill_progress"))

    backfill_batch() >> update_progress()


bkk_traffic_backfill()
