"""ทุก 15 นาที: ดึงเหตุการณ์ใหม่ (เก็บเฉพาะ กทม.) + traffic index แล้วอัปเดต progress."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from bkk_traffic import config

default_args = {"retries": 2, "retry_delay": timedelta(minutes=2), "pool": "longdo_api", "priority_weight": 10}


@dag(
    dag_id="bkk_traffic_realtime",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    default_args=default_args,
    tags=["bkk-traffic", "scrape"],
)
def bkk_traffic_realtime():
    @task.short_circuit(pool="default_pool")
    def scraping_enabled():
        return Variable.get("bkk_scrape_stopped", "false") != "true"

    @task
    def traffic_index():
        from bkk_traffic import scrape
        return scrape.run_traffic_index()

    @task(execution_timeout=timedelta(minutes=10))
    def incidents():
        from bkk_traffic import scrape
        max_pages = int(Variable.get("bkk_realtime_max_pages", config.DEFAULT_REALTIME_MAX_PAGES))
        return scrape.run_realtime_incidents(max_pages)

    @task(pool="default_pool", trigger_rule="all_done")
    def update_progress():
        from bkk_traffic import progress
        s = progress.write_progress_file()
        print({k: s[k] for k in ("incidents_bangkok", "accidents_bangkok", "eids_seen_all",
                                 "traffic_index_points", "backfill_progress") if k in s})

    scraping_enabled() >> [traffic_index(), incidents()] >> update_progress()


bkk_traffic_realtime()
