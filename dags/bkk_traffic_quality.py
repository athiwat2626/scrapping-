"""ทุกวัน 01:00: หา eid ที่ขาด -> เติม -> ตรวจคุณภาพ -> export CSV -> แจ้งผล."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from bkk_traffic import config


@dag(
    dag_id="bkk_traffic_quality",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bkk-traffic", "quality"],
)
def bkk_traffic_quality():
    @task(pool="longdo_api", priority_weight=5)
    def refresh_event_types():
        from bkk_traffic import scrape
        if Variable.get("bkk_scrape_stopped", "false") == "true":
            return "skipped: scraping stopped"
        return scrape.refresh_event_types()

    @task
    def detect_missing_eids():
        from bkk_traffic import scrape
        return scrape.detect_missing_eids()

    @task(pool="longdo_api", priority_weight=5, execution_timeout=timedelta(minutes=45))
    def recover_missing_eids():
        from bkk_traffic import scrape
        if Variable.get("bkk_scrape_stopped", "false") == "true":
            return "skipped: scraping stopped"
        limit = int(Variable.get("bkk_recover_per_run", config.DEFAULT_RECOVER_PER_RUN))
        return scrape.recover_missing_eids(limit, max_seconds=35 * 60)

    @task
    def quality_checks(data_interval_end=None):
        from bkk_traffic import quality
        day = data_interval_end.in_timezone("Asia/Bangkok").date() - timedelta(days=1)
        report = quality.run_quality_checks(day)
        return {"overall": report["overall"],
                "problems": [c for c in report["checks"] if c["status"] != "OK"]}

    @task(trigger_rule="all_done")
    def export_csv():
        from bkk_traffic import progress, publish
        progress.write_progress_file()
        return publish.publish_csv()  # ต้องผ่าน validation gate ก่อน

    @task
    def gate(result: dict):
        for p in result["problems"]:
            print(f"[{p['status']}] {p['check']}: {p['value']} {p['detail']}")
        if result["overall"] == "FAIL":
            raise AirflowFailException("Data quality FAIL - ดู data/quality/dt=<วัน>/report.md")

    checks = quality_checks()
    refresh_event_types() >> detect_missing_eids() >> recover_missing_eids() >> checks >> export_csv()
    gate(checks)


bkk_traffic_quality()
