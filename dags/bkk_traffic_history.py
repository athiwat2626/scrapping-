"""ทุกวัน 02:30: ไฟล์รายปีที่ Longdo เปิดให้ดาวน์โหลด (https://traffic.longdo.com/download)

1) ดัชนีรถติดทุก 5 นาที (สภาพจราจรปกติ) 2012-ปัจจุบัน
2) เหตุการณ์ย้อนหลัง (เสริมข้อมูลจาก API: เพิ่มเฉพาะ eid ที่ยังไม่มี, ผ่าน clean/gate เหมือนกัน)
รอบแรกโหลดทุกปี (~15 request ต่อชุด), รอบถัดไปโหลดเฉพาะปีปัจจุบัน
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable


@dag(
    dag_id="bkk_traffic_history",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "pool": "longdo_api",
                  "execution_timeout": timedelta(minutes=30)},
    tags=["bkk-traffic", "scrape", "traffic-index"],
)
def bkk_traffic_history():
    @task
    def load_traffic_index_history():
        from bkk_traffic import history
        if Variable.get("bkk_scrape_stopped", "false") == "true":
            raise AirflowSkipException("การสแคปถูกหยุดแล้ว")
        result = history.run()
        for year, n in result.items():
            print(f"{year}: {n:,} จุด (ทุก 5 นาที)")
        return result

    @task(execution_timeout=timedelta(minutes=60))
    def load_event_feed():
        """เหตุการณ์ย้อนหลังจากไฟล์รายปี: รอบแรกทุกปี, ถัดไปเฉพาะปีปัจจุบัน (ช่วยเติมที่ realtime พลาด)."""
        from bkk_traffic import feed
        if Variable.get("bkk_scrape_stopped", "false") == "true":
            raise AirflowSkipException("การสแคปถูกหยุดแล้ว")
        result = feed.run()
        for year, s in result.items():
            print(f"{year}: {s}")
        return {str(k): v for k, v in result.items()}

    load_traffic_index_history() >> load_event_feed()


bkk_traffic_history()
