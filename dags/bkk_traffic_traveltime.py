"""ทุก 5 นาที: เก็บเวลาเดินทางจุด A -> B (Longdo RouteService, คิดสภาพจราจร).

8 เส้นทาง x 288 รอบ/วัน = ~2,300 ครั้ง/วัน (~69,000/เดือน) อยู่ในโควตา key ฟรี (100,000/เดือน, 5,000/วัน)
มีตัวกันโควตาใน traveltime.py: ถ้าใกล้เต็มจะข้ามรอบแทนการยิงเกิน

ต้องตั้ง Airflow Variable `longdo_api_key` ก่อน (สมัครฟรี https://map.longdo.com/console) - ถ้ายังไม่ตั้ง DAG จะ skip
เปลี่ยนเส้นทางได้ที่ Variable `bkk_od_pairs` เช่น [["บ้าน -> ที่ทำงาน", 13.80, 100.55, 13.74, 100.56], ...]
"""
import json
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="bkk_traffic_traveltime",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2), "execution_timeout": timedelta(minutes=10)},
    tags=["bkk-traffic", "scrape", "travel-time"],
)
def bkk_traffic_traveltime():
    @task.short_circuit
    def has_api_key():
        if Variable.get("bkk_scrape_stopped", "false") == "true":
            return False
        if not Variable.get("longdo_api_key", ""):
            print("ยังไม่มี Variable longdo_api_key -> ข้าม (สมัครฟรีที่ https://map.longdo.com/console)")
            return False
        return True

    @task
    def collect_travel_times():
        from bkk_traffic import traveltime
        pairs = json.loads(Variable.get("bkk_od_pairs", "null") or "null")
        result = traveltime.collect(Variable.get("longdo_api_key"), pairs)
        print(result)
        return result

    has_api_key() >> collect_travel_times()


bkk_traffic_traveltime()
