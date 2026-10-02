"""สภาพจราจรปกติ: ดัชนีรถติด Longdo ย้อนหลังทุก 5 นาที (ไฟล์ที่ Longdo เปิดให้ดาวน์โหลดรายปี).

ที่มา: https://traffic.longdo.com/download -> /api/raw/trafficindex/{ปี}  (CSV: timestamp,datetime,index)
ดัชนี 0-10 = ความติดขัดภาพรวม กทม.+ปริมณฑล (0 = ไม่ติด, 10 = ติดทุกถนน) คำนวณทุก 5 นาที
เงื่อนไขของ Longdo: โปรดอ้างถึงแหล่งที่มา "Longdo Traffic (https://traffic.longdo.com)"

ปีที่โหลดครบแล้วไม่โหลดซ้ำ ปีปัจจุบันโหลดใหม่ทุกครั้ง (มีข้อมูลเพิ่มทุก 5 นาที)
"""
import csv
import io
import logging
from datetime import datetime

from bkk_traffic import config, storage
from bkk_traffic.client import LongdoClient

log = logging.getLogger(__name__)

URL = f"{config.BASE_URL}/api/raw/trafficindex/{{year}}"
FIRST_YEAR = 2012


def load_year(conn, client, year: int) -> int:
    text = client.get_text(URL.format(year=year))
    config.RAW_DIR.joinpath("traffic_index_history").mkdir(parents=True, exist_ok=True)
    config.RAW_DIR.joinpath("traffic_index_history", f"{year}.csv").write_text(text, encoding="utf-8")
    rows = []
    for r in csv.DictReader(io.StringIO(text)):
        ts, value = storage.to_int(r.get("timestamp")), storage.to_float(r.get("index"))
        if ts is None or value is None:
            continue
        rows.append((ts, datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"), value, f"history:{year}"))
    conn.executemany("INSERT INTO traffic_index_history(ts, ts_local, index_value, source) VALUES(?,?,?,?) "
                     "ON CONFLICT(ts) DO UPDATE SET index_value=excluded.index_value, source=excluded.source", rows)
    conn.commit()
    return len(rows)


def run(years=None) -> dict:
    client = LongdoClient("traffic_index_history")
    this_year = datetime.now().year
    result = {}
    with storage.connect() as conn:
        state = storage.get_checkpoint(conn, "traffic_index_history")
        done = set(state.get("years_done", []))
        todo = years or [y for y in range(FIRST_YEAR, this_year + 1) if y not in done or y == this_year]
        for year in todo:
            n = load_year(conn, client, year)
            result[year] = n
            if year < this_year and n > 0:
                done.add(year)
            log.info("traffic index %s: %d rows", year, n)
        storage.set_checkpoint(conn, "traffic_index_history", {
            "years_done": sorted(done), "last_run": storage.now_str(), "last_result": result,
            "rows_total": conn.execute("SELECT COUNT(*) FROM traffic_index_history").fetchone()[0],
            "range": list(conn.execute("SELECT MIN(ts_local), MAX(ts_local) FROM traffic_index_history").fetchone()),
        })
    return result
