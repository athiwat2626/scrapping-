"""เวลาเดินทางจากจุด A -> จุด B ในกรุงเทพฯ (Longdo RouteService, mode=t = คิดสภาพจราจร ณ ตอนนั้น).

ต้องมี API key ของตัวเอง (สมัครฟรีที่ https://map.longdo.com/console) ใส่ใน Airflow Variable `longdo_api_key`
เส้นทางที่เก็บ: Airflow Variable `bkk_od_pairs` (JSON) ถ้าไม่ตั้ง ใช้ DEFAULT_OD_PAIRS
"""
import json
import logging
import time

import requests

from bkk_traffic import config, storage

log = logging.getLogger(__name__)

ROUTE_URL = "https://api.longdo.com/RouteService/json/route/guide"
DELAY_BETWEEN_ROUTES_SEC = 1          # โควตา: ไม่เกิน 60 ครั้ง/นาที

# โควตา key ฟรีของ Longdo: 100,000 ครั้ง/เดือน, 5,000 ครั้ง/วัน (เผื่อไว้ ~10%)
# https://map.longdo.com/products/pricing
QUOTA_PER_DAY = 4_500
QUOTA_PER_MONTH = 95_000


def _usage(conn):
    state = storage.get_checkpoint(conn, "traveltime_usage")
    today, month = storage.now_str()[:10], storage.now_str()[:7]
    if state.get("day") != today:
        state.update(day=today, day_calls=0)
    if state.get("month") != month:
        state.update(month=month, month_calls=0)
    return state


def quota_left(conn) -> int:
    u = _usage(conn)
    return max(0, min(QUOTA_PER_DAY - u["day_calls"], QUOTA_PER_MONTH - u["month_calls"]))

# (ชื่อเส้นทาง, lat ต้นทาง, lon ต้นทาง, lat ปลายทาง, lon ปลายทาง)
DEFAULT_OD_PAIRS = [
    ("อนุสาวรีย์ชัยฯ -> สยาม", 13.7650, 100.5383, 13.7457, 100.5348),
    ("สีลม -> อโศก", 13.7290, 100.5340, 13.7370, 100.5603),
    ("ห้าแยกลาดพร้าว -> อนุสาวรีย์ชัยฯ", 13.8160, 100.5610, 13.7650, 100.5383),
    ("บางนา -> พระราม 9", 13.6680, 100.6040, 13.7570, 100.5650),
    ("ดอนเมือง -> หมอชิต", 13.9126, 100.6068, 13.8027, 100.5536),
    ("สาทร -> วงเวียนใหญ่", 13.7210, 100.5290, 13.7260, 100.4940),
    ("มีนบุรี -> รามคำแหง", 13.8140, 100.7480, 13.7550, 100.6200),
    ("บางแค -> ปิ่นเกล้า", 13.7130, 100.4060, 13.7600, 100.4900),
]


def compact_raw(body: dict) -> str:
    """เก็บ raw แบบย่อ: ตัด "guide" (รายการเลี้ยวทุกจุด ~10-20 KB) ออก เหลือ meta + ระยะ/เวลา."""
    data = body.get("data")
    if isinstance(data, list):
        data = [{k: v for k, v in d.items() if k != "guide"} for d in data if isinstance(d, dict)]
    return json.dumps({"meta": body.get("meta"), "data": data}, ensure_ascii=False)


def _parse(resp: dict):
    """คืน (distance_m, duration_s). รองรับทั้งรูปแบบ {"data":[{...}]} และ {...} ตรงๆ."""
    data = resp.get("data", resp)
    if isinstance(data, list):
        data = data[0] if data else {}
    distance = data.get("distance")
    duration = data.get("interval", data.get("time", data.get("duration")))
    if distance is None or duration is None:
        raise ValueError(f"รูปแบบผลลัพธ์ไม่ตรงที่คาด: keys={list(data)[:15]}")
    return float(distance), float(duration)


def collect(api_key: str, pairs=None) -> dict:
    pairs = pairs or DEFAULT_OD_PAIRS
    session = requests.Session()
    session.headers["User-Agent"] = config.USER_AGENT
    fetched_at = storage.now_str()
    ok, errors = 0, []
    with storage.connect() as conn:
        left = quota_left(conn)
        if left < len(pairs):
            u = _usage(conn)
            return {"ok": 0, "failed": 0, "skipped_quota": True,
                    "usage": {"day": u["day_calls"], "month": u["month_calls"]}}
        usage = _usage(conn)
        for name, flat, flon, tlat, tlon in pairs:
            params = {"flat": flat, "flon": flon, "tlat": tlat, "tlon": tlon, "mode": "t", "type": 25,
                      "locale": "th", "key": api_key}
            usage["day_calls"] += 1
            usage["month_calls"] += 1
            try:
                resp = session.get(ROUTE_URL, params=params, timeout=config.REQUEST_TIMEOUT_SEC)
                resp.raise_for_status()
                if resp.text.lstrip().startswith("throw"):
                    raise RuntimeError(resp.text.strip()[:120])  # เช่น 'Route Service API Key Error'
                body = resp.json()
                distance, duration = _parse(body)
                conn.execute(
                    "INSERT OR REPLACE INTO travel_times(route, fetched_at, distance_m, duration_s, duration_min, "
                    "from_lat, from_lon, to_lat, to_lon, raw_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (name, fetched_at, distance, duration, round(duration / 60, 2), flat, flon, tlat, tlon,
                     compact_raw(body)),
                )
                ok += 1
            except Exception as err:  # noqa: BLE001 - เส้นทางเดียวพังไม่ควรทำให้เส้นอื่นหาย
                log.warning("route %s failed: %s", name, err)
                errors.append(f"{name}: {err}")
            time.sleep(DELAY_BETWEEN_ROUTES_SEC)
        conn.commit()
        storage.set_checkpoint(conn, "traveltime_usage", usage)
    if ok == 0 and errors:
        raise RuntimeError("ดึงเวลาเดินทางไม่ได้เลย: " + errors[0])
    return {"ok": ok, "failed": len(errors), "errors": errors[:3],
            "usage": {"day": usage["day_calls"], "month": usage["month_calls"]}}
