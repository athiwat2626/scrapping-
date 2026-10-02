"""นำเข้าเหตุการณ์ย้อนหลังจากไฟล์รายปีที่ Longdo เปิดให้ดาวน์โหลด (https://traffic.longdo.com/download).

https://event.longdo.com/feed/{ปี}  CSV: eid,title,title_en,description,description_en,latitude,longitude,type,start,stop,contributor
- ทั้งปีใน 1 request (เร็วกว่าดึงทีละหน้ามาก)
- ตรวจเทียบกับข้อมูลจาก API แล้ว: title / start / stop / พิกัด / type ตรงกัน 100%
- แต่ไฟล์ไม่มี ~25% ของเหตุการณ์ที่ API มี และไม่มี severity_level / createtime / updatedtime
  -> ใช้เสริม API ไม่ใช่แทน: เพิ่มเฉพาะ eid ที่ยังไม่มีใน bronze (ข้อมูลจาก API ไม่ถูกทับ)
     และเมื่อ backfill/realtime ดึง eid เดียวกันจาก API ทีหลัง ข้อมูล API จะแทนที่ของจากไฟล์
ผ่าน pipeline เดียวกับ API ทุกขั้น: กรอง กทม.+จราจร -> bronze -> clean (R1-R8) -> dedup -> gate -> Parquet
"""
import csv
import io
import logging
from datetime import datetime

from bkk_traffic import storage
from bkk_traffic.client import LongdoClient

log = logging.getLogger(__name__)

URL = "https://event.longdo.com/feed/{year}"
FIRST_YEAR = 2012
CHUNK = 2000


def to_item(r: dict) -> dict:
    """แปลงแถว CSV ให้เป็นรูปแบบเดียวกับ incident.json (ฟิลด์ "severity" ของ API = type id)."""
    return {
        "eid": r.get("eid"), "title": r.get("title"), "title_en": r.get("title_en") or None,
        "comment": r.get("description"), "comment_en": r.get("description_en") or None,
        "latitude": r.get("latitude"), "longitude": r.get("longitude"),
        "severity": r.get("type"), "severity_level": None,
        "start_time": r.get("start"), "stop_time": r.get("stop") or None,
        "contributor": r.get("contributor"), "source": "longdo_event_feed",
    }


def load_year(client, year: int) -> dict:
    from bkk_traffic.scrape import _store
    text = client.get_text(URL.format(year=year)).lstrip("﻿")
    rows = list(csv.DictReader(io.StringIO(text)))
    stats = {"rows": len(rows), "added": 0, "skipped_existing": 0}
    with storage.connect() as conn:
        for i in range(0, len(rows), CHUNK):
            items = [to_item(r) for r in rows[i:i + CHUNK]]
            eids = [storage.to_int(it["eid"]) for it in items]
            have = {r[0] for r in conn.execute(
                f"SELECT eid FROM bronze_incidents WHERE eid IN ({','.join('?' * len(eids))})", eids)}
            fresh = [it for it, e in zip(items, eids) if e is not None and e not in have]
            stats["skipped_existing"] += len(items) - len(fresh)
            if fresh:
                stats["added"] += _store(conn, client, fresh, "feed", f"feed{year}_{i // CHUNK:03d}")
    return stats


def run(years=None) -> dict:
    client = LongdoClient("event_feed")
    this_year = datetime.now().year
    result = {}
    with storage.connect() as conn:
        state = storage.get_checkpoint(conn, "event_feed")
    done = set(state.get("years_done", []))
    todo = years or [y for y in range(this_year, FIRST_YEAR - 1, -1) if y not in done or y == this_year]
    for year in todo:
        result[year] = load_year(client, year)
        if year < this_year:
            done.add(year)
        log.info("event feed %s: %s", year, result[year])
        with storage.connect() as conn:
            storage.set_checkpoint(conn, "event_feed", {"years_done": sorted(done), "last_run": storage.now_str(),
                                                        "last_result": {str(k): v for k, v in result.items()}})
    return result
