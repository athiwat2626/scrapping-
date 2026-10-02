"""HTTP client สำหรับ traffic.longdo.com: rate limit ข้าม process + retry + เก็บ raw."""
import fcntl
import gzip
import json
import logging
import re
import time
from datetime import datetime

import requests

from bkk_traffic import config

log = logging.getLogger(__name__)


class LongdoClient:
    def __init__(self, source_tag: str = "misc"):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.USER_AGENT, "Accept": "application/json"})
        self.source_tag = source_tag
        self.request_count = 0
        config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    def _wait_turn(self):
        """ล็อกไฟล์ร่วมกันทุก task/DAG เพื่อให้เว้นช่วง >= CRAWL_DELAY ระหว่าง request เสมอ."""
        lock_path = config.RATE_LOCK_FILE
        lock_path.touch(exist_ok=True)
        with open(lock_path, "r+") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                fh.seek(0)
                last = float(fh.read().strip() or 0)
                wait = last + config.CRAWL_DELAY_SEC - time.time()
                if wait > 0:
                    time.sleep(wait)
                fh.seek(0)
                fh.truncate()
                fh.write(str(time.time()))
                fh.flush()
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)

    def get_json(self, url: str, params: dict | None = None, raw_name: str | None = None):
        data = self._get(url, params, lambda resp: resp.json())
        if raw_name:
            self.save_raw(raw_name, data)
        return data

    def get_text(self, url: str, params: dict | None = None) -> str:
        """สำหรับไฟล์ CSV ที่เว็บเปิดให้ดาวน์โหลด (เช่น ดัชนีรถติดย้อนหลัง)."""
        return self._get(url, params, lambda resp: resp.content.decode("utf-8-sig"))

    def _get(self, url, params, parse):
        last_err = None
        for attempt in range(1, config.MAX_RETRIES + 1):
            self._wait_turn()
            try:
                resp = self.session.get(url, params=params, timeout=config.REQUEST_TIMEOUT_SEC)
                self.request_count += 1
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                return parse(resp)
            except (requests.RequestException, ValueError) as err:
                last_err = err
                backoff = 30 * attempt
                log.warning("GET %s %s failed (%s), retry %d/%d in %ss",
                            url, params, err, attempt, config.MAX_RETRIES, backoff)
                time.sleep(backoff)
        raise RuntimeError(f"GET {url} {params} failed after {config.MAX_RETRIES} tries: {last_err}")

    def save_raw(self, name: str, data):
        now = datetime.now()
        folder = config.RAW_DIR / self.source_tag / f"dt={now:%Y-%m-%d}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{now:%H%M%S}_{name}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)

    # ---------- endpoints ----------
    # incident/event ไม่บันทึก raw ที่นี่ เพราะมีทั้งประเทศ - scrape.py บันทึกเฉพาะรายการ กทม. เอง
    def incident_page(self, page: int, limit: int | None = None):
        """คืน (items, total_pages) - total_pages อ่านจาก pager HTML ที่แนบมากับ JSON.

        limit = จำนวนรายการ/หน้า (ค่าเริ่มต้นของเว็บคือ 10)
        """
        params = {"page": page}
        if limit:
            params["limit"] = limit
        data = self.get_json(config.INCIDENT_URL, params)
        pages = [int(p) for p in re.findall(r"getIncident\((\d+)\)", data.get("pager") or "")]
        return data.get("item") or [], max(pages, default=None)

    def event_by_id(self, eid: int):
        data = self.get_json(config.EVENT_URL, {"eid": eid})
        items = data.get("item") or []
        return items[0] if items else None

    def traffic_index(self):
        return self.get_json(config.TRAFFIC_INDEX_URL, {"now": int(time.time() * 1000)}, raw_name="index")

    def event_types(self):
        return self.get_json(config.EVENT_TYPE_URL, raw_name="types")
